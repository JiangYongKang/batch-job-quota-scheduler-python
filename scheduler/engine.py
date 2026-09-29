"""Core scheduler engine.

Responsibilities:
  * admission control (capacity / per-caller quota) with distinguishable
    rejection reasons, and stable priority + FIFO queue ordering;
  * concurrent execution bounded by cluster capacity, with resource release
    on success, failure, timeout, cancel and retry;
  * bounded retries with backoff;
  * full-snapshot persistence and restart recovery (running jobs from a
    previous process are re-queued, usage is recomputed, never double counted).

Concurrency model: a single asyncio event loop. All state mutation happens
under ``self._lock``; job coroutines run concurrently but only touch engine
state through small locked helper methods.
"""
from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import time
from typing import Any

from scheduler.config import SchedulerConfig
from scheduler.models import FailureReason, Job, JobStatus, RejectReason
from scheduler.persistence import StateStore
from scheduler.workload import WorkloadError, WorkloadSpec, run_workload

logger = logging.getLogger("scheduler.engine")

# Backoff before attempt N+1; capped so tests do not wait long.
RETRY_BACKOFF_BASE = 0.05
RETRY_BACKOFF_CAP = 2.0


class AdmissionError(Exception):
    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class SchedulerEngine:
    def __init__(self, config: SchedulerConfig, store: StateStore) -> None:
        self.config = config
        self.store = store
        self._jobs: dict[str, Job] = {}
        self._seq = itertools.count(1)
        self._used = 0
        self._per_caller_used: dict[str, int] = {}
        # job_id -> asyncio.Task for currently running workloads
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        # Condition tied to the lock: signals state changes that may make a
        # dispatch pass productive. Notifying while holding the lock avoids
        # lost-wakeup races between a submit and the loop going to sleep.
        self._cond = asyncio.Condition(self._lock)
        self._tick_task: asyncio.Task[None] | None = None
        self._stopping = False

    # ------------------------------------------------------------------ #
    # lifecycle / persistence
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        await self._recover()
        self._tick_task = asyncio.create_task(self._run_loop(), name="scheduler-loop")
        logger.info(
            "engine started capacity=%d default_quota=%d jobs_loaded=%d",
            self.config.cluster_capacity,
            self.config.default_quota,
            len(self._jobs),
        )

    async def stop(self) -> None:
        async with self._lock:
            self._stopping = True
            # Settle running jobs BEFORE tearing down their tasks: explicit
            # cancel requests are honored (terminal), everything else is put
            # back in the queue so the next process resumes it. Resources are
            # released here and the snapshot is persisted, so nothing is lost
            # or double counted on restart.
            now = time.time()
            for job_id, job in list(self._jobs.items()):
                if job.status is not JobStatus.RUNNING:
                    continue
                await self._release_resources_locked(job)
                if job.cancel_requested:
                    job.status = JobStatus.CANCELED
                    job.finished_at = now
                    job.fail_reason = FailureReason.CANCELED
                    job.fail_detail = "canceled before shutdown completed"
                    logger.info("shutdown: running job %s cancel honored", job_id)
                else:
                    job.status = JobStatus.QUEUED
                    job.started_at = None
                    job.not_before = now
                    job.fail_reason = FailureReason.RESTART_INTERRUPTED
                    job.fail_detail = "process stopped while the job was running"
                    logger.warning(
                        "shutdown: running job %s re-queued for next start", job_id
                    )
            self._persist_locked()
            self._cond.notify_all()
        if self._tick_task:
            self._tick_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tick_task
        # Cancel the workload coroutines. Their finalize callbacks become
        # no-ops: the jobs are no longer RUNNING and resources were released
        # exactly once above.
        for task in list(self._tasks.values()):
            task.cancel()
        for task in list(self._tasks.values()):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        logger.info("engine stopped, snapshot persisted")

    def _persist_locked(self) -> None:
        snapshot = {
            "version": 1,
            "saved_at": time.time(),
            "used_resources": self._used,
            "jobs": {jid: job.to_dict() for jid, job in self._jobs.items()},
        }
        # Persist outside the lock-free section: file I/O is quick and callers
        # already hold the lock; atomic rename keeps the file consistent.
        self.store.save(snapshot)

    async def _recover(self) -> None:
        snapshot = self.store.load()
        if not snapshot:
            return
        recovered_running = 0
        max_seq = 0
        for raw in snapshot.get("jobs", {}).values():
            job = Job.from_dict(raw)
            max_seq = max(max_seq, job.enqueue_seq)
            if job.status is JobStatus.RUNNING:
                # The previous process died (or was stopped) mid-execution.
                # Re-queue; resources are recomputed below, never trusted from disk.
                if job.cancel_requested:
                    # A hard crash between persisting the cancel request and
                    # finalizing it: honor the cancel now instead of rerunning
                    # (the graceful-stop path reaches the same terminal state).
                    job.status = JobStatus.CANCELED
                    job.finished_at = time.time()
                    job.fail_reason = FailureReason.CANCELED
                    job.fail_detail = (
                        "cancel requested before the previous process crashed"
                    )
                    logger.warning(
                        "recovery: job %s had a pending cancel at crash -> CANCELED",
                        job.job_id,
                    )
                else:
                    logger.warning(
                        "recovery: job %s was RUNNING at shutdown -> re-queued "
                        "(attempts so far=%d, resources not double counted)",
                        job.job_id,
                        job.attempts,
                    )
                    job.status = JobStatus.QUEUED
                    job.started_at = None
                    job.cancel_requested = False
                    job.not_before = time.time()
                    job.fail_reason = FailureReason.RESTART_INTERRUPTED
                    job.fail_detail = (
                        "process restarted while the job was running"
                    )
                    recovered_running += 1
            self._jobs[job.job_id] = job
        # Advance sequence counter past every enqueue_seq seen on disk.
        self._seq = itertools.count(max_seq + 1)
        # Recompute resource usage purely from in-memory state. Only RUNNING
        # jobs hold resources; after recovery none do (all were re-queued).
        self._used = 0
        self._per_caller_used = {}
        for job in self._jobs.values():
            if job.status is JobStatus.RUNNING:
                self._used += job.resource
                self._per_caller_used[job.caller] = (
                    self._per_caller_used.get(job.caller, 0) + job.resource
                )
        logger.info(
            "recovery complete jobs=%d requeued_running=%d recomputed_used=%d "
            "(persisted counter was %s; ignored, recomputed from job states)",
            len(self._jobs),
            recovered_running,
            self._used,
            snapshot.get("used_resources"),
        )
        self._persist_locked()

    # ------------------------------------------------------------------ #
    # admission / submit
    # ------------------------------------------------------------------ #
    async def submit(
        self,
        job_id: str,
        caller: str,
        resource: int,
        priority: int,
        max_retries: int,
        timeout_seconds: float,
        workload: WorkloadSpec,
    ) -> Job:
        async with self._lock:
            if job_id in self._jobs:
                existing = self._jobs[job_id]
                raise AdmissionError(
                    RejectReason.DUPLICATE_JOB,
                    f"job_id {job_id!r} already exists with status "
                    f"{existing.status.value}; resubmission is rejected, "
                    f"query GET /jobs/{job_id} for its current state",
                )
            if resource < 1:
                raise AdmissionError(
                    RejectReason.INVALID_REQUEST,
                    f"resource must be >= 1, got {resource}",
                )
            if resource > self.config.cluster_capacity:
                raise AdmissionError(
                    RejectReason.CAPACITY_EXCEEDED,
                    f"job requests {resource} resources but cluster capacity is "
                    f"{self.config.cluster_capacity}; such a job can never run",
                )
            quota = self.config.quota_for(caller)
            if resource > quota:
                raise AdmissionError(
                    RejectReason.QUOTA_EXCEEDED,
                    f"job requests {resource} resources but caller {caller!r} "
                    f"quota is {quota}; single request may not exceed quota",
                )
            now = time.time()
            seq = next(self._seq)
            job = Job(
                job_id=job_id,
                caller=caller,
                resource=resource,
                priority=priority,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
                enqueue_seq=seq,
                enqueued_at=now,
                workload=workload.to_dict(),
            )
            self._jobs[job_id] = job
            logger.info(
                "submit accepted job_id=%s caller=%s resource=%d priority=%d "
                "retries=%d timeout=%.2fs seq=%d -> queued "
                "(capacity=%d used=%d caller_quota=%d caller_used=%d)",
                job_id, caller, resource, priority, max_retries,
                timeout_seconds, seq, self.config.cluster_capacity, self._used,
                quota, self._per_caller_used.get(caller, 0),
            )
            self._persist_locked()
            self._cond.notify_all()
        return job

    async def cancel(self, job_id: str, caller: str | None) -> Job:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise AdmissionError(
                    RejectReason.NOT_FOUND, f"no such job: {job_id!r}"
                )
            if caller is not None and job.caller != caller:
                raise AdmissionError(
                    RejectReason.NOT_FOUND,
                    "no such job for this caller",
                )
            if job.is_terminal:
                raise AdmissionError(
                    RejectReason.ALREADY_TERMINAL,
                    f"job {job_id} is already {job.status.value}"
                    + (
                        f" ({job.fail_reason.value})"
                        if job.fail_reason
                        else ""
                    )
                    + "; canceling a finished job has no effect",
                )
            if job.status is JobStatus.QUEUED:
                logger.info(
                    "cancel queued job_id=%s caller=%s wait_so_far=%.3fs",
                    job_id, job.caller, time.time() - job.enqueued_at,
                )
                job.status = JobStatus.CANCELED
                job.finished_at = time.time()
                job.fail_reason = FailureReason.CANCELED
                job.fail_detail = "canceled while queued"
                self._persist_locked()
                self._cond.notify_all()
                return job
            # RUNNING: request cooperative cancel; completion path finalizes.
            assert job.status is JobStatus.RUNNING
            job.cancel_requested = True
            task = self._tasks.get(job_id)
            logger.info(
                "cancel requested for running job_id=%s attempt=%d",
                job_id, job.attempts,
            )
            if task:
                task.cancel()
            self._persist_locked()
            return job

    # ------------------------------------------------------------------ #
    # scheduling
    # ------------------------------------------------------------------ #
    def _ordered_queue_locked(self) -> list[Job]:
        return sorted(
            (
                job
                for job in self._jobs.values()
                if job.status is JobStatus.QUEUED
            ),
            key=lambda j: (-j.priority, j.enqueue_seq),
        )

    async def _dispatch_pass_locked(self) -> int:
        """One stable, explainable dispatch pass.

        Order: highest priority first, then submission order (enqueue_seq).
        Within the queue we walk strictly in that order. A job is admitted
        only when BOTH the cluster and the caller's currently used resources
        leave room. On the *first* job that cannot be admitted we stop the
        pass: no later job (same or lower priority) can jump ahead, so the
        queue order is respected and decisions are easy to explain.
        """
        now = time.time()
        admitted = 0
        for job in self._ordered_queue_locked():
            if job.not_before > now:
                # Retry backoff: it and everything behind it wait.
                logger.info(
                    "dispatch stop: job_id=%s in retry backoff for %.3fs more",
                    job.job_id, job.not_before - now,
                )
                break
            caller_used = self._per_caller_used.get(job.caller, 0)
            quota = self.config.quota_for(job.caller)
            if self._used + job.resource > self.config.cluster_capacity:
                logger.info(
                    "dispatch stop: head job_id=%s needs=%d capacity_left=%d "
                    "(used=%d/%d); later jobs must not overtake it",
                    job.job_id, job.resource,
                    self.config.cluster_capacity - self._used,
                    self._used, self.config.cluster_capacity,
                )
                break
            if caller_used + job.resource > quota:
                logger.info(
                    "dispatch stop: head job_id=%s caller=%s needs=%d "
                    "quota_left=%d (used=%d/%d); FIFO order preserved",
                    job.job_id, job.caller, job.resource,
                    quota - caller_used, caller_used, quota,
                )
                break
            self._admit_locked(job, now)
            admitted += 1
        return admitted

    def _admit_locked(self, job: Job, now: float) -> None:
        job.status = JobStatus.RUNNING
        job.started_at = now
        job.attempts += 1
        self._used += job.resource
        self._per_caller_used[job.caller] = (
            self._per_caller_used.get(job.caller, 0) + job.resource
        )
        spec = WorkloadSpec.from_payload(job.workload)
        logger.info(
            "admit job_id=%s caller=%s attempt=%d resource=%d -> used=%d/%d "
            "caller_used=%d/%d",
            job.job_id, job.caller, job.attempts, job.resource,
            self._used, self.config.cluster_capacity,
            self._per_caller_used[job.caller],
            self.config.quota_for(job.caller),
        )
        task = asyncio.create_task(self._run_job(job.job_id, spec), name=f"job-{job.job_id}")
        task.add_done_callback(self._task_done)
        self._tasks[job.job_id] = task

    @staticmethod
    def _task_done(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("job task ended with unhandled exception: %r", exc)

    async def _run_loop(self) -> None:
        # The condition is bound to self._lock: wait_for() atomically releases
        # the lock while sleeping and re-acquires it when notified, so a
        # notification sent while we transition into waiting cannot be lost.
        async with self._cond:
            while not self._stopping:
                try:
                    await self._dispatch_pass_locked()
                    await asyncio.wait_for(
                        self._cond.wait(),
                        timeout=self.config.tick_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    # periodic tick: also covers retry backoff expirations
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover - defensive
                    logger.exception("scheduler loop error")
                    await asyncio.sleep(self.config.tick_interval_seconds)

    # ------------------------------------------------------------------ #
    # job execution / completion
    # ------------------------------------------------------------------ #
    async def _run_job(self, job_id: str, spec: WorkloadSpec) -> None:
        try:
            await asyncio.wait_for(
                run_workload(spec), timeout=self._timeout_for(job_id)
            )
        except asyncio.CancelledError:
            await self._finalize(job_id, canceled=True)
            raise
        except asyncio.TimeoutError:
            await self._finalize(job_id, failure=FailureReason.TIMEOUT,
                                 detail=f"exceeded timeout of {self._timeout_for(job_id):g}s")
        except WorkloadError as exc:
            await self._finalize(job_id, failure=FailureReason.WORKLOAD_FAILED,
                                 detail=str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            await self._finalize(job_id, failure=FailureReason.WORKLOAD_FAILED,
                                 detail=f"unexpected error: {exc!r}")
        else:
            await self._finalize(job_id, result=f"{spec.type}:{spec.duration_seconds:g}s")

    def _timeout_for(self, job_id: str) -> float:
        job = self._jobs.get(job_id)
        if job is not None:
            return job.timeout_seconds
        return self.config.default_timeout_seconds

    async def _release_resources_locked(self, job: Job) -> None:
        self._used -= job.resource
        if self._used < 0:  # pragma: no cover - invariant guard
            logger.error("resource accounting went negative for %s", job.job_id)
            self._used = 0
        caller_used = self._per_caller_used.get(job.caller, 0) - job.resource
        if caller_used <= 0:
            self._per_caller_used.pop(job.caller, None)
        else:
            self._per_caller_used[job.caller] = caller_used

    async def _finalize(
        self,
        job_id: str,
        result: str | None = None,
        failure: FailureReason | None = None,
        detail: str | None = None,
        canceled: bool = False,
    ) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            self._tasks.pop(job_id, None)
            if job is None or job.status is not JobStatus.RUNNING:
                logger.warning(
                    "finalize ignored for job_id=%s (state=%s) — stale callback",
                    job_id, job.status.value if job else "missing",
                )
                return
            # Resources are always released exactly once: every terminal
            # transition and every requeue goes through this method.
            await self._release_resources_locked(job)
            now = time.time()

            if canceled or job.cancel_requested:
                job.status = JobStatus.CANCELED
                job.finished_at = now
                job.fail_reason = FailureReason.CANCELED
                job.fail_detail = detail or "canceled while running"
                logger.info("job %s CANCELED, resource %d released, used=%d/%d",
                            job_id, job.resource, self._used,
                            self.config.cluster_capacity)
            elif failure is None:
                job.status = JobStatus.SUCCEEDED
                job.finished_at = now
                job.result = result
                # Clear any failure marker from a previous failed attempt or a
                # restart interruption: the final outcome is a success.
                job.fail_reason = None
                job.fail_detail = None
                logger.info("job %s SUCCEEDED attempt=%d, resource %d released, used=%d/%d",
                            job_id, job.attempts, job.resource, self._used,
                            self.config.cluster_capacity)
            else:
                can_retry = job.attempts <= job.max_retries
                if can_retry:
                    backoff = min(
                        RETRY_BACKOFF_CAP,
                        RETRY_BACKOFF_BASE * (2 ** (job.attempts - 1)),
                    )
                    job.status = JobStatus.QUEUED
                    job.started_at = None
                    job.not_before = now + backoff
                    job.fail_reason = failure
                    job.fail_detail = detail
                    logger.warning(
                        "job %s failed attempt=%d/%d reason=%s detail=%s -> requeued "
                        "with backoff %.3fs, resource %d released (not double held)",
                        job_id, job.attempts, job.max_retries + 1, failure.value,
                        detail, backoff, job.resource,
                    )
                else:
                    job.status = JobStatus.FAILED
                    job.finished_at = now
                    # Preserve the concrete last-attempt reason while marking
                    # that the retry budget is exhausted.
                    job.fail_reason = failure
                    job.fail_detail = (
                        f"{detail}; retries exhausted after {job.attempts} attempt(s)"
                    )
                    logger.error(
                        "job %s FAILED reason=%s after %d/%d attempts, resource %d released",
                        job_id, failure.value, job.attempts, job.max_retries + 1,
                        job.resource,
                    )
            self._persist_locked()
            self._cond.notify_all()

    # ------------------------------------------------------------------ #
    # reads
    # ------------------------------------------------------------------ #
    def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def list_jobs(
        self, caller: str | None, status: str | None, limit: int
    ) -> list[Job]:
        jobs = list(self._jobs.values())
        if caller is not None:
            jobs = [j for j in jobs if j.caller == caller]
        if status is not None:
            jobs = [j for j in jobs if j.status.value == status]
        jobs.sort(key=lambda j: j.enqueue_seq, reverse=True)
        return jobs[:limit]

    def stats(self) -> dict[str, Any]:
        counts = {s.value: 0 for s in JobStatus}
        running_ids: list[str] = []
        queued_ids: list[str] = []
        for job in self._jobs.values():
            counts[job.status.value] += 1
            if job.status is JobStatus.RUNNING:
                running_ids.append(job.job_id)
            elif job.status is JobStatus.QUEUED:
                queued_ids.append(job.job_id)
        running_ids.sort()
        queued_ids.sort(
            key=lambda jid: (
                -self._jobs[jid].priority,
                self._jobs[jid].enqueue_seq,
            )
        )
        return {
            "cluster_capacity": self.config.cluster_capacity,
            "used_resources": self._used,
            "available_resources": self.config.cluster_capacity - self._used,
            "running": counts[JobStatus.RUNNING.value],
            "queued": counts[JobStatus.QUEUED.value],
            "succeeded": counts[JobStatus.SUCCEEDED.value],
            "failed": counts[JobStatus.FAILED.value],
            "canceled": counts[JobStatus.CANCELED.value],
            "running_jobs": running_ids,
            "queued_jobs": queued_ids,
            "per_caller_running": dict(sorted(self._per_caller_used.items())),
        }
