"""Scheduling core: submit / query / cancel / dispatch loop / recovery.

Concurrency model: a single asyncio event loop and one :class:`asyncio.Lock`
guards every mutation of the job table. The dispatch loop only holds the lock
while changing bookkeeping; actual attempt execution awaits *without* the
lock. Resource usage is derived purely from job status (RUNNING), so releasing
a resource is just a status transition -- there is no separate counter that
could leak or be double-counted.
"""

from __future__ import annotations

import asyncio
import time

from .config import Config
from .executor import ExecutionOutcome, run_attempt
from .logging_setup import get_logger
from .models import (
    Attempt,
    CancelResult,
    FailureReason,
    Job,
    JobStatus,
)
from .quota import owner_usage, used_capacity, validate_submit
from .store import JobStore

_TERMINAL = {
    JobStatus.SUCCEEDED,
    JobStatus.FAILED,
    JobStatus.CANCELED,
    JobStatus.EXPIRED,
}


class Scheduler:
    def __init__(self, config: Config, store: JobStore):
        self.config = config
        self.store = store
        self.jobs: dict[str, Job] = {}
        self._lock: asyncio.Lock | None = None
        self._seq = 0
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None
        self._wake: asyncio.Event | None = None
        self._stopping = False
        self._shutdown_cancels: set[str] = set()
        self.log = get_logger()

    # ------------------------------------------------------------------ life

    async def start(self) -> None:
        # Lazily create loop-bound primitives so start() works even if the
        # scheduler object outlives the event loop it was constructed in.
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self._stopping = False
        self._shutdown_cancels = set()
        async with self._lock:
            self.jobs = self.store.load()
            self._recover_locked()
            self._seq = max((j.seq for j in self.jobs.values()), default=0)
            self._persist_locked()
        self.log.info(
            "scheduler started: capacity=%s quota_per_owner=%s recovered_jobs=%s",
            self.config.cluster_capacity,
            self.config.quota_per_owner,
            len(self.jobs),
        )
        self._loop_task = asyncio.create_task(self._dispatch_loop(), name="dispatch")

    async def stop(self) -> None:
        self._stopping = True

        # 1. Stop the dispatch loop FIRST so no new attempt can start while we
        #    are tearing down (otherwise a fresh RUNNING row could race the
        #    final snapshot and survive to the next start).
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

        # 2. Signal every in-flight attempt and wait for its task to finish.
        #    Cancellation during shutdown means "interrupted by restart", not
        #    a user-requested terminal CANCELED.
        async with self._lock:
            in_flight = [
                jid for jid, job in self.jobs.items()
                if job.status is JobStatus.RUNNING
            ]
            for jid in in_flight:
                self._shutdown_cancels.add(jid)
                event = self._cancel_events.get(jid)
                if event is not None:
                    event.set()
            tasks = [t for jid, t in self._tasks.items() if jid in in_flight]

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 3. Anything still RUNNING (attempts that did not observe the signal,
        #    e.g. a hard-killed event loop) is treated like an interrupted
        #    attempt across a restart: NOT a user CANCELED (terminal), but
        #    INTERRUPTED + re-queued, so it runs again without losing the job
        #    or charging a retry. The snapshot therefore contains no RUNNING.
        async with self._lock:
            now = time.time()
            for jid in in_flight:
                job = self.jobs.get(jid)
                if job is None or job.terminal:
                    continue
                was_running = job.status is JobStatus.RUNNING
                if was_running:
                    # Task did not run its finalize (hard cancellation).
                    self._mark_interrupted_locked(job, now)
                # Tasks that observed the signal already closed the attempt as
                # INTERRUPTED (via _run_job); here we only reset scheduling
                # state -- never decrement attempt_count a second time.
                job.status = JobStatus.WAITING
                job.started_at = None
                job.next_attempt_at = None
                job.enqueued_again_at = None
                job.cancel_requested = False
                if was_running:
                    self.log.warning(
                        "shutdown: job=%s attempt interrupted; re-queued for restart",
                        jid,
                    )
            self._cancel_events.clear()
            self._tasks.clear()
            self._persist_locked()
        self.log.info(
            "scheduler stopped; %d in-flight attempt(s) settled; state persisted",
            len(in_flight),
        )

    # -------------------------------------------------------------- recovery

    def _recover_locked(self) -> None:
        """Re-queue every non-terminal job after a restart.

        A clean shutdown persists in-flight attempts already closed as
        INTERRUPTED (and already uncharged). A hard crash may leave an open
        (unterminated) attempt; only then do we close and uncharge it here.
        """
        recovered = 0
        for job in self.jobs.values():
            if job.terminal:
                continue
            active = job.active_attempt
            if active is not None:
                self._mark_interrupted_locked(job, time.time())
            job.status = JobStatus.WAITING
            job.started_at = None
            job.next_attempt_at = None
            job.enqueued_again_at = None
            job.cancel_requested = False
            recovered += 1
            self.log.warning(
                "recovery: job=%s owner=%s re-queued (previous_status interrupted)",
                job.job_id, job.owner,
            )
        if recovered:
            self.log.warning("recovery: %d unfinished job(s) re-queued", recovered)

    def _mark_interrupted_locked(self, job: Job, now: float) -> None:
        """Close an in-flight attempt as INTERRUPTED.

        An interrupted attempt is an execution that did start, so it is kept in
        ``attempts`` and counted in ``attempt_count`` (attempts started). It is
        NOT a retryable failure, however: the retry budget is checked against
        FAILED/TIMEOUT attempts only (see ``_run_job``), so a restart never
        burns one of the caller's retries.

        Idempotent: no-op when the last attempt was already closed.
        """
        active = job.active_attempt
        if active is None:
            return
        active.ended_at = now
        active.outcome = "INTERRUPTED"
        active.detail = "interrupted by scheduler restart/shutdown; re-queued"

    # ---------------------------------------------------------------- submit

    def submit(self, *, name: str, owner: str, resource: object,
               priority: int = 0, duration: float = 0.1,
               fail_times: int = 0, timeout: float | None = None,
               queue_timeout: float | None = None,
               max_attempts: int | None = None,
               job_id: str | None = None) -> tuple[Job, bool]:
        """Validate and enqueue a job. Returns ``(job, duplicate)``.

        ``duplicate=True`` means the same client-supplied id was submitted
        before; the existing job is returned unchanged.
        """
        if self._stopping:
            raise RuntimeError("scheduler is shutting down")
        if job_id and job_id in self.jobs:
            existing = self.jobs[job_id]
            self.log.info(
                "submit duplicate idempotency-key=%s -> existing job=%s status=%s",
                job_id, existing.job_id, existing.status.value,
            )
            return existing, True

        resource = validate_submit(
            resource=resource,
            owner=owner,
            jobs=self.jobs,
            capacity=self.config.cluster_capacity,
            quota=self.config.quota_per_owner,
        )
        self._seq += 1
        attempts = max_attempts or self.config.default_attempts
        job = Job.new(
            name=name,
            owner=owner,
            resource=resource,
            priority=priority,
            seq=self._seq,
            duration=duration,
            fail_times=fail_times,
            timeout=timeout if timeout is not None else self.config.default_attempt_timeout,
            queue_timeout=(
                queue_timeout
                if queue_timeout is not None
                else self.config.default_queue_timeout
            ),
            max_attempts=attempts,
            job_id=job_id,
        )
        self.jobs[job.job_id] = job
        self._persist_locked()
        self.log.info(
            "submit accepted: job=%s name=%r owner=%s resource=%s priority=%s "
            "seq=%s (basis: capacity=%s used=%s owner_usage=%s quota=%s)",
            job.job_id, name, owner, resource, priority, job.seq,
            self.config.cluster_capacity, used_capacity(self.jobs),
            owner_usage(owner, self.jobs), self.config.quota_per_owner,
        )
        self._wake.set()
        return job, False

    # --------------------------------------------------------------- query /

    def get_job(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def cancel(self, job_id: str) -> tuple[Job, CancelResult]:
        job = self.jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if self._stopping:
            # During shutdown every in-flight attempt is canceled centrally.
            return job, CancelResult.CANCELING if job.status is JobStatus.RUNNING \
                else CancelResult.ALREADY_TERMINAL
        if job.terminal:
            self.log.info(
                "cancel rejected: job=%s already terminal status=%s",
                job_id, job.status.value,
            )
            return job, CancelResult.ALREADY_TERMINAL

        if job.status is JobStatus.RUNNING:
            job.cancel_requested = True
            event = self._cancel_events.get(job_id)
            if event is not None:
                event.set()
            self._persist_locked()
            self.log.info("cancel requested: job=%s status=RUNNING -> CANCELING", job_id)
            return job, CancelResult.CANCELING

        # WAITING / BACKOFF: it holds no resource, terminate immediately.
        prev = job.status.value
        job.status = JobStatus.CANCELED
        job.finished_at = time.time()
        job.fail_detail = f"canceled while {prev}"
        job.next_attempt_at = None
        self._persist_locked()
        self.log.info(
            "cancel done: job=%s %s -> CANCELED (no resource held)", job_id, prev,
        )
        self._wake.set()
        return job, CancelResult.CANCELED

    def snapshot(self) -> dict:
        running = [j for j in self.jobs.values() if j.status is JobStatus.RUNNING]
        by_status: dict[str, int] = {}
        owner_used: dict[str, int] = {}
        for j in self.jobs.values():
            by_status[j.status.value] = by_status.get(j.status.value, 0) + 1
            if not j.terminal:
                owner_used[j.owner] = owner_used.get(j.owner, 0) + j.resource
        return {
            "cluster_capacity": self.config.cluster_capacity,
            "used_capacity": used_capacity(self.jobs),
            "available": self.config.cluster_capacity - used_capacity(self.jobs),
            "quota_per_owner": self.config.quota_per_owner,
            "owner_usage": owner_used,
            "jobs_by_status": by_status,
            "running_job_ids": [j.job_id for j in running],
            "total_jobs": len(self.jobs),
        }

    # --------------------------------------------------------- dispatch loop

    async def _dispatch_loop(self) -> None:
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # the loop must never die on a single bad tick
                self.log.exception("dispatch tick error")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.config.tick_interval)
                self._wake.clear()
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        if self._stopping:
            return
        async with self._lock:
            if self._stopping:
                return
            now = time.time()
            self._expire_waiting_locked(now)
            self._reactivate_backoff_locked(now)
            self._dispatch_locked(now)

    def _expire_waiting_locked(self, now: float) -> None:
        for job in list(self.jobs.values()):
            if job.status is not JobStatus.WAITING or job.queue_timeout is None:
                continue
            queue_since = job.enqueued_again_at or job.created_at
            waited = now - queue_since
            if waited >= job.queue_timeout:
                job.status = JobStatus.EXPIRED
                job.fail_reason = FailureReason.QUEUE_TIMEOUT
                job.fail_detail = (
                    f"queued {waited:.3f}s >= queue_timeout {job.queue_timeout}s"
                )
                job.finished_at = now
                self.log.warning(
                    "expire: job=%s owner=%s waited=%.3fs queue_timeout=%s "
                    "-> EXPIRED(QUEUE_TIMEOUT)",
                    job.job_id, job.owner, waited, job.queue_timeout,
                )

    def _reactivate_backoff_locked(self, now: float) -> None:
        for job in self.jobs.values():
            if (
                job.status is JobStatus.BACKOFF
                and job.next_attempt_at is not None
                and now >= job.next_attempt_at
            ):
                job.status = JobStatus.WAITING
                job.enqueued_again_at = now
                job.next_attempt_at = None
                self.log.info(
                    "retry due: job=%s re-queued at seq=%s priority=%s "
                    "(attempts_used=%s/%s)",
                    job.job_id, job.seq, job.priority,
                    job.attempt_count, job.max_attempts,
                )

    def _dispatch_locked(self, now: float) -> None:
        waiting = [
            j for j in self.jobs.values() if j.status is JobStatus.WAITING
        ]
        if not waiting:
            return
        # Global dispatch order: priority desc, then submission seq asc.
        waiting.sort(key=lambda j: (-j.priority, j.seq))

        available = self.config.cluster_capacity - used_capacity(self.jobs)
        # Walk priority level by priority level so that a blocked job at the
        # head of a priority stops that priority and every lower one too:
        # same-priority jobs may never jump the queue, and lower-priority jobs
        # must not steal the resources a waiting higher-priority job needs.
        blocked = False
        for job in waiting:
            if blocked:
                break
            if job.resource <= available:
                self._start_attempt_locked(job, now)
                available -= job.resource
            else:
                self.log.info(
                    "hold: job=%s owner=%s resource=%s does not fit available=%s "
                    "(priority=%s seq=%s); lower/same-priority dispatch paused",
                    job.job_id, job.owner, job.resource, available,
                    job.priority, job.seq,
                )
                blocked = True

    def _start_attempt_locked(self, job: Job, now: float) -> None:
        job.attempt_count += 1
        job.status = JobStatus.RUNNING
        job.started_at = now
        if job.enqueued_again_at is not None:
            job.enqueued_again_at = None
        attempt = Attempt(attempt_no=job.attempt_count, started_at=now)
        job.attempts.append(attempt)
        event = asyncio.Event()
        self._cancel_events[job.job_id] = event
        self._persist_locked()
        self.log.info(
            "dispatch: job=%s owner=%s resource=%s priority=%s seq=%s "
            "attempt=%s/%s (basis: capacity=%s used_after=%s)",
            job.job_id, job.owner, job.resource, job.priority, job.seq,
            job.attempt_count, job.max_attempts,
            self.config.cluster_capacity, used_capacity(self.jobs),
        )
        task = asyncio.create_task(self._run_job(job.job_id, event))
        self._tasks[job.job_id] = task

    async def _run_job(self, job_id: str, event: asyncio.Event) -> None:
        job = self.jobs[job_id]
        attempt_no = job.attempt_count
        outcome, detail = await run_attempt(job, event, attempt_no=attempt_no)

        async with self._lock:
            # The job may have vanished only in theory; guard anyway.
            job = self.jobs.get(job_id)
            if job is None:
                return
            attempt = job.active_attempt
            now = time.time()
            if attempt is not None:
                attempt.ended_at = now
                attempt.outcome = outcome
                attempt.detail = detail
            self._cancel_events.pop(job_id, None)
            self._tasks.pop(job_id, None)

            if outcome == ExecutionOutcome.CANCELED:
                if job_id in self._shutdown_cancels:
                    # Process is shutting down: the attempt was interrupted,
                    # not user-canceled. Re-queue without charging a retry.
                    self._mark_interrupted_locked(job, now)
                    attempt.outcome = "INTERRUPTED"
                    attempt.detail = "interrupted by scheduler shutdown; re-queued"
                    job.status = JobStatus.WAITING
                    job.started_at = None
                    job.cancel_requested = False
                    self.log.warning(
                        "shutdown: job=%s attempt interrupted; re-queued for restart",
                        job_id,
                    )
                else:
                    job.status = JobStatus.CANCELED
                    job.finished_at = now
                    # cancel_requested stays True as the audit trail.
                    self.log.info(
                        "finalize: job=%s -> CANCELED; resource released", job_id,
                    )

            elif outcome == ExecutionOutcome.SUCCESS:
                job.status = JobStatus.SUCCEEDED
                job.finished_at = now
                self.log.info(
                    "finalize: job=%s -> SUCCEEDED on attempt %s; resource released",
                    job_id, attempt_no,
                )

            elif self._retryable_failures(job) >= job.max_attempts:
                # The just-finished attempt is the max_attempts-th
                # FAILED/TIMEOUT: the retry budget is exhausted. (INTERRUPTED
                # attempts never enter this count.)
                job.status = JobStatus.FAILED
                job.finished_at = now
                job.fail_reason = (
                    FailureReason.ATTEMPT_TIMEOUT
                    if outcome == ExecutionOutcome.TIMEOUT
                    else FailureReason.ATTEMPT_FAILED
                )
                job.fail_detail = detail
                self.log.warning(
                    "finalize: job=%s -> FAILED reason=%s after %s/%s failed "
                    "attempts: %s",
                    job_id, job.fail_reason.value,
                    self._retryable_failures(job),
                    job.max_attempts, detail,
                )

            else:
                # Retry: release everything first (status leaves RUNNING), then
                # wait in BACKOFF. No resource is held during backoff.
                job.status = JobStatus.BACKOFF
                job.started_at = None
                job.next_attempt_at = now + self.config.retry_backoff
                self.log.warning(
                    "retry: job=%s attempt=%s/%s outcome=%s (%s); resource "
                    "released, backoff=%.2fs then re-queue at seq=%s",
                    job_id, attempt_no, job.max_attempts, outcome, detail,
                    self.config.retry_backoff, job.seq,
                )

            self._persist_locked()
            self._wake.set()

    # ------------------------------------------------------------- internals

    @staticmethod
    def _retryable_failures(job: Job) -> int:
        """Count attempts that ended FAILED or TIMEOUT (consume retry budget)."""
        return sum(
            1 for a in job.attempts
            if a.ended_at is not None and a.outcome in ("FAILED", "TIMEOUT")
        )

    def _persist_locked(self) -> None:
        self.store.save(self.jobs)
