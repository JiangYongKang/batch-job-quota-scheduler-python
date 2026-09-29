"""HTTP layer: submit / query / cancel / status endpoints.

The scheduler is injected (dependency inversion) so tests can build an app
backed by a temp-file store. Lifespan starts/stops the dispatch loop.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, StrictInt

from .errors import SubmitError
from .models import Job, JobStatus, RejectReason
from .scheduler import Scheduler


class SubmitRequest(BaseModel):
    name: str = Field(default="job", description="human-readable job name")
    owner: str = Field(description="caller identity; quota subject")
    resource: StrictInt = Field(description="required resource units (positive int)")
    priority: int = 0
    duration: float = 0.1
    fail_times: int = 0
    timeout: float | None = None
    queue_timeout: float | None = None
    max_attempts: int | None = None
    job_id: str | None = Field(default=None, description="client idempotency key")


def _job_to_dict(job: Job, *, now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    queue_since = job.enqueued_again_at or job.created_at
    data: dict[str, Any] = {
        "job_id": job.job_id,
        "name": job.name,
        "owner": job.owner,
        "resource": job.resource,
        "priority": job.priority,
        "seq": job.seq,
        "status": job.status.value,
        "attempt_count": job.attempt_count,
        "max_attempts": job.max_attempts,
        "fail_reason": job.fail_reason.value if job.fail_reason else None,
        "fail_detail": job.fail_detail,
        "cancel_requested": job.cancel_requested,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "next_attempt_at": job.next_attempt_at,
        "attempts": [
            {
                "attempt_no": a.attempt_no,
                "started_at": a.started_at,
                "ended_at": a.ended_at,
                "outcome": a.outcome,
                "detail": a.detail,
            }
            for a in job.attempts
        ],
    }
    # Explainability for "queued too long": queue position + waited seconds.
    data["waited_seconds"] = round(now - queue_since, 6)
    return data


def create_app(scheduler: Scheduler) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await scheduler.start()
        try:
            yield
        finally:
            await scheduler.stop()

    app = FastAPI(
        title="Local Batch Job Quota Scheduler",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.exception_handler(SubmitError)
    async def _submit_error_handler(_request, exc: SubmitError):
        return JSONResponse(
            status_code=400,
            content={
                "error": "SUBMIT_REJECTED",
                "reason": exc.reason.value,
                "message": exc.message,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(_request, exc: RequestValidationError):
        # Normalize request-shape errors to the same envelope. A bad resource
        # is reported as INVALID_RESOURCE so callers have one consistent code.
        errors = exc.errors()
        fields = {tuple(err.get("loc", [])): err.get("msg", "invalid")
                  for err in errors}
        resource_err = next(
            (msg for loc, msg in fields.items()
             if loc and loc[-1] == "resource"),
            None,
        )
        if resource_err is not None:
            reason = RejectReason.INVALID_RESOURCE.value
            message = f"resource must be a positive integer ({resource_err})"
        else:
            reason = "INVALID_REQUEST"
            message = errors[0].get("msg", "invalid request") if errors else "invalid request"
        return JSONResponse(
            status_code=400,
            content={"error": "SUBMIT_REJECTED", "reason": reason,
                     "message": message, "details": errors},
        )

    @app.exception_handler(KeyError)
    async def _not_found_handler(_request, exc: KeyError):
        return JSONResponse(
            status_code=404,
            content={"error": "NOT_FOUND", "job_id": exc.args[0]},
        )

    @app.get("/")
    async def root() -> dict:
        return {"service": "batch-job-quota-scheduler", "status": "ok"}

    @app.post("/jobs", status_code=201)
    async def submit_job(req: SubmitRequest):
        job, duplicate = scheduler.submit(
            name=req.name,
            owner=req.owner,
            resource=req.resource,
            priority=req.priority,
            duration=req.duration,
            fail_times=req.fail_times,
            timeout=req.timeout,
            queue_timeout=req.queue_timeout,
            max_attempts=req.max_attempts,
            job_id=req.job_id,
        )
        return {
            "job": _job_to_dict(job),
            "duplicate": duplicate,
            "message": "existing job returned for duplicate idempotency key"
            if duplicate else "accepted and queued",
        }

    @app.get("/jobs")
    async def list_jobs(owner: str | None = None,
                        status: JobStatus | None = None) -> dict:
        now = time.time()
        jobs = list(scheduler.jobs.values())
        if owner is not None:
            jobs = [j for j in jobs if j.owner == owner]
        if status is not None:
            jobs = [j for j in jobs if j.status is status]
        jobs.sort(key=lambda j: j.seq)
        return {"jobs": [_job_to_dict(j, now=now) for j in jobs], "count": len(jobs)}

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict:
        job = scheduler.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return _job_to_dict(job)

    @app.post("/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str) -> dict:
        job, result = scheduler.cancel(job_id)
        return {"job_id": job_id, "result": result.value, "job": _job_to_dict(job)}

    @app.get("/status")
    async def status() -> dict:
        return scheduler.snapshot()

    return app
