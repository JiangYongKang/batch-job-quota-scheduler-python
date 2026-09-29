"""FastAPI HTTP layer for the local batch-job scheduler."""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from scheduler.config import SchedulerConfig
from scheduler.engine import AdmissionError, SchedulerEngine
from scheduler.models import JobStatus, RejectReason
from scheduler.persistence import StateStore
from scheduler.schemas import (
    CancelResponse,
    ClusterStats,
    ErrorResponse,
    JobSubmitRequest,
    JobView,
    SubmitResponse,
)
from scheduler.workload import WorkloadSpec

logger = logging.getLogger("scheduler.api")

REJECTION_STATUS = {
    RejectReason.INVALID_REQUEST: 400,
    RejectReason.CAPACITY_EXCEEDED: 422,
    RejectReason.QUOTA_EXCEEDED: 422,
    RejectReason.DUPLICATE_JOB: 409,
    RejectReason.NOT_FOUND: 404,
    RejectReason.ALREADY_TERMINAL: 409,
}

VALID_STATUSES = {s.value for s in JobStatus}


def create_app(config: SchedulerConfig | None = None) -> FastAPI:
    config = config or SchedulerConfig.from_env()
    engine = SchedulerEngine(config, StateStore(config.state_file))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await engine.start()
        try:
            yield
        finally:
            await engine.stop()

    app = FastAPI(
        title="Local Batch Job Quota Scheduler",
        version="0.1.0",
        lifespan=lifespan,
    )

    def view(job) -> JobView:
        return JobView.from_job(job, time.time(), config.stale_queue_seconds)

    def raise_admission(exc: AdmissionError, job=None) -> None:
        status_code = REJECTION_STATUS[exc.reason]
        logger.info(
            "reject request reason=%s http=%d detail=%s",
            exc.reason.value, status_code, exc.detail,
        )
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": "request_rejected",
                "reason": exc.reason.value,
                "detail": str(exc.detail),
                "job": view(job).model_dump() if job is not None else None,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ):
        # Normalize schema-level rejections (bad resource/priority/workload…)
        # into the same error envelope as admission rejections so clients can
        # branch on ``reason``. The first failing field is named in detail.
        first = exc.errors()[0] if exc.errors() else {}
        loc = ".".join(str(p) for p in first.get("loc", []) if p != "body")
        detail = f"{loc}: {first.get('msg', 'invalid request')}".strip(": ")
        logger.info("reject request reason=invalid_request http=422 detail=%s",
                    detail)
        return JSONResponse(
            status_code=422,
            content={
                "detail": {
                    "error": "request_rejected",
                    "reason": "invalid_request",
                    "detail": detail,
                    "job": None,
                    "validation_errors": exc.errors(),
                }
            },
        )

    @app.get("/")
    async def root():
        return {
            "service": "batch-job-quota-scheduler",
            "capacity": config.cluster_capacity,
            "docs": "/docs",
        }

    @app.post("/jobs", response_model=SubmitResponse, status_code=201)
    async def submit_job(
        payload: JobSubmitRequest,
        x_caller: str = Header(default="anonymous", alias="X-Caller"),
    ):
        timeout = payload.timeout_seconds
        if timeout is None:
            timeout = config.default_timeout_seconds
        if timeout > config.max_timeout_seconds:
            raise_admission(
                AdmissionError(
                    RejectReason.INVALID_REQUEST,
                    f"timeout_seconds {timeout:g} exceeds maximum "
                    f"{config.max_timeout_seconds:g}",
                )
            )
        caller = x_caller.strip() or "anonymous"
        try:
            job = await engine.submit(
                job_id=payload.job_id,
                caller=caller,
                resource=payload.resource,
                priority=payload.priority,
                max_retries=payload.max_retries,
                timeout_seconds=timeout,
                workload=WorkloadSpec.from_request(payload.workload),
            )
        except AdmissionError as exc:
            raise_admission(exc)
        return SubmitResponse(accepted=True, queued=True, job=view(job))

    @app.get("/jobs", response_model=list[JobView])
    async def list_jobs(
        x_caller: str | None = Header(default=None, alias="X-Caller"),
        status: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=500),
    ):
        if status is not None and status not in VALID_STATUSES:
            raise_admission(
                AdmissionError(
                    RejectReason.INVALID_REQUEST,
                    f"unknown status filter {status!r}; valid: "
                    f"{sorted(VALID_STATUSES)}",
                )
            )
        jobs = engine.list_jobs(x_caller, status, limit)
        return [view(j) for j in jobs]

    @app.get("/jobs/{job_id}", response_model=JobView)
    async def get_job(job_id: str):
        job = engine.get_job(job_id)
        if job is None:
            raise_admission(AdmissionError(
                RejectReason.NOT_FOUND, f"no such job: {job_id!r}"))
        return view(job)

    @app.post("/jobs/{job_id}/cancel", response_model=CancelResponse)
    async def cancel_job(
        job_id: str,
        x_caller: str | None = Header(default=None, alias="X-Caller"),
    ):
        job = engine.get_job(job_id)
        if job is None:
            raise_admission(AdmissionError(
                RejectReason.NOT_FOUND, f"no such job: {job_id!r}"))
        previous_status = job.status.value
        try:
            updated = await engine.cancel(job_id, x_caller)
        except AdmissionError as exc:
            raise_admission(exc, job=engine.get_job(job_id))
        # For a running job the cancel is requested; final state settles
        # asynchronously in the completion path.
        canceled_now = updated.status.value == JobStatus.CANCELED.value
        return CancelResponse(
            canceled=canceled_now,
            previous_status=previous_status,
            job=view(updated),
        )

    @app.get("/stats", response_model=ClusterStats)
    async def get_stats():
        return ClusterStats(**engine.stats())

    # Exposed for tests that want to drive the engine directly.
    app.state.engine = engine
    app.state.config = config
    return app
