"""Pydantic request/response schemas for the HTTP API."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from scheduler.models import Job

# Bounds kept generous; admission still enforces cluster/quota constraints.
MIN_PRIORITY = 0
MAX_PRIORITY = 100
MAX_RETRIES_HARD_CAP = 10
MIN_RESOURCE = 1
MAX_RESOURCE = 10_000


class WorkloadRequest(BaseModel):
    """Deterministic simulated work. No network or real credentials are used."""

    type: Literal["sleep", "succeed", "fail"]
    duration_seconds: float = Field(default=0.0, ge=0.0, le=3600.0)


class JobSubmitRequest(BaseModel):
    job_id: str = Field(min_length=1, max_length=128)
    resource: int = Field(ge=MIN_RESOURCE, le=MAX_RESOURCE)
    priority: int = Field(default=10, ge=MIN_PRIORITY, le=MAX_PRIORITY)
    max_retries: int = Field(default=0, ge=0, le=MAX_RETRIES_HARD_CAP)
    timeout_seconds: float | None = Field(default=None, gt=0.0, le=3600.0)
    workload: WorkloadRequest


class JobView(BaseModel):
    job_id: str
    caller: str
    resource: int
    priority: int
    max_retries: int
    timeout_seconds: float
    status: str
    attempts: int
    enqueue_seq: int
    enqueued_at: float
    started_at: float | None
    finished_at: float | None
    wait_seconds: float | None = None
    stale: bool = False
    fail_reason: str | None = None
    fail_detail: str | None = None
    result: str | None = None
    cancel_requested: bool = False
    workload: dict[str, Any]

    @classmethod
    def from_job(cls, job: Job, now: float, stale_after: float) -> "JobView":
        wait = job.wait_seconds(now)
        return cls(
            job_id=job.job_id,
            caller=job.caller,
            resource=job.resource,
            priority=job.priority,
            max_retries=job.max_retries,
            timeout_seconds=job.timeout_seconds,
            status=job.status.value,
            attempts=job.attempts,
            enqueue_seq=job.enqueue_seq,
            enqueued_at=job.enqueued_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            wait_seconds=wait,
            stale=bool(wait is not None and wait >= stale_after),
            fail_reason=job.fail_reason.value if job.fail_reason else None,
            fail_detail=job.fail_detail,
            result=job.result,
            cancel_requested=job.cancel_requested,
            workload=job.workload,
        )


class SubmitResponse(BaseModel):
    accepted: bool
    queued: bool
    job: JobView


class CancelResponse(BaseModel):
    canceled: bool
    previous_status: str
    job: JobView


class ErrorResponse(BaseModel):
    error: str
    reason: str
    detail: str | None = None
    job: JobView | None = None


class ClusterStats(BaseModel):
    cluster_capacity: int
    used_resources: int
    available_resources: int
    running: int
    queued: int
    succeeded: int
    failed: int
    canceled: int
    running_jobs: list[str]
    queued_jobs: list[str]
    per_caller_running: dict[str, int]
