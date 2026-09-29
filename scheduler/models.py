"""Domain model: job state machine plus rejection/failure reason taxonomy."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


TERMINAL_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELED}
)


class RejectReason(str, enum.Enum):
    """Why a submit/cancel request was rejected (HTTP 4xx)."""

    INVALID_REQUEST = "invalid_request"
    CAPACITY_EXCEEDED = "capacity_exceeded"
    QUOTA_EXCEEDED = "quota_exceeded"
    DUPLICATE_JOB = "duplicate_job"
    NOT_FOUND = "not_found"
    ALREADY_TERMINAL = "already_terminal"


class FailureReason(str, enum.Enum):
    """Why a job ended without succeeding (persisted on the job record)."""

    WORKLOAD_FAILED = "workload_failed"
    TIMEOUT = "timeout"
    RETRIES_EXHAUSTED = "retries_exhausted"
    CANCELED = "canceled"
    RESTART_INTERRUPTED = "restart_interrupted"


@dataclass
class Job:
    job_id: str
    caller: str
    resource: int
    priority: int
    max_retries: int
    timeout_seconds: float
    enqueue_seq: int
    enqueued_at: float
    workload: dict[str, Any]
    status: JobStatus = JobStatus.QUEUED
    attempts: int = 0
    started_at: float | None = None
    finished_at: float | None = None
    fail_reason: FailureReason | None = None
    fail_detail: str | None = None
    result: str | None = None
    cancel_requested: bool = False
    # Wall-clock time before which the job must not be dispatched (retry backoff).
    not_before: float = 0.0

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def wait_seconds(self, now: float) -> float | None:
        """Time spent in the queue since first submission, while still queued."""
        if self.status is JobStatus.QUEUED:
            return max(0.0, now - self.enqueued_at)
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "caller": self.caller,
            "resource": self.resource,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
            "enqueue_seq": self.enqueue_seq,
            "enqueued_at": self.enqueued_at,
            "workload": self.workload,
            "status": self.status.value,
            "attempts": self.attempts,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "fail_reason": self.fail_reason.value if self.fail_reason else None,
            "fail_detail": self.fail_detail,
            "result": self.result,
            "cancel_requested": self.cancel_requested,
            "not_before": self.not_before,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Job":
        return cls(
            job_id=data["job_id"],
            caller=data["caller"],
            resource=int(data["resource"]),
            priority=int(data["priority"]),
            max_retries=int(data["max_retries"]),
            timeout_seconds=float(data["timeout_seconds"]),
            enqueue_seq=int(data["enqueue_seq"]),
            enqueued_at=float(data["enqueued_at"]),
            workload=dict(data.get("workload") or {}),
            status=JobStatus(data["status"]),
            attempts=int(data.get("attempts", 0)),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            fail_reason=(
                FailureReason(data["fail_reason"])
                if data.get("fail_reason")
                else None
            ),
            fail_detail=data.get("fail_detail"),
            result=data.get("result"),
            cancel_requested=bool(data.get("cancel_requested", False)),
            not_before=float(data.get("not_before", 0.0)),
        )
