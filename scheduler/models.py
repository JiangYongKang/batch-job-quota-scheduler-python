"""Domain models: status / reason enums and the Job data model."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class JobStatus(str, Enum):
    WAITING = "WAITING"          # queued, not yet dispatched
    RUNNING = "RUNNING"          # an attempt is executing
    SUCCEEDED = "SUCCEEDED"      # terminal: finished successfully
    FAILED = "FAILED"            # terminal: all retries exhausted
    CANCELED = "CANCELED"        # terminal: canceled by caller
    EXPIRED = "EXPIRED"          # terminal: waited in queue too long
    # transient state between failed attempts:
    BACKOFF = "BACKOFF"          # waiting before next retry attempt


class RejectReason(str, Enum):
    """Distinguishable reasons for rejecting a submission (HTTP 400)."""
    INVALID_RESOURCE = "INVALID_RESOURCE"      # resource missing / <=0 / wrong type
    EXCEEDS_CAPACITY = "EXCEEDS_CAPACITY"      # > total cluster capacity
    EXCEEDS_QUOTA = "EXCEEDS_QUOTA"            # caller already at its quota


class FailureReason(str, Enum):
    """Distinguishable reasons for a terminal failure."""
    ATTEMPT_FAILED = "ATTEMPT_FAILED"          # task raised / returned failure, retries used up
    ATTEMPT_TIMEOUT = "ATTEMPT_TIMEOUT"        # last allowed attempt timed out
    QUEUE_TIMEOUT = "QUEUE_TIMEOUT"            # expired while waiting in queue


class CancelResult(str, Enum):
    CANCELED = "CANCELED"                      # was WAITING/BACKOFF, canceled directly
    CANCELING = "CANCELING"                    # was RUNNING, cancel requested
    ALREADY_TERMINAL = "ALREADY_TERMINAL"      # already in a terminal state


@dataclass
class Attempt:
    """One execution attempt of a job (for audit / explanations)."""
    attempt_no: int
    started_at: float
    ended_at: float | None = None
    outcome: str | None = None                 # SUCCESS / FAILED / TIMEOUT / CANCELED / INTERRUPTED
    detail: str = ""


@dataclass
class Job:
    job_id: str
    name: str
    owner: str                     # caller identity (quota subject)
    resource: int                  # requested resource units
    priority: int                  # larger = more important
    seq: int                       # global submission sequence (FIFO tie-break)
    created_at: float

    # simulation knobs (local, deterministic, no external services)
    duration: float = 0.1
    fail_times: int = 0            # fail the first N attempts, then succeed
    timeout: float | None = None   # per-attempt execution timeout
    queue_timeout: float | None = None  # max time allowed waiting in queue
    max_attempts: int = 3          # retry ceiling (total attempts, incl. first)

    status: JobStatus = JobStatus.WAITING
    attempts: list[Attempt] = field(default_factory=list)
    attempt_count: int = 0
    fail_reason: FailureReason | None = None
    fail_detail: str = ""
    cancel_requested: bool = False

    # timestamps for explainability
    started_at: float | None = None
    finished_at: float | None = None
    next_attempt_at: float | None = None     # set while in BACKOFF
    enqueued_again_at: float | None = None   # when a backoff job re-enters the queue head

    @classmethod
    def new(cls, *, name: str, owner: str, resource: int, priority: int,
            seq: int, duration: float = 0.1, fail_times: int = 0,
            timeout: float | None = None,
            queue_timeout: float | None = None,
            max_attempts: int | None = None,
            job_id: str | None = None) -> "Job":
        return cls(
            job_id=job_id or uuid.uuid4().hex[:12],
            name=name,
            owner=owner,
            resource=resource,
            priority=priority,
            seq=seq,
            created_at=time.time(),
            duration=duration,
            fail_times=fail_times,
            timeout=timeout,
            queue_timeout=queue_timeout,
            max_attempts=max_attempts if max_attempts is not None else 3,
        )

    @property
    def terminal(self) -> bool:
        return self.status in (
            JobStatus.SUCCEEDED, JobStatus.FAILED,
            JobStatus.CANCELED, JobStatus.EXPIRED,
        )

    @property
    def in_queue(self) -> bool:
        """Waiting to be scheduled (resource not held)."""
        return self.status in (JobStatus.WAITING, JobStatus.BACKOFF)

    @property
    def active_attempt(self) -> Attempt | None:
        if self.attempts and self.attempts[-1].ended_at is None:
            return self.attempts[-1]
        return None
