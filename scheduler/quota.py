"""Capacity & quota accounting."""

from __future__ import annotations

from .errors import SubmitError
from .models import Job, JobStatus, RejectReason


def validate_submit(*, resource: object, owner: str, jobs: dict[str, Job],
                    capacity: int, quota: int) -> int:
    """Validate a resource request.

    Returns the normalized positive-int resource, or raises SubmitError with
    one of three distinguishable reasons:
      INVALID_RESOURCE / EXCEEDS_CAPACITY / EXCEEDS_QUOTA.
    """
    # bool is a subclass of int but is not a meaningful resource amount.
    if not isinstance(resource, int) or isinstance(resource, bool) or resource <= 0:
        raise SubmitError(
            RejectReason.INVALID_RESOURCE,
            f"resource must be a positive integer, got {resource!r}",
        )

    if resource > capacity:
        raise SubmitError(
            RejectReason.EXCEEDS_CAPACITY,
            f"requested {resource} > cluster capacity {capacity}; "
            "it can never be scheduled",
        )

    in_use = owner_usage(owner, jobs)
    if in_use + resource > quota:
        raise SubmitError(
            RejectReason.EXCEEDS_QUOTA,
            f"owner {owner!r} requests {resource}; already using {in_use}, "
            f"quota {quota} (would be {in_use + resource})",
        )
    return resource


def used_capacity(jobs: dict[str, Job]) -> int:
    """Resource units currently held = resources of RUNNING jobs only."""
    return sum(j.resource for j in jobs.values() if j.status is JobStatus.RUNNING)


def owner_usage(owner: str, jobs: dict[str, Job]) -> int:
    """Resource units counted against an owner's quota = all non-terminal jobs."""
    return sum(
        j.resource
        for j in jobs.values()
        if j.owner == owner and not j.terminal
    )
