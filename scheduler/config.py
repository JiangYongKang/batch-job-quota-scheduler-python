"""Static configuration: cluster capacity, per-owner quota, timeouts."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    cluster_capacity: int            # total resource units of the cluster
    quota_per_owner: int             # max non-terminal resource units per caller
    default_attempts: int            # max execution attempts per job
    default_attempt_timeout: float | None  # default per-attempt timeout (s)
    default_queue_timeout: float | None    # default max queue wait (s)
    retry_backoff: float             # delay between retry attempts (s)
    tick_interval: float             # scheduler loop period (s)
    store_path: str                  # JSON snapshot path for recovery

    @staticmethod
    def from_env() -> "Config":
        def _int(name: str, default: int) -> int:
            raw = os.environ.get(name)
            return int(raw) if raw not in (None, "") else default

        def _float(name: str, default: float | None) -> float | None:
            raw = os.environ.get(name)
            if raw in (None, ""):
                return default
            value = float(raw)
            return value if value > 0 else None

        return Config(
            cluster_capacity=_int("SCHED_CLUSTER_CAPACITY", 10),
            quota_per_owner=_int("SCHED_QUOTA_PER_OWNER", 6),
            default_attempts=_int("SCHED_DEFAULT_ATTEMPTS", 3),
            default_attempt_timeout=_float("SCHED_ATTEMPT_TIMEOUT", 30.0),
            default_queue_timeout=_float("SCHED_QUEUE_TIMEOUT", None),
            retry_backoff=float(os.environ.get("SCHED_RETRY_BACKOFF", "0.2")),
            tick_interval=float(os.environ.get("SCHED_TICK", "0.02")),
            store_path=os.environ.get("SCHED_STORE_PATH", "data/scheduler.json"),
        )
