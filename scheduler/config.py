"""Scheduler configuration loaded from environment variables.

Environment variables (all optional):
  SCHEDULER_CLUSTER_CAPACITY   total cluster resources (integer slots, default 8)
  SCHEDULER_DEFAULT_QUOTA      per-caller quota when not listed explicitly (8)
  SCHEDULER_QUOTAS             JSON object mapping caller -> quota, e.g. {"alice": 4}
  SCHEDULER_DEFAULT_TIMEOUT    workload timeout in seconds (30)
  SCHEDULER_MAX_TIMEOUT        upper bound a single job may request (300)
  SCHEDULER_STALE_QUEUE_SECONDS  queued longer than this is flagged "stale" (60)
  SCHEDULER_STATE_FILE         path of the JSON snapshot (data/scheduler-state.json)
  SCHEDULER_TICK_INTERVAL      scheduler loop tick interval in seconds (0.1)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


@dataclass(frozen=True)
class SchedulerConfig:
    cluster_capacity: int = 8
    default_quota: int = 8
    quotas: dict[str, int] = field(default_factory=dict)
    default_timeout_seconds: float = 30.0
    max_timeout_seconds: float = 300.0
    stale_queue_seconds: float = 60.0
    state_file: str = "data/scheduler-state.json"
    tick_interval_seconds: float = 0.1

    def quota_for(self, caller: str) -> int:
        return int(self.quotas.get(caller, self.default_quota))

    @classmethod
    def from_env(cls) -> "SchedulerConfig":
        capacity = _env_int("SCHEDULER_CLUSTER_CAPACITY", 8)
        default_quota = _env_int("SCHEDULER_DEFAULT_QUOTA", capacity)
        raw_quotas = os.environ.get("SCHEDULER_QUOTAS", "").strip()
        quotas: dict[str, int] = {}
        if raw_quotas:
            try:
                parsed = json.loads(raw_quotas)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"SCHEDULER_QUOTAS must be a JSON object, got {raw_quotas!r}"
                ) from exc
            if not isinstance(parsed, dict):
                raise ValueError("SCHEDULER_QUOTAS must be a JSON object")
            for name, value in parsed.items():
                if not isinstance(name, str) or not isinstance(value, int) or value <= 0:
                    raise ValueError(
                        f"SCHEDULER_QUOTAS entries must be str->positive int: {name!r}: {value!r}"
                    )
                quotas[name] = value
        config = cls(
            cluster_capacity=capacity,
            default_quota=default_quota,
            quotas=quotas,
            default_timeout_seconds=_env_float("SCHEDULER_DEFAULT_TIMEOUT", 30.0),
            max_timeout_seconds=_env_float("SCHEDULER_MAX_TIMEOUT", 300.0),
            stale_queue_seconds=_env_float("SCHEDULER_STALE_QUEUE_SECONDS", 60.0),
            state_file=os.environ.get(
                "SCHEDULER_STATE_FILE", "data/scheduler-state.json"
            ),
            tick_interval_seconds=_env_float("SCHEDULER_TICK_INTERVAL", 0.1),
        )
        if config.default_timeout_seconds > config.max_timeout_seconds:
            raise ValueError("default timeout must not exceed max timeout")
        return config
