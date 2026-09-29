"""Deterministic simulated workloads; nothing external is ever contacted."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scheduler.schemas import WorkloadRequest


@dataclass(frozen=True)
class WorkloadSpec:
    type: str  # "sleep" | "succeed" | "fail"
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "duration_seconds": self.duration_seconds}

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "WorkloadSpec":
        wtype = str(payload.get("type", "sleep"))
        duration = float(payload.get("duration_seconds", 0.0) or 0.0)
        if wtype not in ("sleep", "succeed", "fail"):
            raise ValueError(f"unknown workload type: {wtype!r}")
        if duration < 0.0:
            raise ValueError("duration_seconds must be >= 0")
        return cls(type=wtype, duration_seconds=duration)

    @classmethod
    def from_request(cls, request: "WorkloadRequest") -> "WorkloadSpec":
        return cls(type=request.type, duration_seconds=request.duration_seconds)


class WorkloadError(Exception):
    """Raised when a simulated workload fails on its own."""


async def run_workload(spec: WorkloadSpec) -> str:
    """Run the simulated workload.

    Returns a small result string; raises WorkloadError on simulated failure;
    asyncio.CancelledError propagates when the task is canceled or times out.
    """
    if spec.duration_seconds:
        await asyncio.sleep(spec.duration_seconds)
    if spec.type == "fail":
        raise WorkloadError("simulated workload failure")
    return f"{spec.type}:{spec.duration_seconds:g}s"
