"""Shared helpers for scheduler tests."""

from __future__ import annotations

import os
import tempfile
import time

from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.config import Config
from scheduler.scheduler import Scheduler
from scheduler.store import JobStore


class ServiceHarness:
    """Starts a real HTTP service backed by a temp JSON store."""

    def __init__(self, *, capacity: int = 10, quota: int = 6,
                 backoff: float = 0.05, tick: float = 0.01,
                 default_attempts: int = 3,
                 attempt_timeout: float | None = 30.0,
                 queue_timeout: float | None = None):
        self.store_path = tempfile.mktemp(prefix="sched-test-", suffix=".json")
        self.config = Config(
            cluster_capacity=capacity,
            quota_per_owner=quota,
            default_attempts=default_attempts,
            default_attempt_timeout=attempt_timeout,
            default_queue_timeout=queue_timeout,
            retry_backoff=backoff,
            tick_interval=tick,
            store_path=self.store_path,
        )
        self.scheduler = Scheduler(self.config, JobStore(self.store_path))
        self.app = create_app(self.scheduler)
        self.client = TestClient(self.app)
        self.client.__enter__()

    def stop(self) -> None:
        self.client.__exit__(None, None, None)

    def restart(self) -> "ServiceHarness":
        """Simulate a process restart against the same on-disk state."""
        self.stop()
        self.scheduler = Scheduler(self.config, JobStore(self.store_path))
        self.app = create_app(self.scheduler)
        self.client = TestClient(self.app)
        self.client.__enter__()
        return self

    # ---- convenience --------------------------------------------------------

    def submit(self, **kwargs):
        return self.client.post("/jobs", json=kwargs)

    def status_of(self, job_id: str) -> str:
        return self.client.get(f"/jobs/{job_id}").json()["status"]

    def wait_status(self, job_id: str, statuses, timeout: float = 5.0,
                    tick: float = 0.01) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            data = self.client.get(f"/jobs/{job_id}").json()
            if data["status"] in statuses:
                return data
            time.sleep(tick)
        raise AssertionError(
            f"job {job_id} did not reach {statuses}; "
            f"last={self.client.get(f'/jobs/{job_id}').json()}"
        )

    def drain(self, timeout: float = 5.0) -> None:
        """Cancel all running jobs and wait for capacity to be fully released.

        No-op if the service is already stopped (tests that stop the client
        themselves leave persisted state behind intentionally).
        """
        try:
            snap = self.client.get("/status").json()
        except Exception:
            return
        if snap["used_capacity"] == 0 and not snap["running_job_ids"]:
            return
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                snap = self.client.get("/status").json()
            except Exception:
                return
            if snap["used_capacity"] == 0 and not snap["running_job_ids"]:
                return
            for jid in snap["running_job_ids"]:
                self.client.post(f"/jobs/{jid}/cancel")
            time.sleep(0.02)

    def fill_capacity(self, owner_prefix: str = "filler",
                      duration: float = 30.0) -> list[str]:
        """Submit unit-sized jobs until the whole cluster capacity is held."""
        snap = self.client.get("/status").json()
        need = snap["available"]
        ids = []
        for i in range(need):
            r = self.submit(name=f"{owner_prefix}-{i}", owner=f"{owner_prefix}-{i}",
                            resource=1, duration=duration)
            assert r.status_code == 201, r.text
            ids.append(r.json()["job"]["job_id"])
        # wait until capacity really full
        deadline = time.time() + 2
        while time.time() < deadline:
            if self.client.get("/status").json()["available"] == 0:
                return ids
            time.sleep(0.01)
        raise AssertionError("failed to fill capacity")
