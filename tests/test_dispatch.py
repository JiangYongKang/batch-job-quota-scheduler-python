"""Concurrent dispatch: capacity boundary, priority, strict same-priority FIFO."""

from __future__ import annotations

import time
import unittest

from tests.base import ServiceHarness


class DispatchTest(unittest.TestCase):
    def setUp(self) -> None:
        # capacity 10; per-owner quota 10 so quota never interferes here.
        self.h = ServiceHarness(capacity=10, quota=100)

    def tearDown(self) -> None:
        self.h.drain()
        self.h.stop()

    def test_capacity_never_exceeded_under_concurrent_submits(self) -> None:
        # 20 jobs each wanting 3 units -> at most 3 running at once (3*3<=10).
        ids = []
        for i in range(20):
            r = self.h.submit(name=f"j{i}", owner=f"u{i}", resource=3,
                              duration=0.05)
            self.assertEqual(r.status_code, 201, r.text)
            ids.append(r.json()["job"]["job_id"])
            self.assertLessEqual(
                self.h.client.get("/status").json()["used_capacity"], 10,
            )
        for jid in ids:
            d = self.h.wait_status(jid, {"SUCCEEDED"})
            self.assertEqual(d["status"], "SUCCEEDED")
        snap = self.h.client.get("/status").json()
        self.assertEqual(snap["used_capacity"], 0)
        self.assertEqual(snap["jobs_by_status"].get("SUCCEEDED"), 20)

    def test_same_priority_fifo_no_jumping(self) -> None:
        self.h.drain()
        self.h.stop()
        self.h = ServiceHarness(capacity=6, quota=100)
        blocker = self.h.submit(name="blocker", owner="blk", resource=6,
                                duration=5.0).json()["job"]["job_id"]
        time.sleep(0.05)
        a = self.h.submit(name="a", owner="x", resource=4, priority=1,
                          duration=0.25).json()["job"]["job_id"]
        b = self.h.submit(name="b", owner="x", resource=4, priority=1,
                          duration=0.25).json()["job"]["job_id"]
        c = self.h.submit(name="c", owner="x", resource=3, priority=0,
                          duration=0.25).json()["job"]["job_id"]
        for jid in (a, b, c):
            self.assertEqual(self.h.status_of(jid), "WAITING")
        self.h.client.post(f"/jobs/{blocker}/cancel")

        # a must be dispatched alone; poll until a starts, then assert b,c wait.
        self.h.wait_status(a, {"RUNNING"})
        self.assertEqual(self.h.status_of(b), "WAITING")
        self.assertEqual(self.h.status_of(c), "WAITING")
        da = self.h.wait_status(a, {"SUCCEEDED"})

        # after a: only b (prio1 head) may run; c(3) also fits (6-4=2 <3) no,
        # but once b starts with 4 used, c cannot fit anyway.
        self.h.wait_status(b, {"RUNNING"})
        self.assertEqual(self.h.status_of(c), "WAITING")
        db = self.h.wait_status(b, {"SUCCEEDED"})
        self.assertLessEqual(da["started_at"], db["started_at"])

        dc = self.h.wait_status(c, {"SUCCEEDED"})
        self.assertLess(db["started_at"], dc["started_at"])

    def test_higher_priority_overtakes_blocked_lower_priority_head(self) -> None:
        # hold 2 units for a while: 8 free.
        self.h.submit(name="holder", owner="holder", resource=2, duration=5.0)
        time.sleep(0.05)
        big = self.h.submit(name="big", owner="y", resource=9, priority=0,
                            duration=0.05).json()["job"]["job_id"]
        small = self.h.submit(name="small", owner="y", resource=1, priority=5,
                              duration=0.05).json()["job"]["job_id"]
        # high-priority small fits and finishes while big remains queued.
        self.h.wait_status(small, {"SUCCEEDED"})
        self.assertEqual(self.h.status_of(big), "WAITING")
        # release holder -> big runs
        running = self.h.client.get("/status").json()["running_job_ids"]
        for jid in running:
            self.h.client.post(f"/jobs/{jid}/cancel")
        self.h.wait_status(big, {"SUCCEEDED", "CANCELED"})
        self.assertEqual(self.h.status_of(big), "SUCCEEDED")

    def test_exact_capacity_boundary_fits(self) -> None:
        r = self.h.submit(name="exact", owner="z", resource=10, duration=0.3)
        self.assertEqual(r.status_code, 201, r.text)
        jid = r.json()["job"]["job_id"]
        self.h.wait_status(jid, {"RUNNING"})
        self.assertEqual(self.h.client.get("/status").json()["used_capacity"], 10)
        self.h.wait_status(jid, {"SUCCEEDED"})


if __name__ == "__main__":
    unittest.main()
