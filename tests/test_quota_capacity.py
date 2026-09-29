"""Quota & capacity boundaries: three distinguishable rejection reasons."""

from __future__ import annotations

import time
import unittest

from tests.base import ServiceHarness


class QuotaCapacityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ServiceHarness(capacity=10, quota=6)

    def tearDown(self) -> None:
        self.h.drain()
        self.h.stop()

    # ---- invalid resource ---------------------------------------------------

    def test_invalid_resource_rejected(self) -> None:
        for bad in (0, -1):
            r = self.h.submit(name="x", owner="u", resource=bad)
            self.assertEqual(r.status_code, 400)
            self.assertEqual(r.json()["reason"], "INVALID_RESOURCE")
            self.assertEqual(r.json()["error"], "SUBMIT_REJECTED")

    def test_malformed_resource_types_rejected_as_invalid(self) -> None:
        # type-level malformed requests are normalized to the same 400 code
        for body in (
            {"name": "x", "owner": "u", "resource": "abc"},
            {"name": "x", "owner": "u", "resource": 1.5},
            {"name": "x", "owner": "u", "resource": True},
            {"name": "x", "owner": "u"},  # missing resource
        ):
            r = self.h.client.post("/jobs", json=body)
            self.assertEqual(r.status_code, 400, body)
            self.assertEqual(r.json()["reason"], "INVALID_RESOURCE", body)
            self.assertNotIn("job", r.json())

    def test_other_malformed_fields_distinguishable(self) -> None:
        r = self.h.client.post("/jobs", json={"name": "x", "resource": 2})  # no owner
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["reason"], "INVALID_REQUEST")

    # ---- exceeds cluster capacity ------------------------------------------

    def test_exceeds_capacity_rejected_distinctly(self) -> None:
        r = self.h.submit(name="big", owner="u", resource=11)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["reason"], "EXCEEDS_CAPACITY")
        self.assertNotIn("job", r.json())  # rejected -> nothing created
        self.assertEqual(self.h.client.get("/jobs").json()["count"], 0)

    def test_exact_capacity_accepted(self) -> None:
        r = self.h.submit(name="exact", owner="u", resource=6, duration=0.05)
        self.assertEqual(r.status_code, 201, r.text)
        self.h.wait_status(r.json()["job"]["job_id"], {"SUCCEEDED"})

    # ---- exceeds per-owner quota -------------------------------------------

    def test_quota_boundary(self) -> None:
        # 4 units running -> a further 3 would exceed quota 6 (4+3=7).
        j1 = self.h.submit(name="j1", owner="alice", resource=4,
                           duration=1.0).json()["job"]["job_id"]
        time.sleep(0.05)
        self.assertEqual(self.h.status_of(j1), "RUNNING")

        r = self.h.submit(name="j2", owner="alice", resource=3)
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["reason"], "EXCEEDS_QUOTA")
        self.assertIn("quota 6", r.json()["message"])

        # exactly remaining quota 2 is accepted (even though it cannot start
        # while 4 capacity is consumed by *other* owners? here j1 uses 4,
        # available 6 globally -> it can in fact run; 4+2<=10)
        r2 = self.h.submit(name="j3", owner="alice", resource=2, duration=0.05)
        self.assertEqual(r2.status_code, 201, r2.text)
        self.h.wait_status(r2.json()["job"]["job_id"], {"SUCCEEDED"})

        # quota is released only when jobs reach a terminal state; j1 still
        # holds 4, so 3 units are still over quota.
        r3 = self.h.submit(name="j4", owner="alice", resource=3)
        self.assertEqual(r3.status_code, 400)
        self.assertEqual(r3.json()["reason"], "EXCEEDS_QUOTA")

    def test_quota_is_per_owner(self) -> None:
        self.h.submit(name="a", owner="alice", resource=6, duration=1.0)
        time.sleep(0.05)
        # bob has his own quota despite alice being full
        r = self.h.submit(name="b", owner="bob", resource=6, duration=0.05)
        self.assertEqual(r.status_code, 201, r.text)
        self.h.wait_status(r.json()["job"]["job_id"], {"SUCCEEDED"})

    def test_finished_jobs_release_quota(self) -> None:
        j = self.h.submit(name="short", owner="alice", resource=6,
                          duration=0.05).json()["job"]["job_id"]
        self.h.wait_status(j, {"SUCCEEDED"})
        r = self.h.submit(name="again", owner="alice", resource=6, duration=0.05)
        self.assertEqual(r.status_code, 201, r.text)
        self.h.wait_status(r.json()["job"]["job_id"], {"SUCCEEDED"})

    def test_capacity_vs_quota_reasons_distinguishable(self) -> None:
        # capacity=10 > quota=6: a request of 7 fails quota for an empty owner
        # but a request of 11 fails capacity regardless.
        r_cap = self.h.submit(name="c", owner="new1", resource=11)
        r_quo = self.h.submit(name="q", owner="new2", resource=7)
        self.assertEqual(r_cap.json()["reason"], "EXCEEDS_CAPACITY")
        self.assertEqual(r_quo.json()["reason"], "EXCEEDS_QUOTA")


if __name__ == "__main__":
    unittest.main()
