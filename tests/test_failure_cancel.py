"""Failure/retry and cancel chains: no double-accounting, distinguishable results."""

from __future__ import annotations

import time
import unittest

from tests.base import ServiceHarness


class FailureCancelTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ServiceHarness(capacity=10, quota=100, backoff=0.05)

    def tearDown(self) -> None:
        self.h.drain()
        self.h.stop()

    # ---- retries ------------------------------------------------------------

    def test_retry_then_success_releases_resource_between_attempts(self) -> None:
        jid = self.h.submit(
            name="flaky", owner="u", resource=4, duration=0.05,
            fail_times=2, max_attempts=3,
        ).json()["job"]["job_id"]
        d = self.h.wait_status(jid, {"SUCCEEDED"})
        self.assertEqual(d["attempt_count"], 3)
        outcomes = [a["outcome"] for a in d["attempts"]]
        self.assertEqual(outcomes, ["FAILED", "FAILED", "SUCCESS"])
        # no resource remains
        self.assertEqual(self.h.client.get("/status").json()["used_capacity"], 0)

    def test_resource_released_during_backoff(self) -> None:
        # While the job is in BACKOFF, capacity must be fully available.
        # A dedicated long-backoff harness widens the observable window.
        self.h.drain(); self.h.stop()
        self.h = ServiceHarness(capacity=10, quota=100, backoff=1.0)
        seen_backoff = False
        jid = self.h.submit(
            name="r", owner="u", resource=10, duration=0.05,
            fail_times=5, max_attempts=6,
        ).json()["job"]["job_id"]
        deadline = time.time() + 2
        while time.time() < deadline:
            d = self.h.client.get(f"/jobs/{jid}").json()
            if d["status"] == "BACKOFF":
                seen_backoff = True
                # one consistent snapshot: BACKOFF job holds no resource
                snap = self.h.client.get("/status").json()
                self.assertEqual(snap["used_capacity"], 0)
                self.assertEqual(snap["available"], 10)
                break
            if d["status"] in ("FAILED", "SUCCEEDED"):
                break
            time.sleep(0.005)
        self.assertTrue(seen_backoff, "job never entered BACKOFF")
        self.h.client.post(f"/jobs/{jid}/cancel")

    def test_failures_exhausted_attempt_failed(self) -> None:
        jid = self.h.submit(
            name="always-fail", owner="u", resource=1, duration=0.02,
            fail_times=99, max_attempts=2,
        ).json()["job"]["job_id"]
        d = self.h.wait_status(jid, {"FAILED"})
        self.assertEqual(d["fail_reason"], "ATTEMPT_FAILED")
        self.assertEqual(d["attempt_count"], 2)
        self.assertTrue(d["fail_detail"])

    def test_timeout_failure_distinguishable(self) -> None:
        jid = self.h.submit(
            name="slow", owner="u", resource=1, duration=5.0,
            timeout=0.05, max_attempts=1,
        ).json()["job"]["job_id"]
        d = self.h.wait_status(jid, {"FAILED"})
        self.assertEqual(d["fail_reason"], "ATTEMPT_TIMEOUT")
        self.assertEqual(d["attempts"][-1]["outcome"], "TIMEOUT")
        self.assertEqual(self.h.client.get("/status").json()["used_capacity"], 0)

    # ---- cancel -------------------------------------------------------------

    def test_cancel_waiting_is_immediate_and_idempotently_terminal(self) -> None:
        # fill capacity so a new job must wait
        self.h.fill_capacity(duration=30.0)
        wj = self.h.submit(name="w", owner="w", resource=1).json()["job"]["job_id"]
        self.assertEqual(self.h.status_of(wj), "WAITING")
        r1 = self.h.client.post(f"/jobs/{wj}/cancel")
        self.assertEqual(r1.json()["result"], "CANCELED")
        self.assertEqual(self.h.status_of(wj), "CANCELED")
        r2 = self.h.client.post(f"/jobs/{wj}/cancel")
        self.assertEqual(r2.json()["result"], "ALREADY_TERMINAL")
        # canceling any finished job is the same distinguishable result
        self.h.drain()

    def test_cancel_running_releases_resource(self) -> None:
        jid = self.h.submit(name="long", owner="u", resource=6,
                            duration=30.0).json()["job"]["job_id"]
        time.sleep(0.05)
        self.assertEqual(self.h.status_of(jid), "RUNNING")
        r = self.h.client.post(f"/jobs/{jid}/cancel")
        self.assertEqual(r.json()["result"], "CANCELING")
        d = self.h.wait_status(jid, {"CANCELED"})
        self.assertTrue(d["cancel_requested"])
        self.assertEqual(d["attempts"][-1]["outcome"], "CANCELED")
        self.assertEqual(self.h.client.get("/status").json()["used_capacity"], 0)

    def test_cancel_unknown_job_404(self) -> None:
        r = self.h.client.post("/jobs/does-not-exist/cancel")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.json()["error"], "NOT_FOUND")

    def test_cancel_during_backoff_terminates_without_resource(self) -> None:
        jid = self.h.submit(
            name="b", owner="u", resource=10, duration=0.02,
            fail_times=9, max_attempts=9,
        ).json()["job"]["job_id"]
        # wait until it enters backoff, then cancel
        deadline = time.time() + 2
        while time.time() < deadline:
            if self.h.status_of(jid) == "BACKOFF":
                break
            time.sleep(0.005)
        r = self.h.client.post(f"/jobs/{jid}/cancel")
        self.assertEqual(r.json()["result"], "CANCELED")
        d = self.h.wait_status(jid, {"CANCELED"})
        self.assertEqual(d["status"], "CANCELED")
        self.assertEqual(self.h.client.get("/status").json()["used_capacity"], 0)

    # ---- duplicates ---------------------------------------------------------

    def test_duplicate_idempotency_key_returns_existing_job(self) -> None:
        body = {"name": "d", "owner": "u", "resource": 1, "duration": 0.05,
                "job_id": "idem-123"}
        r1 = self.h.submit(**body)
        self.assertFalse(r1.json()["duplicate"])
        r2 = self.h.submit(**body)
        self.assertEqual(r2.status_code, 201)
        self.assertTrue(r2.json()["duplicate"])
        self.assertEqual(r2.json()["job"]["job_id"], "idem-123")

    def test_queue_timeout_expires_with_distinguishable_reason(self) -> None:
        self.h.fill_capacity(duration=5.0)
        jid = self.h.submit(
            name="starved", owner="late", resource=1, queue_timeout=0.1,
        ).json()["job"]["job_id"]
        d = self.h.wait_status(jid, {"EXPIRED"})
        self.assertEqual(d["fail_reason"], "QUEUE_TIMEOUT")
        self.assertIn("queue_timeout", d["fail_detail"])


if __name__ == "__main__":
    unittest.main()
