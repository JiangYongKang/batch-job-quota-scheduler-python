"""Restart recovery: unfinished jobs & resources neither vanish nor double-count."""

from __future__ import annotations

import asyncio
import time
import unittest

from scheduler.scheduler import Scheduler
from scheduler.store import JobStore
from tests.base import ServiceHarness


class RecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ServiceHarness(capacity=10, quota=100, backoff=0.05)

    def tearDown(self) -> None:
        self.h.drain()
        self.h.stop()

    def test_running_job_requeued_on_restart_and_succeeds(self) -> None:
        jid = self.h.submit(name="long", owner="rc", resource=4,
                            duration=0.3).json()["job"]["job_id"]
        time.sleep(0.1)  # ensure it is RUNNING
        self.assertEqual(self.h.status_of(jid), "RUNNING")
        before = self.h.client.get(f"/jobs/{jid}").json()

        # restart before the job finishes
        self.h.restart()
        d = self.h.wait_status(jid, {"SUCCEEDED"}, timeout=3)
        self.assertEqual(d["status"], "SUCCEEDED")
        # audit trail: first attempt INTERRUPTED (shutdown/restart), then SUCCESS
        self.assertEqual([a["outcome"] for a in d["attempts"]],
                         ["INTERRUPTED", "SUCCESS"])
        # attempt_count counts attempts that STARTED: interrupted(1)+success(1)=2
        self.assertEqual(d["attempt_count"], 2)
        # ...but the interrupted attempt did not consume the retry budget:
        # after the restart the job still succeeded on its first post-restart try
        self.assertEqual(d["max_attempts"], before["max_attempts"])
        self.assertEqual(
            [a["outcome"] for a in d["attempts"]].count("SUCCESS"), 1,
        )
        snap = self.h.client.get("/status").json()
        self.assertEqual(snap["used_capacity"], 0)

    def test_terminal_jobs_survive_restart_unchanged(self) -> None:
        ok = self.h.submit(name="ok", owner="u", resource=1,
                           duration=0.02).json()["job"]["job_id"]
        bad = self.h.submit(name="bad", owner="u", resource=1, duration=0.02,
                            fail_times=9, max_attempts=1).json()["job"]["job_id"]
        self.h.wait_status(ok, {"SUCCEEDED"})
        self.h.wait_status(bad, {"FAILED"})
        self.h.restart()
        self.assertEqual(self.h.status_of(ok), "SUCCEEDED")
        bd = self.h.client.get(f"/jobs/{bad}").json()
        self.assertEqual(bd["status"], "FAILED")
        self.assertEqual(bd["fail_reason"], "ATTEMPT_FAILED")

    def test_waiting_job_still_queued_after_restart(self) -> None:
        fillers = self.h.fill_capacity(duration=30.0)
        wid = self.h.submit(name="w", owner="w", resource=1,
                            duration=0.05).json()["job"]["job_id"]
        self.assertEqual(self.h.status_of(wid), "WAITING")
        self.h.restart()
        # fillers are re-queued too; with all jobs WAITING the original w keeps
        # its submission order and eventually succeeds after fillers cancel
        self.assertEqual(self.h.status_of(wid), "WAITING")
        # cancel all fillers -> w should dispatch and succeed
        time.sleep(0.05)
        snap = self.h.client.get("/status").json()
        for jid in snap["running_job_ids"]:
            if jid != wid:
                self.h.client.post(f"/jobs/{jid}/cancel")
        # wait again, cancel repeatedly any filler re-runs
        for _ in range(100):
            if self.h.status_of(wid) == "SUCCEEDED":
                break
            for jid in self.h.client.get("/status").json()["running_job_ids"]:
                if jid != wid:
                    self.h.client.post(f"/jobs/{jid}/cancel")
            time.sleep(0.02)
        self.assertEqual(self.h.status_of(wid), "SUCCEEDED")

    def test_no_phantom_capacity_immediately_after_restart(self) -> None:
        # Load via a raw scheduler start (dispatch loop not ticked yet):
        # capacity must be derived only from RUNNING jobs -> exactly zero.
        self.h.fill_capacity(duration=30.0)
        running = self.h.submit(name="r", owner="r", resource=3,
                                duration=30.0).json()["job"]["job_id"]
        time.sleep(0.05)
        self.h.stop()  # persist while RUNNING; no HTTP shutdown dispatch

        fresh = Scheduler(self.h.config, JobStore(self.h.config.store_path))

        async def probe():
            await fresh.start()
            try:
                self.assertEqual(fresh.snapshot()["used_capacity"], 0)
                # every non-terminal job recovered to WAITING
                nonterm = [j for j in fresh.jobs.values() if not j.terminal]
                self.assertTrue(nonterm)
                for j in nonterm:
                    self.assertEqual(j.status.value, "WAITING")
            finally:
                await fresh.stop()

        asyncio.run(probe())

    def test_interrupted_attempt_does_not_consume_retry_budget(self) -> None:
        # fail_times=5 / max_attempts=1: a completed attempt would immediately
        # exhaust the budget -> FAILED. But if we restart while the very first
        # attempt is still running, INTERRUPTED is not charged: after restart
        # the job must get a fresh attempt (and, once failure is no longer
        # forced, succeed). We model "failure no longer forced after restart"
        # by editing the recovered job's fail_times via the API-less store.
        jid = self.h.submit(
            name="budget", owner="rc", resource=2, duration=30.0,
            fail_times=5, max_attempts=1,
        ).json()["job"]["job_id"]
        self.h.wait_status(jid, {"RUNNING"})
        # stop, then edit the on-disk payload so the recovered attempt is a
        # quick success (proving it is re-dispatched at all after restart).
        self.h.stop()
        import json
        with open(self.h.config.store_path, encoding="utf-8") as fh:
            raw = json.load(fh)
        raw[jid]["fail_times"] = 0
        raw[jid]["duration"] = 0.05
        with open(self.h.config.store_path, "w", encoding="utf-8") as fh:
            json.dump(raw, fh)
        self.h.restart()
        d = self.h.client.get(f"/jobs/{jid}").json()
        # already re-dispatched or still queued depending on tick timing, but
        # the restarted attempt is recorded and no FAILED has been charged
        self.assertIn(d["status"], ("WAITING", "RUNNING"))
        self.assertEqual(d["attempts"][0]["outcome"], "INTERRUPTED")
        # nothing was charged as FAILED/TIMEOUT against max_attempts=1
        charged = [a["outcome"] for a in d["attempts"]
                   if a["outcome"] in ("FAILED", "TIMEOUT")]
        self.assertEqual(charged, [])
        d = self.h.wait_status(jid, {"SUCCEEDED", "FAILED"}, timeout=3)
        self.assertEqual(d["status"], "SUCCEEDED", d)
        self.assertEqual(
            [a["outcome"] for a in d["attempts"]], ["INTERRUPTED", "SUCCESS"],
        )

    def test_restart_then_cancel_requeued_job(self) -> None:
        jid = self.h.submit(name="long", owner="rc", resource=2,
                            duration=30.0).json()["job"]["job_id"]
        time.sleep(0.05)
        self.h.restart()
        # immediately cancel before it (re)starts, or while running
        result = self.h.client.post(f"/jobs/{jid}/cancel").json()["result"]
        self.assertIn(result, ("CANCELED", "CANCELING"))
        d = self.h.wait_status(jid, {"CANCELED"})
        self.assertEqual(d["status"], "CANCELED")
        self.assertEqual(self.h.client.get("/status").json()["used_capacity"], 0)


if __name__ == "__main__":
    unittest.main()
