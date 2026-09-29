"""HTTP-level integration tests (stdlib unittest + httpx ASGITransport).

Covers the required scenario families:
  1. admission: invalid / capacity / quota / duplicate boundaries
  2. stable dispatch: priority then strict FIFO (no overtaking)
  3. concurrency bounded by cluster capacity and per-caller quota
  4. cancel chain for queued / running / already-terminal jobs
  5. failures, bounded retries and timeouts (no resource leak / double hold)
  6. queue ageing ("stale" jobs stay visible)
  7. restart recovery incl. simulated hard crash (no phantom resources)

Each async test runs in a fresh event loop, which also runs the engine's
scheduler loop and job tasks created inside that test's app instance.
"""
from __future__ import annotations

import asyncio
import functools
import os
import tempfile
import unittest

import httpx

from scheduler.app import create_app
from scheduler.config import SchedulerConfig


def asynctest(func):
    """Run an async test in ONE event loop together with setup/teardown.

    The engine's scheduler loop and job tasks live on the loop that ran the
    lifespan startup, so setup, the test body and teardown must share it.
    """

    @functools.wraps(func)
    def wrapper(self):
        async def runner():
            await self.asyncSetUp()
            try:
                await func(self)
            finally:
                await self.asyncTearDown()

        asyncio.run(runner())

    return wrapper


class ApiTestCase(unittest.TestCase):
    capacity = 8
    default_quota = 8
    quotas: dict[str, int] = {}
    stale_after = 0.5

    async def asyncSetUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="sched-test-")
        self.state_file = os.path.join(self.dir, "state.json")
        self.cfg = SchedulerConfig(
            cluster_capacity=self.capacity,
            default_quota=self.default_quota,
            quotas=dict(self.quotas),
            default_timeout_seconds=5.0,
            max_timeout_seconds=60.0,
            stale_queue_seconds=self.stale_after,
            state_file=self.state_file,
            tick_interval_seconds=0.02,
        )
        self.app = create_app(self.cfg)
        self.transport = httpx.ASGITransport(app=self.app)
        self.client = httpx.AsyncClient(
            transport=self.transport, base_url="http://test"
        )
        self.lifespan = self.app.router.lifespan_context(self.app)
        await self.lifespan.__aenter__()

    async def asyncTearDown(self) -> None:
        if getattr(self, "lifespan", None) is not None and not getattr(
            self, "_crashed", False
        ):
            await self.lifespan.__aexit__(None, None, None)
        await self.client.aclose()

    # -- helpers (all async; the test loop owns the engine) --
    async def submit(self, job_id, resource, caller="alice", *, workload=None,
                     priority=10, max_retries=0, timeout=None):
        body = {
            "job_id": job_id,
            "resource": resource,
            "priority": priority,
            "max_retries": max_retries,
            "workload": workload or {"type": "succeed"},
        }
        if timeout is not None:
            body["timeout_seconds"] = timeout
        return await self.client.post(
            "/jobs", json=body, headers={"X-Caller": caller}
        )

    async def get(self, job_id):
        return await self.client.get(f"/jobs/{job_id}")

    async def list_jobs(self, **params):
        return await self.client.get("/jobs", params=params)

    async def cancel(self, job_id, caller=None):
        headers = {"X-Caller": caller} if caller else {}
        return await self.client.post(f"/jobs/{job_id}/cancel", headers=headers)

    async def stats(self):
        return await self.client.get("/stats")

    async def wait_for(self, job_id, *statuses, timeout=5.0):
        deadline = asyncio.get_running_loop().time() + timeout
        last = None
        while asyncio.get_running_loop().time() < deadline:
            r = await self.get(job_id)
            last = r.json()
            if last["status"] in statuses:
                return last
            await asyncio.sleep(0.02)
        raise AssertionError(f"{job_id} never reached {statuses}; last={last}")

    async def wait_used(self, expected, timeout=5.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            data = (await self.stats()).json()
            if data["used_resources"] == expected:
                return data
            await asyncio.sleep(0.02)
        raise AssertionError(f"used_resources never reached {expected}")

    async def restart(self):
        """Graceful stop followed by a fresh start against the same file."""
        await self.lifespan.__aexit__(None, None, None)
        await self.client.aclose()
        self.app = create_app(self.cfg)
        self.transport = httpx.ASGITransport(app=self.app)
        self.client = httpx.AsyncClient(
            transport=self.transport, base_url="http://test"
        )
        self.lifespan = self.app.router.lifespan_context(self.app)
        await self.lifespan.__aenter__()

    async def hard_crash_and_restart(self):
        """Simulate SIGKILL: no graceful finalization, then a new process.

        The old engine tasks still exist on this event loop; neutralize them
        by making persistence a no-op and canceling them, so the snapshot on
        disk (showing RUNNING jobs) is the only input the new engine sees.
        """
        old_engine = self.app.state.engine
        old_engine.store.save = lambda snapshot: None
        for task in list(old_engine._tasks.values()):
            task.cancel()
        await asyncio.sleep(0.05)
        self._crashed = True
        await self.client.aclose()
        self._crashed = False
        self.app = create_app(self.cfg)
        self.transport = httpx.ASGITransport(app=self.app)
        self.client = httpx.AsyncClient(
            transport=self.transport, base_url="http://test"
        )
        self.lifespan = self.app.router.lifespan_context(self.app)
        await self.lifespan.__aenter__()


# --------------------------------------------------------------------------- #
# 1. Admission boundaries
# --------------------------------------------------------------------------- #
class AdmissionTests(ApiTestCase):
    capacity = 8
    default_quota = 4
    quotas = {"bob": 2}

    @asynctest
    async def test_invalid_requests_are_rejected_as_422(self):
        r = await self.submit("zero", 0)
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["detail"]["reason"], "invalid_request")
        self.assertIn("resource", r.json()["detail"]["detail"])
        self.assertEqual((await self.submit("neg", -1)).status_code, 422)
        self.assertEqual(
            (await self.submit("bad-prio", 1, priority=1000)).status_code, 422
        )
        r = await self.client.post("/jobs", json={
            "job_id": "bad-wl", "resource": 1, "workload": {"type": "nope"},
        })
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["detail"]["reason"], "invalid_request")

    @asynctest
    async def test_over_cluster_capacity_has_distinct_reason(self):
        r = await self.submit("huge", 9)
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["detail"]["reason"], "capacity_exceeded")
        self.assertIn("capacity", r.json()["detail"]["detail"])

    @asynctest
    async def test_over_caller_quota_has_distinct_reason(self):
        # capacity 8 but bob's quota is 2 -> quota, not capacity
        r = await self.submit("big", 3, caller="bob")
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["detail"]["reason"], "quota_exceeded")
        self.assertIn("quota", r.json()["detail"]["detail"])

    @asynctest
    async def test_quota_reason_when_request_fits_cluster_but_not_quota(self):
        r = await self.submit("q", 5, caller="alice")  # 4 < 5 <= 8
        self.assertEqual(r.json()["detail"]["reason"], "quota_exceeded")

    @asynctest
    async def test_duplicate_submit_is_rejected_with_current_status(self):
        self.assertEqual(
            (await self.submit("dup", 1,
             workload={"type": "succeed"})).status_code, 201
        )
        await self.wait_for("dup", "succeeded")
        r = await self.submit("dup", 1)
        self.assertEqual(r.status_code, 409)
        body = r.json()["detail"]
        self.assertEqual(body["reason"], "duplicate_job")
        self.assertIn("succeeded", body["detail"])

    @asynctest
    async def test_unknown_job_endpoints_are_404(self):
        self.assertEqual((await self.get("nope")).status_code, 404)
        self.assertEqual((await self.cancel("nope")).status_code, 404)

    @asynctest
    async def test_head_blocking_queues_rather_than_rejects(self):
        # Once a caller's quota is busy, another fitting request from that
        # caller queues; it is not rejected (queue != quota rejection).
        await self.submit("r1", 4,
                          workload={"type": "sleep", "duration_seconds": 0.4})
        await self.wait_used(4)
        r = await self.submit("r2", 4)
        self.assertEqual(r.status_code, 201)
        self.assertTrue(r.json()["queued"])


# --------------------------------------------------------------------------- #
# 2. Stable ordering: priority then FIFO
# --------------------------------------------------------------------------- #
class SchedulingOrderTests(ApiTestCase):
    capacity = 4
    default_quota = 8

    @asynctest
    async def test_higher_priority_dispatches_before_lower(self):
        # occ holds 2 of 4; when it finishes only 2 slots free up at once in
        # the head position, while both waiters need 3 -> exactly one can go,
        # and it must be the higher-priority job.
        await self.submit("occ", 2,
                          workload={"type": "sleep", "duration_seconds": 0.35})
        await self.wait_used(2)
        await self.submit("low", 3, priority=1,
                          workload={"type": "sleep", "duration_seconds": 0.25})
        await self.submit("high", 3, priority=20,
                          workload={"type": "sleep", "duration_seconds": 0.25})
        s = (await self.stats()).json()
        self.assertEqual(s["queued_jobs"], ["high", "low"])
        await self.wait_for("occ", "succeeded")
        await asyncio.sleep(0.08)
        self.assertEqual((await self.get("high")).json()["status"], "running")
        self.assertEqual((await self.get("low")).json()["status"], "queued")
        await self.wait_for("high", "succeeded")
        await self.wait_for("low", "succeeded")
        high = (await self.get("high")).json()
        low = (await self.get("low")).json()
        self.assertGreater(high["enqueue_seq"], low["enqueue_seq"])
        self.assertLess(high["started_at"], low["started_at"])

    @asynctest
    async def test_same_priority_is_strict_fifo_even_for_smaller_jobs(self):
        await self.submit("occ", 3,
                          workload={"type": "sleep", "duration_seconds": 0.4})
        await self.wait_used(3)
        await self.submit("first", 2, priority=5,
                          workload={"type": "sleep", "duration_seconds": 0.25})
        # 'second' asks for 1 slot (1 is free) but must not overtake first,
        # which needs 2; after occ finishes first still goes strictly first.
        await self.submit("second", 1, priority=5,
                          workload={"type": "sleep", "duration_seconds": 0.25})
        s = (await self.stats()).json()
        self.assertEqual(s["queued_jobs"], ["first", "second"])
        self.assertEqual(s["running_jobs"], ["occ"])
        await asyncio.sleep(0.02)
        self.assertEqual((await self.get("first")).json()["status"], "queued")
        self.assertEqual((await self.get("second")).json()["status"], "queued")
        await self.wait_for("first", "succeeded")
        first = (await self.get("first")).json()
        # While first runs (holds 2/4), second's 1 slot now fits and runs,
        # but it was never admitted before first — the guarantee under test.
        await self.wait_for("second", "succeeded")
        second = (await self.get("second")).json()
        self.assertLess(first["enqueue_seq"], second["enqueue_seq"])
        self.assertLessEqual(first["started_at"], second["started_at"])

# --------------------------------------------------------------------------- #
# 3. Concurrency bounded by capacity and quota
# --------------------------------------------------------------------------- #
class CapacityConcurrencyTests(ApiTestCase):
    capacity = 6
    default_quota = 8

    @asynctest
    async def test_never_exceed_capacity_and_all_resources_returned(self):
        for i in range(4):  # 4 jobs * 2 slots on capacity 6 -> 3 at a time
            await self.submit(
                f"j{i}", 2,
                workload={"type": "sleep", "duration_seconds": 0.25},
            )
        await asyncio.sleep(0.12)
        s = (await self.stats()).json()
        self.assertEqual(s["used_resources"], 6)
        self.assertEqual(sorted(s["running_jobs"]), ["j0", "j1", "j2"])
        self.assertEqual(s["queued_jobs"], ["j3"])
        for i in range(4):
            await self.wait_for(f"j{i}", "succeeded")
        s = (await self.stats()).json()
        self.assertEqual(s["used_resources"], 0)
        self.assertEqual(s["available_resources"], 6)
        self.assertEqual(s["succeeded"], 4)

    @asynctest
    async def test_head_blocked_on_capacity_blocks_whole_pass(self):
        await self.submit("occ", 3,
                          workload={"type": "sleep", "duration_seconds": 0.4})
        await self.wait_used(3)
        await self.submit("head", 4, caller="alice",
                          workload={"type": "succeed"})
        await self.submit("small", 1, caller="carol",
                          workload={"type": "succeed"})
        await asyncio.sleep(0.1)
        s = (await self.stats()).json()
        # strict FIFO: 'small' fits but may not jump the blocked head
        self.assertEqual(s["running_jobs"], ["occ"])
        self.assertEqual(s["queued_jobs"], ["head", "small"])
        await self.wait_for("head", "succeeded")
        await self.wait_for("small", "succeeded")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)


class QuotaConcurrencyTests(ApiTestCase):
    capacity = 6
    default_quota = 2
    quotas = {"bob": 6}

    @asynctest
    async def test_quota_caps_caller_but_cluster_shares_remainder(self):
        await self.submit("a1", 2, caller="alice",
                          workload={"type": "sleep", "duration_seconds": 0.4})
        await self.submit("a2", 2, caller="alice",
                          workload={"type": "sleep", "duration_seconds": 0.4})
        await self.submit("a3", 2, caller="alice",
                          workload={"type": "sleep", "duration_seconds": 0.4})
        # bob has quota 6 and cluster has 4 slots free at dispatch time;
        # FIFO order is a1, a2, a3, b1: a3 sits at the head once alice is
        # full and blocks b1 (documented strict-FIFO rule). Verify alice
        # never exceeds her quota regardless of dispatch outcome.
        await self.submit("b1", 2, caller="bob",
                          workload={"type": "sleep", "duration_seconds": 0.4})
        await asyncio.sleep(0.15)
        s = (await self.stats()).json()
        self.assertLessEqual(s["per_caller_running"].get("alice", 0), 2)
        self.assertLessEqual(s["used_resources"], 6)
        for jid in ("a1", "a2", "a3", "b1"):
            await self.wait_for(jid, "succeeded", timeout=6)
        self.assertEqual((await self.stats()).json()["used_resources"], 0)

    @asynctest
    async def test_quota_releases_for_next_jobs(self):
        # alice quota is 2 and each job asks for 2: at most one runs at a
        # time. Queued jobs start promptly as the running one finishes — no
        # leaked resource, no stuck queue.
        await self.submit("a1", 2, caller="alice",
                          workload={"type": "sleep", "duration_seconds": 0.2})
        await self.submit("a2", 2, caller="alice",
                          workload={"type": "sleep", "duration_seconds": 0.2})
        await self.submit("a3", 2, caller="alice",
                          workload={"type": "succeed"})
        await self.wait_used(2)
        self.assertEqual((await self.stats()).json()["queued"], 2)
        for jid in ("a1", "a2", "a3"):
            await self.wait_for(jid, "succeeded", timeout=6)
        self.assertEqual((await self.stats()).json()["used_resources"], 0)


# --------------------------------------------------------------------------- #
# 4. Cancellation
# --------------------------------------------------------------------------- #
class CancelTests(ApiTestCase):
    capacity = 4
    default_quota = 8

    @asynctest
    async def test_cancel_queued_job_is_immediate(self):
        await self.submit("occ", 3,
                          workload={"type": "sleep", "duration_seconds": 0.4})
        await self.wait_used(3)
        await self.submit("q", 2, workload={"type": "succeed"})
        r = await self.cancel("q")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["canceled"])
        self.assertEqual(r.json()["previous_status"], "queued")
        j = (await self.get("q")).json()
        self.assertEqual(j["status"], "canceled")
        self.assertEqual(j["fail_reason"], "canceled")
        self.assertEqual((await self.stats()).json()["queued"], 0)

    @asynctest
    async def test_cancel_running_job_releases_resource_once(self):
        await self.submit("run", 3,
                          workload={"type": "sleep", "duration_seconds": 2.0})
        await self.wait_used(3)
        r = await self.cancel("run")
        self.assertEqual(r.status_code, 200)
        j = await self.wait_for("run", "canceled")
        self.assertEqual(j["fail_reason"], "canceled")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)
        again = await self.cancel("run")
        self.assertEqual(again.status_code, 409)
        self.assertEqual(
            again.json()["detail"]["reason"], "already_terminal"
        )

    @asynctest
    async def test_cancel_terminal_job_is_distinct_conflict(self):
        await self.submit("done", 1, workload={"type": "succeed"})
        await self.wait_for("done", "succeeded")
        r = await self.cancel("done")
        self.assertEqual(r.status_code, 409)
        body = r.json()["detail"]
        self.assertEqual(body["reason"], "already_terminal")
        self.assertIn("succeeded", body["detail"])

    @asynctest
    async def test_cancel_one_running_job_does_not_touch_other(self):
        await self.submit("keep", 2,
                          workload={"type": "sleep", "duration_seconds": 0.5})
        await self.submit("kill", 2,
                          workload={"type": "sleep", "duration_seconds": 2.0})
        await self.wait_used(4)
        await self.cancel("kill")
        await self.wait_for("kill", "canceled")
        s = (await self.stats()).json()
        self.assertEqual(s["used_resources"], 2)
        self.assertEqual(s["running_jobs"], ["keep"])
        await self.wait_for("keep", "succeeded")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)


# --------------------------------------------------------------------------- #
# 5. Failure, retries, timeout
# --------------------------------------------------------------------------- #
class RetryFailureTests(ApiTestCase):
    capacity = 4
    default_quota = 8

    @asynctest
    async def test_failure_without_retry_is_terminal_with_reason(self):
        await self.submit("f", 1, workload={"type": "fail"})
        j = await self.wait_for("f", "failed")
        self.assertEqual(j["fail_reason"], "workload_failed")
        self.assertEqual(j["attempts"], 1)
        self.assertEqual((await self.stats()).json()["used_resources"], 0)

    @asynctest
    async def test_retries_are_bounded_and_release_between_attempts(self):
        await self.submit("f2", 3, max_retries=2, workload={"type": "fail"})
        j = await self.wait_for("f2", "failed", timeout=6)
        self.assertEqual(j["attempts"], 3)
        self.assertEqual(j["fail_reason"], "workload_failed")
        self.assertIn("retries exhausted", j["fail_detail"])
        self.assertEqual((await self.stats()).json()["used_resources"], 0)

    @asynctest
    async def test_timeout_is_a_distinct_failure_reason(self):
        await self.submit(
            "slow", 4, timeout=0.15,
            workload={"type": "sleep", "duration_seconds": 2.0},
        )
        j = await self.wait_for("slow", "failed", timeout=4)
        self.assertEqual(j["fail_reason"], "timeout")
        self.assertIn("timeout", j["fail_detail"])
        self.assertEqual((await self.stats()).json()["used_resources"], 0)

    @asynctest
    async def test_timeout_retries_budget_is_consumed_once_per_attempt(self):
        await self.submit(
            "t", 1, max_retries=1, timeout=0.1,
            workload={"type": "sleep", "duration_seconds": 1.0},
        )
        j = await self.wait_for("t", "failed", timeout=6)
        self.assertEqual(j["attempts"], 2)
        self.assertEqual(j["fail_reason"], "timeout")

    @asynctest
    async def test_failed_job_frees_capacity_for_next_submission(self):
        await self.submit("bad", 2, workload={"type": "fail"})
        await self.wait_for("bad", "failed")
        await self.submit("good", 2, workload={"type": "succeed"})
        await self.wait_for("good", "succeeded")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)


# --------------------------------------------------------------------------- #
# 6. Queue ageing
# --------------------------------------------------------------------------- #
class StaleQueueTests(ApiTestCase):
    capacity = 2
    default_quota = 8
    stale_after = 0.25

    @asynctest
    async def test_long_waiting_job_is_flagged_stale_with_wait_seconds(self):
        await self.submit("occ", 2,
                          workload={"type": "sleep", "duration_seconds": 1.0})
        await self.wait_used(2)
        await self.submit("waiter", 1, workload={"type": "succeed"})
        await asyncio.sleep(0.4)
        j = (await self.get("waiter")).json()
        self.assertEqual(j["status"], "queued")
        self.assertTrue(j["stale"])
        self.assertIsNotNone(j["wait_seconds"])
        self.assertGreaterEqual(j["wait_seconds"], 0.3)

    @asynctest
    async def test_fresh_queued_job_is_not_stale(self):
        await self.submit("occ", 2,
                          workload={"type": "sleep", "duration_seconds": 0.5})
        await self.wait_used(2)
        await self.submit("w", 1, workload={"type": "succeed"})
        j = (await self.get("w")).json()
        self.assertFalse(j["stale"])


# --------------------------------------------------------------------------- #
# 7. Restart recovery
# --------------------------------------------------------------------------- #
class RestartRecoveryTests(ApiTestCase):
    capacity = 6
    default_quota = 8

    @asynctest
    async def test_terminal_and_queued_jobs_survive_graceful_restart(self):
        await self.submit("done", 1, workload={"type": "succeed"})
        await self.wait_for("done", "succeeded")
        await self.submit("occ", 6,
                          workload={"type": "sleep", "duration_seconds": 0.3})
        await self.wait_used(6)
        await self.submit("waiting", 2, workload={"type": "succeed"})
        await asyncio.sleep(0.05)
        await self.restart()
        self.assertEqual(
            (await self.get("done")).json()["status"], "succeeded"
        )
        self.assertEqual(
            (await self.get("waiting")).json()["status"], "queued"
        )
        await self.wait_for("waiting", "succeeded")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)

    @asynctest
    async def test_graceful_stop_requeues_running_job_without_phantom_usage(self):
        await self.submit("work", 4,
                          workload={"type": "sleep", "duration_seconds": 0.05})
        await self.wait_used(4)
        await self.restart()
        s = (await self.stats()).json()
        self.assertEqual(s["used_resources"], 0)
        j = await self.wait_for("work", "succeeded")
        self.assertGreaterEqual(j["attempts"], 1)
        self.assertIsNone(j["fail_reason"])

    @asynctest
    async def test_hard_crash_recomputes_resources_and_resumes_jobs(self):
        await self.submit("a", 2,
                          workload={"type": "sleep", "duration_seconds": 0.05})
        await self.submit("b", 3,
                          workload={"type": "sleep", "duration_seconds": 0.05})
        await self.wait_used(5)
        await self.hard_crash_and_restart()
        s = (await self.stats()).json()
        self.assertEqual(s["used_resources"], 0)
        queued = sorted(j["job_id"] for j in (await self.list_jobs(
            status="queued")).json())
        self.assertEqual(queued, ["a", "b"])
        await self.wait_for("a", "succeeded")
        await self.wait_for("b", "succeeded")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)

    @asynctest
    async def test_cancel_requested_is_honored_across_graceful_restart(self):
        await self.submit("k", 2,
                          workload={"type": "sleep", "duration_seconds": 3.0})
        await self.wait_used(2)
        await self.cancel("k")
        await asyncio.sleep(0.05)
        self.assertEqual((await self.get("k")).json()["status"], "canceled")
        await self.restart()
        self.assertEqual((await self.get("k")).json()["status"], "canceled")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)

    @asynctest
    async def test_pending_cancel_after_hard_crash_is_honored(self):
        # Persist the cancel request on a running job, then crash BEFORE the
        # cancellation finalizes. Recovery must end it canceled, not rerun it.
        await self.submit("k2", 2,
                          workload={"type": "sleep", "duration_seconds": 5.0})
        await self.wait_used(2)
        await self.cancel("k2")
        await self.hard_crash_and_restart()
        j = (await self.get("k2")).json()
        self.assertEqual(j["status"], "canceled")
        self.assertEqual(j["fail_reason"], "canceled")
        self.assertEqual((await self.stats()).json()["used_resources"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
