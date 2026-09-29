"""Smoke test: the app wires together and basic endpoints respond."""

from __future__ import annotations

import unittest

from tests.base import ServiceHarness


class BootstrapTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ServiceHarness()

    def tearDown(self) -> None:
        self.h.drain()
        self.h.stop()

    def test_root_and_status(self) -> None:
        self.assertEqual(self.h.client.get("/").json()["status"], "ok")
        snap = self.h.client.get("/status").json()
        self.assertEqual(snap["cluster_capacity"], 10)
        self.assertEqual(snap["used_capacity"], 0)
        self.assertEqual(snap["available"], 10)

    def test_submit_and_get_roundtrip(self) -> None:
        r = self.h.submit(name="n", owner="o", resource=1, duration=0.02)
        self.assertEqual(r.status_code, 201)
        jid = r.json()["job"]["job_id"]
        self.h.wait_status(jid, {"SUCCEEDED"})
        self.assertEqual(self.h.client.get(f"/jobs/{jid}").json()["status"],
                         "SUCCEEDED")
        self.assertEqual(self.h.client.get("/jobs/nope").status_code, 404)


if __name__ == "__main__":
    unittest.main()
