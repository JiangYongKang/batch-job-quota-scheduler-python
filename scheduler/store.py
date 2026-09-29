"""Persistence: atomic JSON snapshot of all jobs (restart recovery).

Only one scheduler process is expected locally. Writes go through a temp
file + os.replace so a crash can never leave a half-written snapshot.
"""

from __future__ import annotations

import json
import os
import tempfile

from .models import Attempt, Job, JobStatus


class JobStore:
    def __init__(self, path: str):
        self.path = path

    def load(self) -> dict[str, Job]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return {jid: self._job_from_dict(d) for jid, d in raw.items()}

    def save(self, jobs: dict[str, Job]) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        payload = {jid: self._job_to_dict(j) for jid, j in jobs.items()}
        fd, tmp = tempfile.mkstemp(prefix=".sched-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=1)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    # ---- serialization -----------------------------------------------------

    @staticmethod
    def _attempt_to_dict(a: Attempt) -> dict:
        return {
            "attempt_no": a.attempt_no,
            "started_at": a.started_at,
            "ended_at": a.ended_at,
            "outcome": a.outcome,
            "detail": a.detail,
        }

    @staticmethod
    def _attempt_from_dict(d: dict) -> Attempt:
        return Attempt(
            attempt_no=d["attempt_no"],
            started_at=d["started_at"],
            ended_at=d.get("ended_at"),
            outcome=d.get("outcome"),
            detail=d.get("detail", ""),
        )

    def _job_to_dict(self, j: Job) -> dict:
        return {
            "job_id": j.job_id,
            "name": j.name,
            "owner": j.owner,
            "resource": j.resource,
            "priority": j.priority,
            "seq": j.seq,
            "created_at": j.created_at,
            "duration": j.duration,
            "fail_times": j.fail_times,
            "timeout": j.timeout,
            "queue_timeout": j.queue_timeout,
            "max_attempts": j.max_attempts,
            "status": j.status.value,
            "attempts": [self._attempt_to_dict(a) for a in j.attempts],
            "attempt_count": j.attempt_count,
            "fail_reason": j.fail_reason.value if j.fail_reason else None,
            "fail_detail": j.fail_detail,
            "cancel_requested": j.cancel_requested,
            "started_at": j.started_at,
            "finished_at": j.finished_at,
            "next_attempt_at": j.next_attempt_at,
            "enqueued_again_at": j.enqueued_again_at,
        }

    def _job_from_dict(self, d: dict) -> Job:
        j = Job(
            job_id=d["job_id"],
            name=d["name"],
            owner=d["owner"],
            resource=d["resource"],
            priority=d["priority"],
            seq=d["seq"],
            created_at=d["created_at"],
            duration=d.get("duration", 0.1),
            fail_times=d.get("fail_times", 0),
            timeout=d.get("timeout"),
            queue_timeout=d.get("queue_timeout"),
            max_attempts=d.get("max_attempts", 3),
            status=JobStatus(d["status"]),
            attempts=[self._attempt_from_dict(a) for a in d.get("attempts", [])],
            attempt_count=d.get("attempt_count", 0),
            fail_detail=d.get("fail_detail", ""),
            cancel_requested=d.get("cancel_requested", False),
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
            next_attempt_at=d.get("next_attempt_at"),
            enqueued_again_at=d.get("enqueued_again_at"),
        )
        fr = d.get("fail_reason")
        if fr:
            from .models import FailureReason
            j.fail_reason = FailureReason(fr)
        return j
