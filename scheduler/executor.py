"""Task execution: local deterministic simulation (success / failure / timeout).

No external services: an "attempt" is simply a timed wait whose outcome is
derived from the job's simulation knobs (``fail_times`` / ``timeout``) and the
cancellation event. This function is pure w.r.t. the job: it does not mutate
job state or touch accounting -- the scheduler owns that.

Outcome precedence within one attempt:
    CANCELED  >  TIMEOUT  >  FAILED  >  SUCCESS
"""

from __future__ import annotations

import asyncio


class ExecutionOutcome:
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    CANCELED = "CANCELED"


async def run_attempt(
    job,
    cancel_event: asyncio.Event,
    *,
    attempt_no: int,
    sleep=asyncio.sleep,
) -> tuple[str, str]:
    """Run one attempt; returns ``(outcome, detail)`` without mutating the job."""
    deadline = job.timeout
    wait_time = job.duration
    timed_out = False
    if deadline is not None and job.duration > deadline:
        wait_time = deadline
        timed_out = True

    cancel_task = asyncio.ensure_future(cancel_event.wait())
    work_task = asyncio.ensure_future(sleep(wait_time))
    try:
        done, _ = await asyncio.wait(
            {cancel_task, work_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if cancel_task in done:
            return (
                ExecutionOutcome.CANCELED,
                "cancel requested while running",
            )
        if timed_out:
            return (
                ExecutionOutcome.TIMEOUT,
                f"attempt {attempt_no} exceeded timeout {deadline}s "
                f"(duration={job.duration}s)",
            )
        if attempt_no <= job.fail_times:
            return (
                ExecutionOutcome.FAILED,
                f"simulated failure on attempt {attempt_no} "
                f"(fail_times={job.fail_times})",
            )
        return ExecutionOutcome.SUCCESS, f"completed in {job.duration}s"
    finally:
        for t in (cancel_task, work_task):
            if not t.done():
                t.cancel()
