"""Atomic file-backed JSON snapshots for crash/restart recovery.

Every accepted state transition is persisted as a full snapshot:
write to a temp file in the same directory, fsync, then atomically rename.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any

logger = logging.getLogger("scheduler.persistence")


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def save(self, snapshot: dict[str, Any]) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".scheduler-state-", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, ensure_ascii=False, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.path)
            logger.debug(
                "persisted snapshot jobs=%d running=%d",
                len(snapshot.get("jobs", {})),
                snapshot.get("used_resources"),
            )
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    def load(self) -> dict[str, Any] | None:
        if not os.path.exists(self.path):
            logger.info("no state file at %s, starting empty", self.path)
            return None
        with open(self.path, "r", encoding="utf-8") as handle:
            snapshot = json.load(handle)
        logger.info(
            "loaded snapshot jobs=%d used_resources(persisted)=%s",
            len(snapshot.get("jobs", {})),
            snapshot.get("used_resources"),
        )
        return snapshot
