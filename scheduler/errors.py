"""Typed error carrying a distinguishable machine-readable reason."""

from __future__ import annotations

from .models import RejectReason


class SubmitError(Exception):
    """Raised when a submission must be rejected."""

    def __init__(self, reason: RejectReason, message: str):
        self.reason = reason
        self.message = message
        super().__init__(f"{reason.value}: {message}")
