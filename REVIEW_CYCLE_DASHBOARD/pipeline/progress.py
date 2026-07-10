"""Lightweight progress reporting for long-running pipeline steps."""

from __future__ import annotations

from typing import Callable, Optional

ProgressCallback = Callable[[int, int, str], None]


class PipelineProgress:
    """Report step N of M with a human-readable label."""

    def __init__(self, callback: Optional[ProgressCallback] = None, total: int = 1) -> None:
        self.callback = callback
        self.total = max(int(total), 1)
        self.step = 0

    def set_total(self, total: int) -> None:
        self.total = max(int(total), 1)

    def update(self, step: int, label: str) -> None:
        self.step = max(0, int(step))
        if self.callback:
            try:
                self.callback(self.step, self.total, str(label or ""))
            except Exception:
                pass

    def advance(self, label: str) -> None:
        self.update(self.step + 1, label)
