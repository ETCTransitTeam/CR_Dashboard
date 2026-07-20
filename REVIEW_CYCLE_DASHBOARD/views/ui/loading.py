"""Consistent loading and completion feedback for Review Cycle views."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Iterator

import streamlit as st

ProgressCallback = Callable[[int, int, str], None]
_FLASH_KEY = "rcd_operation_flash"


class StatusProgress:
    """Progress reporter usable both as a callback and as PipelineProgress."""

    def __init__(self, status, progress, label: str) -> None:
        self.status = status
        self.progress = progress
        self.label = label
        self.total = 1
        self.step = 0

    def __call__(self, step: int, total: int, message: str) -> None:
        self.total = max(int(total), 1)
        self.step = min(max(int(step), 0), self.total)
        text = str(message or self.label)
        self.progress.progress(self.step / self.total, text=text)
        self.status.update(label=text, state="running", expanded=True)

    def set_total(self, total: int) -> None:
        self.total = max(int(total), 1)

    def update(self, step: int, label: str) -> None:
        self(step, self.total, label)

    def advance(self, label: str) -> None:
        self.update(self.step + 1, label)


def set_operation_flash(message: str, level: str = "success") -> None:
    """Persist operation feedback across a Streamlit rerun."""
    st.session_state[_FLASH_KEY] = {
        "message": str(message),
        "level": level if level in {"success", "info", "warning", "error"} else "info",
    }


def render_operation_flash() -> None:
    """Render and clear feedback saved before a rerun."""
    flash = st.session_state.pop(_FLASH_KEY, None)
    if not flash:
        return
    renderer = getattr(st, flash.get("level", "info"), st.info)
    renderer(str(flash.get("message", "")))


@contextmanager
def loading(message: str) -> Iterator[None]:
    """Show an elapsed-time spinner for one indeterminate operation."""
    with st.spinner(message, show_time=True):
        yield


@contextmanager
def progress_status(
    label: str,
    *,
    complete_label: str | None = None,
    expanded: bool = True,
) -> Iterator[StatusProgress]:
    """Expose a pipeline-compatible callback in a staged status panel."""
    status = st.status(label, expanded=expanded)
    progress = status.progress(0, text=label)

    reporter = StatusProgress(status, progress, label)

    try:
        yield reporter
    except Exception:
        status.update(label=f"{label} failed", state="error", expanded=True)
        raise
    else:
        progress.progress(1.0, text=complete_label or "Complete")
        status.update(
            label=complete_label or f"{label} complete",
            state="complete",
            expanded=False,
        )
