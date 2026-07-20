from __future__ import annotations

import streamlit as st

from core.data_access import load_reviewer_stats
from pipeline.ingest import format_ingest_counts, sync_project
from views.ui import (
    action_row,
    info_strip,
    loading,
    page_header,
    progress_status,
    section_title,
    set_operation_flash,
)


def _friendly_error(exc: Exception) -> str:
    """Short user-facing message — keep the real exception detail when useful."""
    text = str(exc).strip()
    if text.startswith("Reviewer stats failed:"):
        detail = text.split(":", 1)[1].strip()
    else:
        detail = text

    if not detail:
        return "Reviewer stats failed. Check project data and try again."

    # Hide long tracebacks / file paths; keep KeyError/Exception lines.
    if "Traceback" in detail or ".py" in detail.splitlines()[0]:
        for line in reversed(detail.splitlines()):
            line = line.strip()
            if "Error" in line or "Exception" in line:
                return f"Reviewer stats failed: {line[:280]}"
        return "Reviewer stats failed. Check project data and try again."

    return f"Reviewer stats failed: {detail[:280]}"


def render_reviewer_stats_page(user: dict) -> None:
    from core.session_project import require_active_project

    page_header(
        "Reviewer Stats",
        "Compute reviewer performance metrics for the active project. "
        "Full review flags are generated from **Generate Review Flags** on Sync & Admin.",
    )

    project = require_active_project()
    if not project:
        return

    with action_row():
        if st.button("Run reviewer stats", type="primary"):
            try:
                with progress_status(
                    "Running reviewer statistics...",
                    complete_label="Reviewer statistics complete",
                ) as update:
                    result = sync_project(project, phase="stats", progress=update)
                set_operation_flash(format_ingest_counts(result.get("counts", {})))
                st.rerun()
            except Exception as exc:
                st.error(_friendly_error(exc))

    with loading("Loading reviewer performance metrics…"):
        stats = load_reviewer_stats(project)
    if stats.empty:
        st.info(
            "No reviewer stats yet. Click **Run reviewer stats**, or generate review flags "
            "from Sync & Admin."
        )
        return

    stat_types = sorted(stats["STAT_TYPE"].dropna().astype(str).unique())
    tabs = st.tabs(stat_types if stat_types else ["Stats"])
    for i, stat_type in enumerate(stat_types or ["Stats"]):
        with tabs[i]:
            section_title(str(stat_type))
            view = stats[stats["STAT_TYPE"].astype(str) == stat_type] if stat_types else stats
            pivot = view.pivot_table(
                index="STAT_KEY",
                columns="METRIC_NAME",
                values="METRIC_VALUE",
                aggfunc="first",
            ).reset_index()
            st.dataframe(pivot, use_container_width=True, hide_index=True)
            info_strip(f"{len(view)} metric row(s)")
