"""Cleaning assignment overview + unassign for super admins / special emails."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from core.streamlit_cache import bump_data_cache
from rc_auth.access import can_manage_cleaning_assignments, is_super_admin_user
from services import assignments as assign_svc
from views.ui import action_row, filter_panel, info_strip, loading, page_header, section_title

DISPLAY_COLS = [
    "RECORD_ID",
    "ASSIGNED_TO",
    "ASSIGNED_AT",
    "PRIORITY",
    "TEAM",
    "PROJECT_NAME",
]


def _norm_record_id(value) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    return text


def _forget_overlay_ids(record_ids: list[str]) -> None:
    overlay = st.session_state.get("cleaning_head_assignee_overlay")
    if not isinstance(overlay, dict):
        return
    for rid in record_ids:
        overlay.pop(_norm_record_id(rid), None)


def render_assignment_manager_page(user: dict) -> None:
    if is_super_admin_user(user):
        page_header(
            "Cleaning Assignments",
            "All active cleaning assignments — including those held by admins or super admins.",
        )
    else:
        page_header(
            "Cleaning Assignments",
            "Active cleaning-team assignments only. You can unassign cleaners’ records here.",
        )

    if not can_manage_cleaning_assignments(user):
        st.error("You do not have permission to manage cleaning assignments.")
        return

    from core.session_project import require_active_project

    project = require_active_project()
    if not project:
        return

    with loading("Loading active cleaning assignments…"):
        assignments = assign_svc.load_assignments(team="cleaning", project_name=project)

    if assignments.empty:
        st.info("No active cleaning assignments for this project.")
        return

    view = assignments.copy()
    view["RECORD_ID"] = view["RECORD_ID"].map(_norm_record_id)
    if "ASSIGNED_TO" in view.columns:
        view["ASSIGNED_TO"] = view["ASSIGNED_TO"].fillna("").astype(str).str.strip()

    # Special email (Mansi): only cleaning-staff queues. Super admin: all assignees.
    if not is_super_admin_user(user):
        allowed = {
            n.strip().lower()
            for n in assign_svc.cleaning_assignee_options(include_privileged=False)
        }
        view = view[view["ASSIGNED_TO"].astype(str).str.strip().str.lower().isin(allowed)]
        if view.empty:
            st.info("No active assignments to cleaning staff for this project.")
            return

    with filter_panel("Filters", "Narrow by assignee or record ID."):
        c1, c2 = st.columns(2)
        assignees = sorted(
            a for a in view["ASSIGNED_TO"].dropna().astype(str).unique() if a
        )
        assignee_filter = c1.multiselect("Assigned to", assignees)
        record_query = c2.text_input("Record ID contains", placeholder="Optional")

    if assignee_filter:
        view = view[view["ASSIGNED_TO"].isin(assignee_filter)]
    if record_query.strip():
        q = record_query.strip().lower()
        view = view[view["RECORD_ID"].astype(str).str.lower().str.contains(q, na=False)]

    if view.empty:
        st.info("No assignments match the current filters.")
        return

    cols = [c for c in DISPLAY_COLS if c in view.columns]
    section_title("Active assignments")
    by_person = (
        view.groupby("ASSIGNED_TO", dropna=False)["RECORD_ID"]
        .count()
        .sort_values(ascending=False)
        if "ASSIGNED_TO" in view.columns
        else pd.Series(dtype=int)
    )
    summary = " · ".join(f"{name or '(blank)'}: {n}" for name, n in by_person.items())
    info_strip(f"{len(view)} active · {summary}" if summary else f"{len(view)} active")

    st.dataframe(view[cols], use_container_width=True, hide_index=True)

    section_title("Unassign")
    record_ids = sorted(view["RECORD_ID"].astype(str).unique().tolist())
    selected = st.multiselect(
        "Records to unassign",
        options=record_ids,
        help="Select one or more assigned record IDs to release.",
    )

    actor = str(user.get("EMAIL") or user.get("email") or user.get("NAME") or "").strip()
    with action_row():
        unassign_selected = st.button(
            "Unassign selected",
            type="primary",
            disabled=not selected,
            use_container_width=True,
        )
        unassign_visible = st.button(
            f"Unassign all {len(record_ids)} visible",
            type="secondary",
            use_container_width=True,
        )

    to_release: list[str] = []
    if unassign_selected and selected:
        to_release = list(selected)
    elif unassign_visible:
        to_release = list(record_ids)

    if to_release:
        with loading(f"Unassigning {len(to_release)} record(s)…"):
            n = assign_svc.unassign_records(
                project, to_release, team="cleaning", actor=actor or None
            )
            _forget_overlay_ids(to_release)
            bump_data_cache()
        st.success(f"Released {n} assignment(s).")
        st.rerun()
