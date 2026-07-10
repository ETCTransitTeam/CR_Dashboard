from __future__ import annotations

import pandas as pd
import streamlit as st

from core.data_access import load_assignments, load_combined_checks, load_records, records_to_elvis_review
from services import assignments as assignment_svc
from services import history as history_svc
from views.combined_checks_fields import render_combined_checks_table
from views.filters import (
    apply_record_filters,
    filter_review_team_history,
    filter_supervisor_tosia_records,
    record_id_column,
    review_dashboard_record_ids,
)
from views.record_card import render_record_card
from views.ui import empty_state, info_strip, page_header, section_title, stats_bar

HISTORY_DISPLAY_COLS = [
    "CREATED_AT",
    "RECORD_ID",
    "ACTOR",
    "ACTOR_ROLE",
    "FIELD_NAME",
    "OLD_VALUE",
    "NEW_VALUE",
    "ACTION",
]


def _append_sum_all_checks(display: pd.DataFrame, project: str, record_ids: set[str]) -> pd.DataFrame:
    if display.empty or not record_ids:
        return display
    checks = load_combined_checks(project)
    if checks.empty:
        return display
    subset = checks[checks["RECORD_ID"].astype(str).isin(record_ids)][["RECORD_ID", "SUM_ALL_CHECKS"]]
    if subset.empty:
        return display
    id_col = record_id_column(display)
    if not id_col:
        return display
    sums = subset.set_index(subset["RECORD_ID"].astype(str))["SUM_ALL_CHECKS"]
    out = display.copy()
    out["SUM_ALL_CHECKS"] = out[id_col].astype(str).map(lambda rid: sums.get(rid, ""))
    return out


def _supervisor_display(records: pd.DataFrame, project: str) -> pd.DataFrame:
    if records.empty:
        return records
    display = records_to_elvis_review(records)
    ids = set(records["RECORD_ID"].astype(str))
    return _append_sum_all_checks(display, project, ids)


def _render_supervisor_history(project: str) -> None:
    section_title("Review-team history")
    info_strip("Review-team changes for records on Combined Checks and Supervisor View Only.")
    dashboard_ids = review_dashboard_record_ids(project)
    if not dashboard_ids:
        st.info("No review-dashboard records in this project.")
        return
    history = history_svc.load_history(project)
    if history.empty:
        st.info("No history recorded yet for this project.")
        return
    view = filter_review_team_history(history, dashboard_ids)
    if view.empty:
        st.info("No review-team history for review-dashboard records yet.")
        return
    view = view.sort_values("CREATED_AT", ascending=False)
    cols = [c for c in HISTORY_DISPLAY_COLS if c in view.columns]
    info_strip(f"{len(view)} history entries")
    st.dataframe(view[cols], use_container_width=True, hide_index=True)
    st.download_button(
        "Download history (CSV)",
        data=view[cols].to_csv(index=False).encode("utf-8"),
        file_name=f"{project}_supervisor_review_history.csv",
        mime="text/csv",
        key="sup_history_csv",
    )


def _open_record(
    project: str,
    display: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
    role: str,
    key: str,
    *,
    assignments: pd.DataFrame | None = None,
) -> None:
    id_col = record_id_column(display)
    if not id_col or display.empty:
        return
    section_title("Open record details")
    selected_id = st.selectbox("Open record", options=display[id_col].astype(str).tolist(), key=key)
    if not selected_id:
        return
    actor = user.get("name") or user.get("EMAIL")

    if assignments is not None and not assignments.empty:
        match = assignments[assignments["RECORD_ID"].astype(str) == str(selected_id)]
        if not match.empty and st.button("Defer record (push to bottom)", key=f"{key}_defer_{selected_id}"):
            assignment_svc.defer_assignment(int(match.iloc[0]["ASSIGNMENT_ID"]))
            st.success("Record deferred.")
            st.rerun()

    with st.expander("2X review decision"):
        f1, f2 = st.columns(2)
        flag = f1.selectbox("2X flag", ["", "Pass", "Fail", "Needs work"], key=f"{key}_2x_flag_{selected_id}")
        if f2.button("Record 2X review", key=f"{key}_2x_btn_{selected_id}"):
            history_svc.set_two_x_review(project, selected_id, actor, flag, actor, role)
            st.success("2X review recorded.")
            st.rerun()

    render_record_card(
        project,
        selected_id,
        user,
        allow_admin=role in ("admin", "manager"),
        widget_key_prefix=key,
        history_actor_roles=["review"],
    )


def render_supervisor_page(user: dict) -> None:
    page_header(
        "Supervisor View Only",
        "Supervisor-review queue: blank Final Usage with FINAL_REVIEWER = Tosia. "
        "All matching records appear in Combined Checks; assignments are listed separately.",
    )

    role = user.get("ROLE") or user.get("role")
    actor = user.get("name") or user.get("EMAIL")

    from core.session_project import require_active_project

    project = require_active_project()
    if not project:
        return

    records = load_records(project)
    supervisor_records = filter_supervisor_tosia_records(records)
    stats_bar([("Supervisor-view records", str(len(supervisor_records))), ("Active project", project)])

    tab_all, tab_assign, tab_history = st.tabs(["Combined Checks", "My assignments", "Record History"])

    with tab_all:
        if supervisor_records.empty:
            st.info("No supervisor-view records for this project (blank Final Usage + FINAL_REVIEWER = Tosia).")
        else:
            display = _supervisor_display(supervisor_records, project)
            display = apply_record_filters(display, key_prefix="sup_all", include_usage=False)
            info_strip(f"{len(display)} supervisor-view record(s)")
            display = render_combined_checks_table(
                display,
                supervisor_records,
                user,
                editor_key="sup_all_editor",
                project_name=project,
                history_actor_roles=["review"],
            )
            _open_record(project, display, supervisor_records, user, role, key="sup_all_open")

    with tab_assign:
        section_title("My review assignments (supervisor view)")
        assignments = load_assignments(assigned_to=actor, team="review", project_name=project)
        if assignments.empty:
            st.info("Nothing assigned to you in the supervisor queue.")
        else:
            assign_ids = set(assignments["RECORD_ID"].astype(str))
            subset = supervisor_records[supervisor_records["RECORD_ID"].astype(str).isin(assign_ids)]
            if subset.empty:
                st.info("Your review assignments do not include any supervisor-view records.")
            else:
                display = _supervisor_display(subset, project)
                display = apply_record_filters(display, key_prefix="sup_assign", include_usage=False)
                info_strip(f"{len(display)} assigned supervisor-view record(s)")
                assign_subset = assignments[assignments["RECORD_ID"].astype(str).isin(set(subset["RECORD_ID"].astype(str)))]
                display = render_combined_checks_table(
                    display,
                    subset,
                    user,
                    editor_key="sup_assign_editor",
                    project_name=project,
                    history_actor_roles=["review"],
                )
                _open_record(
                    project,
                    display,
                    subset,
                    user,
                    role,
                    key="sup_assign_open",
                    assignments=assign_subset,
                )

    with tab_history:
        _render_supervisor_history(project)
