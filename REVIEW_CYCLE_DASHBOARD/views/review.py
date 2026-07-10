from __future__ import annotations

import pandas as pd
import streamlit as st

from core.data_access import load_assignments, load_combined_checks, load_records, records_to_elvis_review
from services import assignments as assignment_svc
from services import history as history_svc
from services import notifications as notify_svc
from views.combined_checks_fields import render_combined_checks_table
from views.filters import apply_record_filters, record_id_column
from views.record_card import render_record_card
from views.ui import empty_state, info_strip, page_header, section_title, stats_bar


def _default_review_assignee(members: list[str]) -> str:
    for name in members:
        if name.strip().lower() == "tosia":
            return name
    return members[0] if members else ""


def _flagged_records(project: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    checks = load_combined_checks(project)
    records = load_records(project)
    if checks.empty:
        return records.iloc[0:0], checks
    flagged = checks[pd.to_numeric(checks["SUM_ALL_CHECKS"], errors="coerce").fillna(0) > 0]
    if flagged.empty:
        return records.iloc[0:0], flagged
    ids = flagged["RECORD_ID"].astype(str).tolist()
    subset = records[records["RECORD_ID"].astype(str).isin(ids)]
    return subset, flagged


def render_review_page(user: dict) -> None:
    from core.session_project import require_active_project

    page_header("Combined Checks", "Tosia's team: pull flagged records by priority, review, and decide.")

    role = user.get("ROLE") or user.get("role")
    actor = user.get("name") or user.get("EMAIL")
    is_admin_view = role in ("admin", "manager")

    project = require_active_project()
    if not project:
        return

    queue = assignment_svc.build_priority_queue(project, exclude_active=True, team="review")
    stats_bar([("Unassigned flagged records", str(len(queue))), ("Active project", project)])

    with st.container(border=True):
        review_members = assignment_svc.team_members("review")
        default_assignee = _default_review_assignee(review_members)
        assignee = st.selectbox(
            "Assign pulled records to",
            options=review_members or ["(no review users)"],
            index=review_members.index(default_assignee) if default_assignee in review_members else 0,
            key="review_assignee",
        )

        st.markdown("**Pull records** (priority: flag severity + record age; already-reviewed-but-failing sink to the bottom)")
        c1, c2, c3, c4 = st.columns(4)
        for col, count in ((c1, 25), (c2, 50), (c3, 100)):
            if col.button(f"Assign next {count}", key=f"pull_{count}"):
                if assignee and assignee != "(no review users)":
                    ids = assignment_svc.pull_next(project, assignee, count, team="review")
                    notify_svc.notify(assignee, notify_svc.NEW_ASSIGNMENT, f"Pulled {len(ids)} review records for {project}", project)
                    st.success(f"Assigned {len(ids)} record(s) to {assignee}.")
                    st.rerun()
        custom = c4.number_input("Custom N", min_value=1, max_value=500, value=10)
        if c4.button("Assign next N"):
            if assignee and assignee != "(no review users)":
                ids = assignment_svc.pull_next(project, assignee, int(custom), team="review")
                st.success(f"Assigned {len(ids)} record(s) to {assignee}.")
                st.rerun()

        with st.expander("Manager: reallocate for clean (Jason)"):
            rc1, rc2 = st.columns(2)
            manager_count = rc1.number_input("Move N flagged to cleaning", min_value=1, max_value=200, value=25)
            manager_user = rc2.text_input("Assign to", value="Jason")
            if st.button("Reallocate to cleaning"):
                clean_ids = assignment_svc.build_priority_queue(project, limit=int(manager_count), team="review")
                id_list = clean_ids["RECORD_ID"].astype(str).tolist() if not clean_ids.empty else []
                assignment_svc.assign_records(project, id_list, manager_user, team="cleaning", priority=50)
                notify_svc.notify(manager_user, notify_svc.NEW_ASSIGNMENT, f"{len(id_list)} records for manager clean ({project})", project)
                st.success(f"Assigned {len(id_list)} to {manager_user} for cleaning.")
                st.rerun()

    tab_labels = ["My assignments"]
    if is_admin_view:
        tab_labels.insert(0, "All flagged records")
    tabs = st.tabs(tab_labels)

    tab_idx = 0
    if is_admin_view:
        with tabs[tab_idx]:
            subset, _ = _flagged_records(project)
            if subset.empty:
                st.info("No flagged records for this project.")
            else:
                display = records_to_elvis_review(subset)
                display = apply_record_filters(display, key_prefix="rev_all")
                info_strip(f"{len(display)} flagged record(s)")
                display = render_combined_checks_table(
                    display, subset, user, editor_key="rev_all_editor", project_name=project
                )
                _open_record(project, display, subset, user, role, key="rev_all_open")
        tab_idx += 1

    with tabs[tab_idx]:
        section_title("My review queue")
        assignments = load_assignments(assigned_to=actor, team="review", project_name=project)
        records = load_records(project)
        if assignments.empty:
            st.info("Nothing assigned to you. Use 'Assign next' to pull from the queue.")
            if not is_admin_view:
                subset, _ = _flagged_records(project)
                if subset.empty:
                    return
                browse = records_to_elvis_review(subset)
                browse = apply_record_filters(browse, key_prefix="rev_browse")
                browse = render_combined_checks_table(
                    browse, subset, user, editor_key="rev_browse_editor", project_name=project
                )
                _open_record(project, browse, subset, user, role, key="rev_browse_open")
            return

        ids = assignments["RECORD_ID"].astype(str).tolist()
        subset = records[records["RECORD_ID"].astype(str).isin(ids)]
        display = records_to_elvis_review(subset)
        display = apply_record_filters(display, key_prefix="rev")
        info_strip(f"{len(display)} record(s) assigned to you")
        display = render_combined_checks_table(
            display, subset, user, editor_key="rev_queue_editor", project_name=project
        )
        _open_record(project, display, records, user, role, key="rev_open", assignments=assignments)


def _open_record(project, display, records, user, role, key, assignments=None) -> None:
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
    )
