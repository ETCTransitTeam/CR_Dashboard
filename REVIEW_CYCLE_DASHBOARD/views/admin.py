from __future__ import annotations

import pandas as pd
import streamlit as st

from core.data_access import load_combined_checks, load_records
from services import history as history_svc
from views.record_card import render_record_card
from views.record_fields import EDITABLE_FIELD_NAMES
from views.ui import info_strip, page_header, section_title, stats_bar

_ADMIN_SECTIONS = (
    "Approval queue",
    "Escalated only",
    "High change-rate",
)


def _change_rate_table(project: str, records: pd.DataFrame) -> pd.DataFrame:
    """Percent of a record's fields that have been edited, from the audit trail."""
    history = history_svc.load_history(project)
    if history.empty or records.empty:
        return pd.DataFrame(columns=["RECORD_ID", "CHANGED_FIELDS", "TOTAL_FIELDS", "CHANGE_PCT"])
    changed = history.groupby("RECORD_ID")["FIELD_NAME"].apply(
        lambda names: sum(1 for name in names if name in EDITABLE_FIELD_NAMES)
    ).reset_index(name="CHANGED_FIELDS")
    total_fields = len(EDITABLE_FIELD_NAMES)
    changed["TOTAL_FIELDS"] = total_fields
    changed["CHANGE_PCT"] = (changed["CHANGED_FIELDS"] / total_fields * 100).round(1)
    return changed.sort_values("CHANGE_PCT", ascending=False)


def _run_admin_maintenance(project: str) -> int:
    """Sync escalated flags and repair false admin-approvals once per project per session."""
    cache_key = f"admin_maint_v1_{project}"
    if st.session_state.get(cache_key):
        return 0
    with st.spinner("Preparing approval queue..."):
        history_svc.sync_escalated_flags_from_history(project)
        repaired = history_svc.repair_admin_approved_flags(project)
    st.session_state[cache_key] = True
    return repaired


def _prepare_checks_for_queue(project: str) -> pd.DataFrame:
    """Ensure escalated records from history appear in the approval queue."""
    repaired = _run_admin_maintenance(project)
    if repaired:
        info_strip(f"Repaired {repaired} record(s) with incorrect admin-approved flags.")
    checks = load_combined_checks(project)
    if checks.empty:
        history_ids = history_svc.load_escalated_record_ids(project)
        if not history_ids:
            return checks
        return pd.DataFrame(
            {
                "PROJECT_NAME": project,
                "RECORD_ID": list(history_ids),
                "SUM_ALL_CHECKS": 0,
                "ADMIN_APPROVED": False,
                "ESCALATED": True,
            }
        )

    history_ids = history_svc.load_escalated_record_ids(project)
    if not history_ids:
        return checks

    out = checks.copy()
    if "ESCALATED" not in out.columns:
        out["ESCALATED"] = False
    out["RECORD_ID"] = out["RECORD_ID"].astype(str)
    existing = set(out["RECORD_ID"].tolist())
    for rid in history_ids:
        if rid in existing:
            out.loc[out["RECORD_ID"] == rid, "ESCALATED"] = True
        else:
            out = pd.concat(
                [
                    out,
                    pd.DataFrame(
                        [
                            {
                                "PROJECT_NAME": project,
                                "RECORD_ID": rid,
                                "SUM_ALL_CHECKS": 0,
                                "ADMIN_APPROVED": False,
                                "ESCALATED": True,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
    return out


def render_admin_page(user: dict) -> None:
    page_header("Admin Review & Approval", "Review flagged and escalated records awaiting admin approval.")
    actor = user.get("name") or user.get("EMAIL")
    role = user.get("ROLE") or user.get("role")

    from core.session_project import require_active_project

    project = require_active_project()
    if not project:
        return

    section_title("Review controls")
    with st.container(border=True):
        st.caption(f"Active project: **{project}**")
        section = st.radio("View", _ADMIN_SECTIONS, horizontal=True, key="admin_section")

    records = load_records(project)
    if records.empty:
        st.info("No records loaded.")
        return

    if section in ("Approval queue", "Escalated only"):
        checks = _prepare_checks_for_queue(project)
        _render_approval_queue(
            project,
            records,
            checks,
            actor,
            role,
            user,
            escalated_only=(section == "Escalated only"),
        )
        return

    _run_admin_maintenance(project)

    if section == "High change-rate":
        threshold = st.slider("High change threshold (%)", min_value=5, max_value=50, value=10)
        change_tbl = _change_rate_table(project, records)
        high = change_tbl[change_tbl["CHANGE_PCT"] >= threshold]
        info_strip(f"{len(high)} record(s) changed at or above {threshold}% of fields")
        section_title("High change-rate records")
        st.dataframe(high, use_container_width=True, hide_index=True)
        if not high.empty:
            rid = st.selectbox("Inspect record", options=high["RECORD_ID"].astype(str).tolist(), key="chg_inspect")
            if rid:
                render_record_card(project, rid, user, allow_admin=True)


def _render_approval_queue(project, records, checks, actor, role, user, *, escalated_only: bool) -> None:
    if checks.empty:
        st.info("No flag results for this project yet. Run the flags pipeline on Sync & Admin.")
        return

    flagged = pd.to_numeric(checks["SUM_ALL_CHECKS"], errors="coerce").fillna(0) > 0
    escalated = (
        history_svc.coerce_bool_series(checks["ESCALATED"])
        if "ESCALATED" in checks.columns
        else pd.Series(False, index=checks.index)
    )
    not_approved = ~history_svc.coerce_bool_series(checks["ADMIN_APPROVED"].fillna(False))

    if escalated_only:
        pending = checks[escalated & not_approved]
        info_strip("Records escalated to admin that are not yet admin-approved.")
    else:
        pending = checks[(flagged | escalated) & not_approved]

    section_title("Approval summary")
    stats_bar([("Records awaiting admin approval", str(len(pending)))])
    if pending.empty:
        flagged_count = int(flagged.sum())
        if flagged_count and not escalated_only:
            st.info(
                f"{flagged_count} flagged record(s) exist but all are already admin-approved. "
                "Use Reject or clear ADMIN_APPROVED to re-queue."
            )
        else:
            st.success("Nothing pending approval.")
        return

    pending = pending.copy()
    pending["RECORD_ID"] = pending["RECORD_ID"].astype(str)
    records = records.copy()
    records["RECORD_ID"] = records["RECORD_ID"].astype(str)

    merged = pending.merge(
        records[["RECORD_ID", "FINAL_USAGE", "FINAL_REVIEWER", "ROUTE_SURVEYED_CODE"]],
        on="RECORD_ID",
        how="left",
    )
    show_cols = [
        "RECORD_ID",
        "ROUTE_SURVEYED_CODE",
        "FINAL_USAGE",
        "FINAL_REVIEWER",
        "SUM_ALL_CHECKS",
        "ESCALATED",
        "TWO_X_REVIEWED_BY",
        "TWO_X_REVIEWED_FLAG",
    ]
    show_cols = [c for c in show_cols if c in merged.columns]
    view = merged[show_cols]
    section_title("Approval queue")
    st.caption("Select one or more records, then approve or reject from this queue. Use Inspect a record for full history.")
    st.dataframe(view, use_container_width=True, hide_index=True)

    ids = merged["RECORD_ID"].astype(str).tolist()
    with st.container(border=True):
        st.markdown("**Approval actions**")
        selected = st.multiselect("Select records", options=ids, key=f"adm_sel_{escalated_only}")
        note = st.text_input("Note (optional)", key=f"adm_note_{escalated_only}")
        c1, c2, c3 = st.columns(3)
        if c1.button("Approve selected", type="primary", disabled=not selected, key=f"adm_ok_{escalated_only}", use_container_width=True):
            for rid in selected:
                history_svc.set_admin_approved(project, rid, actor, role, approved=True, note=note or None)
            st.session_state.pop(f"admin_maint_v1_{project}", None)
            st.success(f"Approved {len(selected)} record(s).")
            st.rerun()
        if c2.button("Reject selected", disabled=not selected, key=f"adm_rej_{escalated_only}", use_container_width=True):
            for rid in selected:
                history_svc.set_admin_approved(project, rid, actor, role, approved=False, note=note or "Admin-Reject")
            st.session_state.pop(f"admin_maint_v1_{project}", None)
            st.success(f"Rejected {len(selected)} record(s).")
            st.rerun()
        if c3.button("Approve ALL pending", disabled=pending.empty, key=f"adm_all_{escalated_only}", use_container_width=True):
            for rid in ids:
                history_svc.set_admin_approved(project, rid, actor, role, approved=True, note=note or None)
            st.session_state.pop(f"admin_maint_v1_{project}", None)
            st.success(f"Approved {len(ids)} record(s).")
            st.rerun()

    section_title("Inspect record")
    rid = st.selectbox("Inspect a record", options=ids, key=f"approval_inspect_{escalated_only}")
    if rid:
        render_record_card(project, rid, user, allow_admin=True)
