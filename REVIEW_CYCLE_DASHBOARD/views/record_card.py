from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from core.config import REVIEW_CYCLE_SCHEMA
from core.data_access import enrich_payload_from_typed_columns, load_combined_checks, load_record, records_to_dataframe
from core.snowflake_conn import fetch_df
from services import assignments as assignment_svc
from services import history as history_svc
from services import notifications as notify_svc
from views.record_fields import render_editable_form
from views.ui import section_title, stats_bar

from pipeline.elvis_review_format import has_transfer_suggestions, suggestion_route_value

CHECK_COLUMNS = [
    ("TRADITIONAL_CHECK", "Traditional"),
    ("OD_DISTANCE_CHECK", "OD Distance"),
    ("TRANSFER_DISTANCE_CHECK", "Transfer Distance"),
    ("STOPLISTVALIDATION_CHECK", "Stop List"),
    ("TWO_X_REVIEW_CHECK", "2X Review"),
]


def _transfer_suggestions_frame(payload: dict) -> pd.DataFrame:
    rows = []
    for leg in range(1, 5):
        prev = suggestion_route_value(payload, leg, "PREV")
        nxt = suggestion_route_value(payload, leg, "NEXT")
        if prev or nxt:
            rows.append({"Leg": leg, "Suggested prev route": prev or "-", "Suggested next route": nxt or "-"})
    return pd.DataFrame(rows)


def _render_transfer_suggestions(payload: dict) -> None:
    if not has_transfer_suggestions(payload):
        st.caption("No transfer suggestions for this record.")
        return
    frame = _transfer_suggestions_frame(payload)
    st.dataframe(frame, use_container_width=True, hide_index=True)


def _load_original(project_name: str, record_id: str) -> dict:
    df = fetch_df(
        f"SELECT RECORD_PAYLOAD FROM {REVIEW_CYCLE_SCHEMA}.ORIGINAL_RECORDS "
        f"WHERE PROJECT_NAME = %s AND RECORD_ID = %s",
        (project_name, record_id),
    )
    if df.empty:
        return {}
    payload = df.iloc[0]["RECORD_PAYLOAD"]
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return payload or {}


def _norm(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _comparison_frame(original: dict, current: dict) -> pd.DataFrame:
    keys = sorted(set(original) | set(current))
    rows = []
    for key in keys:
        old = original.get(key)
        new = current.get(key)
        rows.append(
            {
                "Field": key,
                "Original": old,
                "Current": new,
                "Changed": _norm(old) != _norm(new),
            }
        )
    return pd.DataFrame(rows)


def _supervisor_comment(payload: dict, record_row) -> str:
    for key in ("ELVIS_COMMENT", "SUPERVISOR_COMMENT"):
        value = _norm(payload.get(key))
        if value:
            return value
    if record_row is not None and hasattr(record_row, "get"):
        for col in ("SUPERVISOR_COMMENT",):
            value = _norm(record_row.get(col))
            if value:
                return value
    return ""


def _first_cleaner(payload: dict, record_row) -> str:
    value = _norm(payload.get("1st Cleaner"))
    if value:
        return value
    if record_row is not None and hasattr(record_row, "get"):
        return _norm(record_row.get("FIRST_CLEANER"))
    return ""


def _render_cleaning_context(payload: dict, record_row) -> None:
    supervisor = _supervisor_comment(payload, record_row)
    cleaner = _first_cleaner(payload, record_row)
    if not supervisor and not cleaner:
        return
    with st.expander("Supervisor & cleaning context", expanded=bool(supervisor)):
        if supervisor:
            st.caption("Latest supervisor / Elvis cleaning guidance (refreshed on pipeline sync).")
            st.markdown("**Supervisor comment**")
            st.info(supervisor)
        if cleaner:
            st.caption(f"**1st Cleaner:** {cleaner}")


def _widget_key(prefix: str | None, name: str) -> str:
    return f"{prefix}_{name}" if prefix else name


def render_record_card(
    project_name: str,
    record_id: str,
    user: dict,
    allow_admin: bool = False,
    *,
    widget_key_prefix: str | None = None,
    history_actor_roles: list[str] | None = None,
) -> None:
    records = load_record(project_name, record_id)
    if records.empty:
        st.warning("Record not found.")
        return
    record_row = records.iloc[0]
    payload = enrich_payload_from_typed_columns(record_row, records_to_dataframe(records).iloc[0].to_dict())
    actor = user.get("name") or user.get("EMAIL")
    role = user.get("ROLE") or user.get("role")
    is_cleaning = role == "cleaning"

    history = history_svc.load_history(project_name, record_id)
    if history_actor_roles and not history.empty and "ACTOR_ROLE" in history.columns:
        allowed = {r.lower() for r in history_actor_roles}
        history = history[history["ACTOR_ROLE"].astype(str).str.lower().isin(allowed)]
    elif is_cleaning and not history.empty and "ACTOR_ROLE" in history.columns:
        history = history[history["ACTOR_ROLE"].astype(str).str.lower() == "cleaning"]

    empty_history_caption = "No decision history yet."
    if history_actor_roles:
        empty_history_caption = "No review history yet."
    elif is_cleaning:
        empty_history_caption = "No cleaning history yet."

    section_title(f"Record {record_id}")
    st.caption(
        history_svc.format_history_tooltip(history)
        if not history.empty
        else empty_history_caption
    )

    stats_bar([
        ("Route", str(payload.get("ROUTE_SURVEYEDCode") or record_row.get("ROUTE_SURVEYED_CODE") or "-")),
        ("Final Usage", str(payload.get("Final_Usage") or record_row.get("FINAL_USAGE") or "-")),
        ("Reviewer", str(payload.get("FINAL_REVIEWER") or "-")),
        ("Interviewer", str(payload.get("INTERV_INIT") or "-")),
    ])

    # ---- Flags panel ----
    checks = load_combined_checks(project_name)
    check_row = checks[checks["RECORD_ID"].astype(str) == str(record_id)]
    flagged = False
    if not check_row.empty:
        cr = check_row.iloc[0]
        flagged = bool(cr.get("SUM_ALL_CHECKS"))
        with st.expander(f"Flags (total {int(cr.get('SUM_ALL_CHECKS') or 0)})", expanded=flagged):
            stats_bar([(label, str(int(cr.get(col) or 0))) for col, label in CHECK_COLUMNS])
            st.caption(
                f"Admin approved: {bool(cr.get('ADMIN_APPROVED'))} | "
                f"2X by: {cr.get('TWO_X_REVIEWED_BY') or '-'} ({cr.get('TWO_X_REVIEWED_FLAG') or '-'})"
            )

    with st.expander("Transfer suggestions", expanded=has_transfer_suggestions(payload)):
        _render_transfer_suggestions(payload)

    _render_cleaning_context(payload, record_row)

    # ---- Edit form (Elvis_Review editable fields only) ----
    form_key = _widget_key(widget_key_prefix, f"edit_{project_name}_{record_id}")
    with st.expander("Edit record", expanded=True):
        with st.form(form_key):
            updates = render_editable_form(payload, record_row, form_key)
            saved = st.form_submit_button("Save changes", type="primary")

    if saved:
        changed = history_svc.apply_record_update(project_name, record_id, updates, actor, role, action="Save")
        if changed:
            st.success(f"Saved {changed} change(s).")
            st.rerun()
        else:
            st.info("No changes to save.")

    # ---- Comparison ----
    original = _load_original(project_name, record_id)
    with st.expander("Original vs current record"):
        compare = _comparison_frame(original, payload)
        changed_only = st.checkbox(
            "Show changed fields only",
            value=True,
            key=_widget_key(widget_key_prefix, f"changed_{record_id}"),
        )
        view = compare[compare["Changed"]] if changed_only else compare
        if view.empty:
            st.caption("No differences from the original record.")
        else:
            st.dataframe(view, use_container_width=True, hide_index=True)

    # ---- History ----
    with st.expander("Decision history"):
        if history.empty:
            st.caption(empty_history_caption)
        else:
            st.dataframe(
                history[["CREATED_AT", "ACTOR", "ACTOR_ROLE", "FIELD_NAME", "OLD_VALUE", "NEW_VALUE", "ACTION"]],
                use_container_width=True,
                hide_index=True,
            )

    _render_actions(project_name, record_id, user, actor, role, allow_admin, flagged, widget_key_prefix)


def _render_actions(
    project_name,
    record_id,
    user,
    actor,
    role,
    allow_admin,
    flagged,
    widget_key_prefix: str | None = None,
) -> None:
    with st.expander("Actions", expanded=True):
        if role == "cleaning":
            cols = st.columns(3)
            if cols[0].button("Mark Use", key=_widget_key(widget_key_prefix, f"use_{record_id}")):
                history_svc.set_final_usage(project_name, record_id, "Use", actor, role, action="Use")
                st.rerun()
            if cols[1].button("Complete cleaning", key=_widget_key(widget_key_prefix, f"complete_{record_id}")):
                assignment_svc.complete_assignment(project_name, record_id, team="cleaning")
                history_svc.apply_record_update(
                    project_name,
                    record_id,
                    {"1st Cleaner": actor},
                    actor,
                    role,
                    action="Complete",
                    editable_only=False,
                )
                history_svc.log_changes(project_name, record_id, [("cleaning_status", "", "completed")], actor, role, "Complete")
                st.success("Cleaning marked complete.")
                st.rerun()
            if cols[2].button("Request review", key=_widget_key(widget_key_prefix, f"reqrev_{record_id}")):
                for reviewer in (assignment_svc.team_members("review") or ["unassigned"]):
                    notify_svc.notify(
                        reviewer,
                        notify_svc.ADMIN_APPROVAL_REQUIRED,
                        f"Cleaning requested review on {record_id} ({project_name})",
                        project_name,
                        record_id,
                    )
                assignment_svc.assign_records(project_name, [record_id], "unassigned", team="review", priority=50)
                history_svc.log_changes(project_name, record_id, [("review_status", "", "requested")], actor, role, "Request-Review")
                st.success("Review requested.")
                st.rerun()
            return

        # review / manager / admin
        cols = st.columns(4)
        if cols[0].button("Approve (Use)", key=_widget_key(widget_key_prefix, f"approve_use_{record_id}")):
            history_svc.set_final_usage(project_name, record_id, "Use", actor, role, action="Approve")
            st.rerun()
        if cols[1].button("Remove", key=_widget_key(widget_key_prefix, f"remove_{record_id}")):
            history_svc.set_final_usage(project_name, record_id, "Remove", actor, role, action="Remove")
            st.rerun()
        if cols[2].button("Escalate to admin", key=_widget_key(widget_key_prefix, f"escalate_{record_id}")):
            for admin in (notify_svc.admins() or ["admin"]):
                notify_svc.notify(
                    admin,
                    notify_svc.ADMIN_APPROVAL_REQUIRED,
                    f"Record {record_id} escalated by {actor} ({project_name})",
                    project_name,
                    record_id,
                )
            history_svc.set_escalated(project_name, record_id, actor, role, escalated=True)
            st.success("Escalated to admin.")
            st.rerun()
        if allow_admin and flagged:
            if cols[3].button("Admin approve", key=_widget_key(widget_key_prefix, f"admin_ok_{record_id}")):
                history_svc.set_admin_approved(project_name, record_id, actor, role, approved=True)
                st.success("Admin approved.")
                st.rerun()
