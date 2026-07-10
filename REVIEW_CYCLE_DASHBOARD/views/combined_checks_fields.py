"""Combined Checks grid: Elvis editable fields + ADMIN_APPROVED + 2X review columns."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from services import history as history_svc
from views.record_fields import (
    EDITABLE_FIELD_NAMES,
    USAGE_OPTIONS,
    _editable_frames_differ,
    _norm,
    _record_id_column,
    _strip_for_config,
    editable_column_config,
    prepare_editable_display,
)

TWO_X_FLAG_OPTIONS = ["", "Pass", "Fail", "Needs work"]

COMBINED_CHECK_FIELDS = frozenset({"ADMIN_APPROVED", "2x_REVIEWED_BY", "2x_REVIEWED_FLAG"})

ALL_EDITABLE = EDITABLE_FIELD_NAMES | COMBINED_CHECK_FIELDS


def prepare_combined_display(display: pd.DataFrame) -> pd.DataFrame:
    out = prepare_editable_display(display)
    if "ADMIN_APPROVED" in out.columns:
        out["ADMIN_APPROVED"] = out["ADMIN_APPROVED"].fillna(False).astype(bool)
    for field in ("2x_REVIEWED_BY", "2x_REVIEWED_FLAG"):
        if field in out.columns:
            out[field] = out[field].fillna("").astype(str)
    return out


def _combined_frames_differ(before: pd.DataFrame, after: pd.DataFrame) -> bool:
    if _editable_frames_differ(before, after):
        return True
    for field in COMBINED_CHECK_FIELDS:
        if field not in before.columns or field not in after.columns:
            continue
        for i in range(len(before)):
            old_v = before.iloc[i][field]
            new_v = after.iloc[i][field]
            if field == "ADMIN_APPROVED":
                if bool(old_v) != bool(new_v):
                    return True
            elif _norm(old_v) != _norm(new_v):
                return True
    return False


def persist_combined_changes(
    before: pd.DataFrame,
    after: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
) -> int:
    id_col = _record_id_column(before)
    if not id_col or records.empty:
        return 0

    project_by_id = records.set_index(records["RECORD_ID"].astype(str))["PROJECT_NAME"].to_dict()
    actor = user.get("name") or user.get("EMAIL")
    role = user.get("ROLE") or user.get("role")
    saved = 0

    for i in range(len(before)):
        record_id = _norm(before.iloc[i][id_col])
        if not record_id:
            continue
        project = project_by_id.get(record_id)
        if not project:
            continue

        elvis_updates: dict[str, Any] = {}
        for field in EDITABLE_FIELD_NAMES:
            if field not in before.columns:
                continue
            old_val = before.iloc[i][field]
            new_val = after.iloc[i][field]
            if _norm(old_val) != _norm(new_val):
                elvis_updates[field] = new_val

        if "Final_Usage" in elvis_updates:
            usage_val = elvis_updates.pop("Final_Usage")
            usage_norm = _norm(usage_val).lower()
            if usage_norm == "use":
                saved += history_svc.set_final_usage(project, record_id, "Use", actor, role)
            elif usage_norm == "remove":
                saved += history_svc.set_final_usage(project, record_id, "Remove", actor, role)
            elif _norm(usage_val) != _norm(before.iloc[i].get("Final_Usage")):
                elvis_updates["Final_Usage"] = usage_val

        if elvis_updates:
            saved += history_svc.apply_record_update(
                project, record_id, elvis_updates, actor, role, action="Edit"
            )

        check_updates: dict[str, Any] = {}
        for field in COMBINED_CHECK_FIELDS:
            if field not in before.columns:
                continue
            old_val = before.iloc[i][field]
            new_val = after.iloc[i][field]
            if field == "ADMIN_APPROVED":
                if bool(old_val) != bool(new_val):
                    check_updates[field] = bool(new_val)
            elif _norm(old_val) != _norm(new_val):
                check_updates[field] = new_val
        if check_updates:
            saved += history_svc.set_combined_check_fields(
                project, record_id, check_updates, actor, role
            )
    return saved


def render_combined_checks_table(
    display: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
    *,
    editor_key: str,
    project_name: str | None = None,
    show_history: bool = True,
    history_actor_roles: list[str] | None = None,
) -> pd.DataFrame:
    """Combined Checks grid with Elvis + flag fields inline editing."""
    from views.grid_tooltips import attach_field_tooltips, render_history_data_editor

    if display.empty:
        return display

    @st.fragment
    def _editor_fragment() -> pd.DataFrame:
        prepared = prepare_combined_display(display)
        empty_history_msg = "No review history yet." if history_actor_roles else "No decision history yet."
        tooltip_fields = sorted(ALL_EDITABLE)
        id_col = _record_id_column(prepared)
        if show_history and id_col:
            prepared = attach_field_tooltips(
                prepared,
                id_col,
                tooltip_fields,
                project_name=project_name,
                records=records,
                actor_roles=history_actor_roles,
                empty_message=empty_history_msg,
            )

        config = editable_column_config(_strip_for_config(prepared))
        if "ADMIN_APPROVED" in prepared.columns:
            config["ADMIN_APPROVED"] = st.column_config.CheckboxColumn("ADMIN_APPROVED")
        if "2x_REVIEWED_BY" in prepared.columns:
            config["2x_REVIEWED_BY"] = st.column_config.TextColumn("2x_REVIEWED_BY")
        if "2x_REVIEWED_FLAG" in prepared.columns:
            config["2x_REVIEWED_FLAG"] = st.column_config.SelectboxColumn(
                "2x_REVIEWED_FLAG",
                options=TWO_X_FLAG_OPTIONS,
                required=False,
            )

        edited = render_history_data_editor(
            prepared,
            editor_key=editor_key,
            editable_fields=ALL_EDITABLE,
            column_config=config,
            selectbox_options={
                "Final_Usage": USAGE_OPTIONS,
                "2x_REVIEWED_FLAG": TWO_X_FLAG_OPTIONS,
            },
            checkbox_fields={"ADMIN_APPROVED"},
        )

        compare_before = _strip_for_config(prepared)
        compare_after = edited
        if _combined_frames_differ(compare_before, compare_after):
            sig_key = f"{editor_key}__last_saved_sig"
            parts: list[str] = []
            id_c = next((c for c in ("elvis_id", "id", "RECORD_ID") if c in compare_after.columns), None)
            for _, row in compare_after.iterrows():
                rid = str(row[id_c]).strip() if id_c else ""
                vals = "|".join(
                    str(row[c]).strip() if c in compare_after.columns else "" for c in sorted(ALL_EDITABLE)
                )
                parts.append(f"{rid}:{vals}")
            signature = "\n".join(parts)
            if st.session_state.get(sig_key) != signature:
                changed = persist_combined_changes(compare_before, compare_after, records, user)
                if changed:
                    st.session_state[sig_key] = signature
                    st.toast(f"Saved {changed} field change(s).")
        return edited

    return _editor_fragment()
