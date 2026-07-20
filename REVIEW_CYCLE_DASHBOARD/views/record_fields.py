"""Elvis_Review fields that users may edit anywhere in the dashboard."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from services import history as history_svc
from views.ui import loading

USAGE_OPTIONS = ["", "Use", "Remove"]

# (payload_key, label, widget kind)
EDITABLE_FIELDS: list[tuple[str, str, str]] = [
    ("Final_Usage", "Final_Usage", "select"),
    ("FINAL_REVIEWER", "FINAL_REVIEWER", "text"),
    ("REASON FOR REMOVAL", "REASON FOR REMOVAL", "text"),
    ("REASON FOR REMOVAL [Other]", "REASON FOR REMOVAL [Other]", "text"),
    ("POSSIBLE ERRORS", "POSSIBLE ERRORS", "textarea"),
]

EDITABLE_FIELD_NAMES = frozenset(field for field, _, _ in EDITABLE_FIELDS)


def _norm(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def field_value(payload: dict, record_row: Any, field: str) -> str:
    """Read a field from payload, with Final_Usage fallback to typed RECORDS column."""
    if field == "Final_Usage":
        usage = _norm(payload.get("Final_Usage"))
        if usage:
            return usage
        if record_row is not None:
            if isinstance(record_row, dict):
                return _norm(record_row.get("FINAL_USAGE"))
            if hasattr(record_row, "get"):
                return _norm(record_row.get("FINAL_USAGE"))
    return _norm(payload.get(field))


def filter_editable_updates(updates: dict[str, Any]) -> dict[str, Any]:
    return {field: value for field, value in updates.items() if field in EDITABLE_FIELD_NAMES}


def prepare_editable_display(display: pd.DataFrame) -> pd.DataFrame:
    """Normalize editable columns for ``st.data_editor``."""
    out = display.copy()
    for field in EDITABLE_FIELD_NAMES:
        if field in out.columns:
            out[field] = out[field].fillna("").astype(str)
    return out


def _record_id_column(display: pd.DataFrame) -> str | None:
    for col in ("elvis_id", "id"):
        if col in display.columns:
            return col
    return None


def _editable_frames_differ(before: pd.DataFrame, after: pd.DataFrame) -> bool:
    id_col = _record_id_column(before)
    if not id_col or id_col not in after.columns:
        for field in EDITABLE_FIELD_NAMES:
            if field not in before.columns or field not in after.columns:
                continue
            for i in range(min(len(before), len(after))):
                if _norm(before.iloc[i][field]) != _norm(after.iloc[i][field]):
                    return True
        return False

    before_map = {
        _norm(row[id_col]): row
        for _, row in before.iterrows()
        if _norm(row[id_col])
    }
    for _, row in after.iterrows():
        record_id = _norm(row[id_col])
        if not record_id or record_id not in before_map:
            continue
        prev = before_map[record_id]
        for field in EDITABLE_FIELD_NAMES:
            if field not in before.columns or field not in after.columns:
                continue
            if _norm(prev[field]) != _norm(row[field]):
                return True
    return False


def persist_editable_elvis_changes(
    before: pd.DataFrame,
    after: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
) -> int:
    """Save inline table edits to Snowflake."""
    id_col = _record_id_column(before)
    if not id_col or records.empty or id_col not in after.columns:
        return 0

    project_by_id = records.set_index(records["RECORD_ID"].astype(str))["PROJECT_NAME"].to_dict()
    actor = user.get("name") or user.get("EMAIL")
    role = user.get("ROLE") or user.get("role")
    saved = 0

    before_map = {
        _norm(row[id_col]): row
        for _, row in before.iterrows()
        if _norm(row[id_col])
    }
    for _, row in after.iterrows():
        record_id = _norm(row[id_col])
        if not record_id or record_id not in before_map:
            continue
        project = project_by_id.get(record_id)
        if not project:
            continue
        prev = before_map[record_id]
        updates: dict[str, Any] = {}
        for field in EDITABLE_FIELD_NAMES:
            if field not in before.columns or field not in after.columns:
                continue
            old_val = prev[field]
            new_val = row[field]
            if _norm(old_val) != _norm(new_val):
                updates[field] = new_val
        if updates:
            saved += history_svc.apply_record_update(
                project,
                record_id,
                updates,
                actor,
                role,
                action="Edit",
            )
    return saved


def editable_column_config(display: pd.DataFrame) -> dict[str, st.column_config.Column]:
    config: dict[str, st.column_config.Column] = {}
    for field, _, kind in EDITABLE_FIELDS:
        if field not in display.columns:
            continue
        if kind == "select":
            config[field] = st.column_config.SelectboxColumn(
                field,
                options=USAGE_OPTIONS,
                required=False,
            )
        else:
            config[field] = st.column_config.TextColumn(field)
    return config


def _strip_for_config(display: pd.DataFrame) -> pd.DataFrame:
    return display.drop(columns=[c for c in display.columns if c.startswith("__tip_")], errors="ignore")


def render_editable_elvis_table(
    display: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
    *,
    editor_key: str,
    project_name: str | None = None,
    show_history: bool = True,
    history_actor_roles: list[str] | None = None,
) -> pd.DataFrame:
    """Elvis_Review grid with inline editing for the five editable fields."""
    from views.grid_tooltips import attach_field_tooltips, render_history_data_editor

    if display.empty:
        return display

    @st.fragment
    def _editor_fragment() -> pd.DataFrame:
        prepared = prepare_editable_display(display)
        empty_msg = "No cleaning history yet." if history_actor_roles else "No decision history yet."
        id_col = _record_id_column(prepared)
        if show_history and id_col:
            prepared = attach_field_tooltips(
                prepared,
                id_col,
                EDITABLE_FIELD_NAMES,
                project_name=project_name,
                records=records,
                actor_roles=history_actor_roles,
                empty_message=empty_msg,
            )

        edited = render_history_data_editor(
            prepared,
            editor_key=editor_key,
            editable_fields=set(EDITABLE_FIELD_NAMES),
            column_config=editable_column_config(_strip_for_config(prepared)),
            selectbox_options={"Final_Usage": USAGE_OPTIONS},
        )

        compare_before = _strip_for_config(prepared).drop(
            columns=["Assigned to me", "Assigned To"], errors="ignore"
        )
        compare_after = edited.drop(columns=["Assigned to me", "Assigned To"], errors="ignore")
        if _editable_frames_differ(compare_before, compare_after):
            # Avoid re-saving the same editor state on every fragment rerun.
            sig_key = f"{editor_key}__last_saved_sig"
            parts: list[str] = []
            id_c = _record_id_column(compare_after)
            for _, row in compare_after.iterrows():
                rid = _norm(row[id_c]) if id_c else ""
                vals = "|".join(
                    _norm(row[f]) if f in compare_after.columns else "" for f in EDITABLE_FIELD_NAMES
                )
                parts.append(f"{rid}:{vals}")
            signature = "\n".join(parts)
            if st.session_state.get(sig_key) != signature:
                with loading("Saving Elvis Review changes..."):
                    changed = persist_editable_elvis_changes(
                        compare_before, compare_after, records, user
                    )
                if changed:
                    st.session_state[sig_key] = signature
                    st.toast(f"Saved {changed} field change(s).")
        return edited

    return _editor_fragment()


def render_editable_form(payload: dict, record_row: Any, form_key: str) -> dict[str, Any]:
    """Render editable Elvis_Review widgets inside an existing ``st.form``."""
    c1, c2 = st.columns(2)
    current_usage = field_value(payload, record_row, "Final_Usage")
    usage_index = USAGE_OPTIONS.index(current_usage) if current_usage in USAGE_OPTIONS else 0
    values: dict[str, Any] = {
        "Final_Usage": c1.selectbox(
            "Final_Usage",
            USAGE_OPTIONS,
            index=usage_index,
            key=f"{form_key}_Final_Usage",
        ),
        "FINAL_REVIEWER": c2.text_input(
            "FINAL_REVIEWER",
            value=field_value(payload, record_row, "FINAL_REVIEWER"),
            key=f"{form_key}_FINAL_REVIEWER",
        ),
        "REASON FOR REMOVAL": c1.text_input(
            "REASON FOR REMOVAL",
            value=field_value(payload, record_row, "REASON FOR REMOVAL"),
            key=f"{form_key}_REASON_FOR_REMOVAL",
        ),
        "REASON FOR REMOVAL [Other]": c2.text_input(
            "REASON FOR REMOVAL [Other]",
            value=field_value(payload, record_row, "REASON FOR REMOVAL [Other]"),
            key=f"{form_key}_REASON_FOR_REMOVAL_OTHER",
        ),
        "POSSIBLE ERRORS": st.text_area(
            "POSSIBLE ERRORS",
            value=field_value(payload, record_row, "POSSIBLE ERRORS"),
            key=f"{form_key}_POSSIBLE_ERRORS",
        ),
    }
    return values
