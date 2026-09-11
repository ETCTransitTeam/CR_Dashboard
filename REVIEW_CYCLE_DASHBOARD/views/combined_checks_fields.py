"""Combined Checks grid: Elvis editable fields + ADMIN_APPROVED + 2X review columns."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st

from core.s3_utils import dataframe_to_excel_bytes
from services import history as history_svc
from views.ui.loading import loading
from views.record_fields import (
    EDITABLE_FIELD_NAMES,
    USAGE_OPTIONS,
    _editable_frames_differ,
    _norm,
    _record_id_column,
    _strip_for_config,
    apply_cell_overrides,
    capture_cell_overrides,
    editable_column_config,
    prepare_editable_display,
)

TWO_X_FLAG_OPTIONS = ["", "Pass", "Fail", "Needs work"]

COMBINED_CHECK_FIELDS = frozenset({"ADMIN_APPROVED", "2x_REVIEWED_BY", "2x_REVIEWED_FLAG"})

ALL_EDITABLE = EDITABLE_FIELD_NAMES | COMBINED_CHECK_FIELDS

COMBINED_CHECK_LEADING_COLUMNS = (
    "Elvis_Date",
    "elvis_id",
    "Assigned To",
    "Assigned to me",
    "Final_Usage",
    "FINAL_REVIEWER",
    "ADMIN_APPROVED",
    "2x_REVIEWED_BY",
    "2x_REVIEWED_FLAG",
    "REASON FOR REMOVAL",
    "REASON FOR REMOVAL [Other]",
    "POSSIBLE ERRORS",
    "Traditional_Check",
    "OD_Distance_Check",
    "Transfer_Distance_Check",
    "StopListValidation_Check",
    "2X_REVIEW_CHECK",
    "SUM_ALL_CHECKS",
)


def _order_combined_columns(display: pd.DataFrame) -> pd.DataFrame:
    """Keep review decisions visible before the wider source-data columns."""
    leading = [column for column in COMBINED_CHECK_LEADING_COLUMNS if column in display.columns]
    remaining = [column for column in display.columns if column not in leading]
    return display.loc[:, leading + remaining]


def prepare_combined_display(display: pd.DataFrame) -> pd.DataFrame:
    out = _order_combined_columns(prepare_editable_display(display))
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


def _combined_checks_excel_bytes(display: pd.DataFrame) -> bytes | None:
    if display is None or display.empty:
        return None
    export = _strip_for_config(prepare_combined_display(display))
    if export.empty:
        return None
    return dataframe_to_excel_bytes({"Combined_Checks": export})


def _render_combined_checks_download(
    display: pd.DataFrame,
    project_name: str | None,
    editor_key: str,
) -> None:
    with loading("Preparing the Combined Checks Excel download..."):
        excel_bytes = _combined_checks_excel_bytes(display)
    if not excel_bytes:
        return
    label = (project_name or "combined_checks").replace(" ", "_")
    filename = f"{label}_combined_checks_{date.today():%Y%m%d}.xlsx"
    st.download_button(
        "Download Excel (Combined Checks)",
        data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"{editor_key}_combined_checks_xlsx",
    )


def persist_combined_changes(
    before: pd.DataFrame,
    after: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
) -> int:
    id_col = _record_id_column(before)
    if not id_col or records.empty:
        return 0

    def _rid(value: Any) -> str:
        text = _norm(value)
        if text.endswith(".0") and text[:-2].replace(".", "", 1).isdigit():
            return text[:-2]
        return text

    project_by_id = {
        _rid(rid): project
        for rid, project in zip(records["RECORD_ID"].tolist(), records["PROJECT_NAME"].tolist())
    }
    actor = user.get("name") or user.get("EMAIL")
    role = user.get("ROLE") or user.get("role")
    saved = 0

    for i in range(len(before)):
        record_id = _rid(before.iloc[i][id_col])
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
    from views.grid_tooltips import (
        attach_field_tooltips,
        consume_save_flash,
        history_grid_caption,
        mark_saved_flash,
        render_history_data_editor,
    )

    if display.empty:
        return display

    _render_combined_checks_download(display, project_name, editor_key)

    @st.fragment
    def _editor_fragment() -> pd.DataFrame:
        # Feedback sits ABOVE the tall grid so save state is always visible.
        feedback = st.empty()
        flash = consume_save_flash(editor_key)
        if flash:
            feedback.success(f"Saved {flash.get('count', 0)} change(s).")

        prepared = prepare_combined_display(display)
        prepared = apply_cell_overrides(prepared, editor_key, ALL_EDITABLE)
        empty_history_msg = history_svc.EMPTY_HISTORY_TOOLTIP
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
            history_grid_caption(review_only=bool(history_actor_roles))

        st.caption(
            f"{len(prepared)} record(s) in this grid. "
            "Column **#** is the row number in the current filtered/sorted view "
            "(filter OD_Distance_Check = 1 and # runs 1…N for those rows)."
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
        if flash:
            return compare_before

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
                feedback.info("Saving…")
                changed = persist_combined_changes(
                    compare_before, compare_after, records, user
                )
                if changed:
                    capture_cell_overrides(
                        compare_before, compare_after, editor_key, ALL_EDITABLE
                    )
                    st.session_state[sig_key] = signature
                    mark_saved_flash(editor_key, changed)
                    st.toast(f"Saved {changed} field change(s).", icon="✅")
                    # Refresh the page-level source frames. Fragment-only
                    # reruns retain the pre-save ``display`` closure.
                    st.rerun()
                else:
                    feedback.empty()
        return edited

    return _editor_fragment()
