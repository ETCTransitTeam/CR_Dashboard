from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from core.data_access import load_records, records_to_elvis_review
from pipeline.runner import build_context, run_post_cleaning_pipeline
from services.field_team import (
    REMOVE_DELETE_EDITABLE,
    SUPERVISOR_REMARK_EDITABLE,
    build_remove_or_delete_sheet,
    build_supervisor_remark_sheet,
    field_team_workbook_bytes,
    persist_remove_delete_edits,
    persist_supervisor_remark_edits,
)
from views.filters import apply_record_filters, subset_records_for_display
from views.grid_tooltips import grid_widget_key
from views.ui import action_row, empty_state, info_strip, page_header, section_title


def _norm(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _frames_differ(before: pd.DataFrame, after: pd.DataFrame, fields: set[str]) -> bool:
    for field in fields:
        if field not in before.columns or field not in after.columns:
            continue
        for i in range(len(before)):
            if _norm(before.iloc[i][field]) != _norm(after.iloc[i][field]):
                return True
    return False


def _prepare_for_editor(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].fillna("").astype(str)
    return out


def _render_field_sheet(
    sheet: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
    *,
    editor_key: str,
    editable_fields: set[str],
    persist_fn,
) -> pd.DataFrame:
    if sheet.empty:
        empty_state("No matching records", "Try adjusting your filters or selecting another project.")
        return sheet

    prepared = _prepare_for_editor(sheet)
    disabled = [col for col in prepared.columns if col not in editable_fields]
    edited = st.data_editor(
        prepared,
        disabled=disabled,
        use_container_width=True,
        hide_index=True,
        key=grid_widget_key(editor_key, prepared),
    )
    if _frames_differ(prepared, edited, editable_fields):
        saved = persist_fn(prepared, edited, records, user)
        if saved:
            st.success(f"Saved {saved} field change(s).")
            st.rerun()
    return edited


def render_field_page(user: dict) -> None:
    from core.session_project import require_active_project

    page_header(
        "Field Team Workspace",
        "Remove/Delete and Supervisor Remark sheets — same layout as the field-team Excel export.",
    )

    project = require_active_project()
    if not project:
        return

    records = load_records(project)
    if records.empty:
        empty_state("No records available", "Run Fetch latest records on Elvis Review or Sync & Admin for this project.")
        return

    elvis_review = records_to_elvis_review(records)
    remove_sheet = build_remove_or_delete_sheet(records, elvis_review=elvis_review)
    supervisor_sheet = build_supervisor_remark_sheet(records, project, elvis_review=elvis_review)

    tab_remove, tab_supervisor = st.tabs(["Remove or Delete", "Supervisor Remark"])

    with tab_remove:
        section_title("Remove or Delete sheet")
        info_strip("Records where **Final Usage = Remove** or **Elvis Status = Delete** (valid 5-min surveys).")
        filtered = apply_record_filters(remove_sheet, key_prefix="field_remove", include_usage=False)
        visible_records = subset_records_for_display(
            filtered.rename(columns={"ID": "id"}),
            records,
        )
        info_strip(f"{len(filtered)} record(s) shown")
        _render_field_sheet(
            filtered,
            visible_records,
            user,
            editor_key="field_remove_editor",
            editable_fields=REMOVE_DELETE_EDITABLE,
            persist_fn=persist_remove_delete_edits,
        )

    with tab_supervisor:
        section_title("Supervisor Remark sheet")
        info_strip("Use records with a supervisor **ElvisRemark** (excludes Remove/Delete rows).")
        filtered = apply_record_filters(supervisor_sheet, key_prefix="field_supervisor", include_usage=False)
        visible_records = subset_records_for_display(
            filtered.rename(columns={"ID": "id"}),
            records,
        )
        info_strip(f"{len(filtered)} record(s) shown")
        _render_field_sheet(
            filtered,
            visible_records,
            user,
            editor_key="field_supervisor_editor",
            editable_fields=SUPERVISOR_REMARK_EDITABLE,
            persist_fn=persist_supervisor_remark_edits,
        )

    st.divider()
    section_title("Export")
    with action_row():
        st.download_button(
            "Download field-team Excel",
            data=field_team_workbook_bytes(remove_sheet, supervisor_sheet),
            file_name=f"{project}_Removed_or_Deleted_Records_by_{date.today():%Y%m%d}.xlsx".replace(" ", "_"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    section_title("Generate Removed IDs (pipeline export)")
    with action_row():
        if st.button("Generate Removed IDs export"):
            try:
                with st.spinner("Running field-team script..."):
                    ctx = build_context(project)
                    outputs = run_post_cleaning_pipeline(ctx)
                removed_path = outputs.get("removed_ids_xlsx")
                if removed_path and removed_path.exists():
                    with open(removed_path, "rb") as handle:
                        st.download_button(
                            "Download pipeline Removed/Deleted file",
                            data=handle.read(),
                            file_name=removed_path.name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                else:
                    st.warning("Removed IDs file was not generated.")
            except Exception as exc:
                st.error(f"Export failed: {exc}")
