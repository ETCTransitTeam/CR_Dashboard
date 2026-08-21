"""Snowflake / Excel-upload switch for Elvis Review and Combined Checks."""

from __future__ import annotations

from datetime import datetime

import streamlit as st

from rc_auth.access import is_super_admin_user
from services.sheets_sync import (
    DEFAULT_WORKSHEET,
    format_import_result,
    import_sheet_into_snowflake,
    read_uploaded_workbook,
)
from views.ui import progress_status, set_operation_flash

SOURCE_KEY = "rcd_elvis_data_source"
LAST_IMPORT_KEY = "rcd_sheets_last_import"
RESET_AFTER_IMPORT_KEY = "rcd_excel_reset_after_import"
SOURCE_SNOWFLAKE = "Snowflake"
SOURCE_UPLOAD = "Excel upload"


def can_import_excel_upload(user: dict | None) -> bool:
    if not user:
        return False
    role = str(user.get("ROLE") or user.get("role") or "").strip().lower()
    if role in {"admin", "manager"}:
        return True
    return is_super_admin_user(user)


def render_data_source_control(user: dict, project: str) -> None:
    """Shared source switch. Import writes into Snowflake; pages keep reading Snowflake."""
    if not can_import_excel_upload(user):
        return

    reset_project = st.session_state.pop(RESET_AFTER_IMPORT_KEY, None)
    if reset_project:
        upload_key = f"rcd_excel_upload_{reset_project}"
        if upload_key in st.session_state:
            del st.session_state[upload_key]
        st.session_state[SOURCE_KEY] = SOURCE_SNOWFLAKE

    if st.session_state.get(SOURCE_KEY) not in {SOURCE_SNOWFLAKE, SOURCE_UPLOAD}:
        st.session_state[SOURCE_KEY] = SOURCE_SNOWFLAKE

    with st.container(border=True):
        st.radio(
            "Data source",
            options=[SOURCE_SNOWFLAKE, SOURCE_UPLOAD],
            horizontal=True,
            key=SOURCE_KEY,
            help=(
                "Snowflake is the live source for these pages. "
                "Excel upload imports matching decision fields into Snowflake, then the pages reload from Snowflake."
            ),
        )
        last = st.session_state.get(LAST_IMPORT_KEY) or {}
        if last.get("project") == project and last.get("message"):
            when = last.get("at") or ""
            suffix = f" ({when})" if when else ""
            st.caption(f"Last import{suffix}: {last['message']}")
        if st.session_state.get(SOURCE_KEY) != SOURCE_UPLOAD:
            return

        st.caption(
            f"Upload the KingElvis / SharePoint workbook for `{project}`. "
            "Existing Snowflake records only."
        )
        uploaded = st.file_uploader(
            "Excel workbook (.xlsx)",
            type=["xlsx"],
            key=f"rcd_excel_upload_{project}",
            help="Download the file from SharePoint, then upload it here.",
        )
        worksheet = st.text_input(
            "Worksheet name",
            value=DEFAULT_WORKSHEET,
            key=f"rcd_excel_worksheet_{project}",
        )

        if st.button(
            "Import uploaded workbook",
            type="primary",
            disabled=uploaded is None,
            key="rcd_excel_import_btn",
            help="Write workbook changes into Snowflake for matching elvis_id / id rows.",
        ):
            actor = user.get("name") or user.get("EMAIL") or "unknown"
            role = user.get("ROLE") or user.get("role") or ""
            try:
                with progress_status(
                    f"Importing workbook into Snowflake for {project}...",
                    complete_label="Workbook import finished",
                ) as update:
                    update(1, 2, "Reading the uploaded workbook and matching records...")
                    sheet = read_uploaded_workbook(uploaded, worksheet)
                    result = import_sheet_into_snowflake(project, str(actor), str(role), sheet)
                    update(2, 2, "Refreshing the dashboard cache...")
                message = format_import_result(result)
                st.session_state[LAST_IMPORT_KEY] = {
                    "project": project,
                    "message": message,
                    "at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
                st.session_state[RESET_AFTER_IMPORT_KEY] = project
                set_operation_flash(f"Import finished. {message}")
                st.rerun()
            except Exception as exc:
                st.error(f"Workbook import failed: {exc}")
