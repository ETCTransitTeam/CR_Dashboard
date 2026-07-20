from __future__ import annotations

import streamlit as st

from core.data_access import load_records
from services import quality
from views.ui import (
    action_row,
    empty_state,
    loading,
    page_header,
    section_title,
    set_operation_flash,
    stats_bar,
)


def _refresh_demographics(project: str) -> bool:
    """Re-run rules and persist failed checks to Snowflake. Button-only."""
    with loading("Checking demographic source data..."):
        records = load_records(project)
    if records.empty:
        st.error("No Elvis Review records found for this project.")
        return False

    with loading("Refreshing demographic checks..."):
        try:
            result = quality.generate_demographic_checks_from_review(project)
            set_operation_flash(f"Refreshed {len(result)} failed check row(s).")
            return True
        except Exception as exc:
            st.error(f"Demographic checks failed: {exc}")
            st.info(
                "Demographics need (1) Elvis Review records in the dashboard and "
                "(2) the full Elvis ODBC export from a prior Sync & Admin run "
                "(StudentStatusCode, YourAge, HHSize, etc.)."
            )
            return False


def render_demographic_page(user: dict) -> None:
    from core.session_project import require_active_project

    page_header(
        "Demographic Review",
        "Runs configured demographic review flags on **Final_Usage = Use** records and shows Snowflake-backed results.",
    )

    project = require_active_project()
    if not project:
        return

    with loading("Loading demographic review records..."):
        records = load_records(project)
    if records.empty:
        empty_state("No records yet", "Run Sync & Admin or Fetch latest records on Elvis Review first.")
        return

    refresh_clicked = st.button(
        "Refresh demographic checks",
        help="Re-run configured demographic rules for the active project when Elvis Review data changes.",
        type="primary",
        key="demo_refresh_checks",
        icon=":material/refresh:",
    )
    if refresh_clicked:
        if _refresh_demographics(project):
            st.rerun()

    with loading("Loading saved demographic check results..."):
        output = quality.load_demographic_checks(project, status="fail")

    if output is None or output.empty:
        st.info(
            "No demographic check results in Snowflake yet. "
            "Click **Refresh demographic checks** to run the rules and save them."
        )
        return

    section_title("Demographic check summary")
    unique_records = int(output["RECORD_ID"].astype(str).nunique()) if "RECORD_ID" in output.columns else len(output)
    failed_checks = len(output)
    flag_count = int(output["CHECK_TYPE"].astype(str).nunique()) if "CHECK_TYPE" in output.columns else 0
    stats_bar(
        [
            ("Flagged records", str(unique_records)),
            ("Failed checks", str(failed_checks)),
            ("Flag types", str(flag_count)),
        ]
    )

    section_title("Export")
    with action_row():
        st.download_button(
            "Download CSV",
            data=output.to_csv(index=False).encode("utf-8"),
            file_name=f"{project}_demographic_checks.csv".replace(" ", "_"),
            mime="text/csv",
        )

    section_title("Flagged demographic records")
    st.dataframe(output, use_container_width=True, hide_index=True)
