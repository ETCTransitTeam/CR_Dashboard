from __future__ import annotations

import streamlit as st

from core.data_access import load_records
from services import quality
from services import demographic_rules
from views.ui import (
    action_row,
    empty_state,
    info_strip,
    loading,
    page_header,
    section_title,
    set_operation_flash,
    stats_bar,
)


def _ensure_demographics(project: str, *, force: bool = False) -> bool:
    """Generate demographic checks from Elvis Review + Elvis export when data is stale."""
    with loading("Checking demographic source data..."):
        records = load_records(project)
        fingerprint = quality.demographics_data_fingerprint(project)
        output = quality.load_demographic_checks(project, status="fail")
    if records.empty:
        return False

    cache_key = f"demo_gen_fp_{project}"
    stale = st.session_state.get(cache_key) != fingerprint
    if not force and not stale and not output.empty:
        return True

    label = "Regenerating demographic checks..." if force else "Generating configured demographic checks..."
    with loading(label):
        try:
            result = quality.generate_demographic_checks_from_review(project)
            st.session_state[cache_key] = fingerprint
            if force:
                set_operation_flash(f"Regenerated {len(result)} failed check row(s).")
            elif stale or output.empty:
                info_strip(f"Loaded {len(result)} configured demographic check row(s).")
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

    with action_row():
        c1, c2 = st.columns([1, 1])
        if c2.button("Refresh demographic checks", help="Re-run configured demographic rules"):
            if _ensure_demographics(project, force=True):
                st.rerun()
        else:
            _ensure_demographics(project)

    with loading("Evaluating demographic review results..."):
        output = demographic_rules.evaluate_project_script_output(project)
    if output.empty:
        st.info(
            "No failed demographic checks, or checks could not be generated. "
            "Click **Refresh demographic checks** after confirming the Elvis export is available."
        )
        return

    section_title("Demographic check summary")
    id_col = "id" if "id" in output.columns else ("elvis_id" if "elvis_id" in output.columns else None)
    flag_cols = [column for column in demographic_rules.script_flag_columns(project) if column in output.columns]
    unique_records = output[id_col].astype(str).nunique() if id_col else len(output)
    failed_checks = int(output[flag_cols].sum().sum()) if flag_cols else len(output)
    stats_bar(
        [
            ("Flagged records", str(unique_records)),
            ("Failed checks", str(failed_checks)),
            ("Flag columns", str(len(flag_cols))),
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
