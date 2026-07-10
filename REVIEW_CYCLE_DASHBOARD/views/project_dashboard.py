from __future__ import annotations

import pandas as pd
import streamlit as st

from core.data_access import load_records
from core.projects import list_projects
from services import analytics
from services import quality
from views.ui import (
    drill_section_label,
    empty_state,
    metric_row,
    page_header,
    section_title,
)
from views.ui.table import render_reference_table


def _pct_of_total(value: int, total: int) -> str:
    if not total:
        return "—"
    return f"{value / total * 100:.1f}% of total"


def render_project_dashboard(user: dict) -> None:
    page_header(
        "Project Dashboard",
        "Portfolio view of records across all projects.",
    )

    projects = list_projects()
    if projects.empty:
        empty_state(
            "No projects configured",
            "Use Sync & Admin to load projects from APP_CONFIG.",
        )
        return

    rows = []
    all_records = load_records()
    for project in projects["PROJECT_NAME"].tolist():
        proj_records = (
            all_records[all_records["PROJECT_NAME"] == project]
            if not all_records.empty
            else pd.DataFrame()
        )
        summary = analytics.status_summary(project, records=proj_records)
        rows.append(
            {
                "Project": project,
                "Total": summary["total"],
                "New": summary["new"],
                "Cleaned (Use)": summary["cleaned"],
                "Reviewed": summary["reviewed"],
                "Removed": summary["removed"],
                "Pending": summary["pending"],
            }
        )
    overview = pd.DataFrame(rows)

    totals = overview[["Total", "Cleaned (Use)", "Reviewed", "Removed", "Pending"]].sum()
    total_n = int(totals["Total"])
    section_title("Portfolio Summary")
    metric_row([
        ("Total records", f"{total_n:,}", "All time total"),
        ("Cleaned (Use)", f"{int(totals['Cleaned (Use)']):,}", _pct_of_total(int(totals["Cleaned (Use)"]), total_n)),
        ("Reviewed", f"{int(totals['Reviewed']):,}", _pct_of_total(int(totals["Reviewed"]), total_n)),
        ("Removed", f"{int(totals['Removed']):,}", _pct_of_total(int(totals["Removed"]), total_n)),
        ("Pending", f"{int(totals['Pending']):,}", _pct_of_total(int(totals["Pending"]), total_n)),
    ])

    render_reference_table(
        overview,
        key="project_portfolio",
        title="Portfolio Table",
        entity_label="projects",
    )

    drill_section_label()
    from core.session_project import require_active_project

    project = require_active_project()
    if not project:
        return
    if project not in overview["Project"].tolist():
        empty_state(
            "Project not in portfolio",
            f"Active project **{project}** has no portfolio row yet.",
        )
        return

    proj_records = (
        all_records[all_records["PROJECT_NAME"] == project]
        if not all_records.empty
        else pd.DataFrame()
    )
    score = analytics.project_quality_score(project, records=proj_records)
    metric_row([
        ("Quality score", str(score["quality_score"]), "Project health index"),
        ("Removal rate", f"{score['removal_rate']}%", "Share of removed records"),
        ("Removed", f"{score['removed']:,} / {score['total']:,}", "Removed vs total"),
    ], columns=3, variant="analytics")

    alerts = quality.list_alerts(project)
    if alerts.empty:
        empty_state("No alerts", "No data quality alerts for this project.")
    else:
        st.warning(f"{len(alerts)} data quality alert(s)")
        st.dataframe(
            alerts[["ALERT_TYPE", "SUBJECT", "METRIC_VALUE", "THRESHOLD", "SEVERITY", "MESSAGE"]],
            use_container_width=True,
            hide_index=True,
        )
