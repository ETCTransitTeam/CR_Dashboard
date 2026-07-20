from __future__ import annotations

import plotly.express as px
import streamlit as st

from core.data_access import load_records
from services import analytics
from services import quality
from views.ui import info_strip, loading, page_header, section_title, set_operation_flash, stats_bar

BAND_ORDER = ["<5%", "5%+", "10%+", "15%+"]


def _show_chart(fig) -> None:
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, Segoe UI, sans-serif", "color": "#111827"},
        margin={"l": 12, "r": 12, "t": 52, "b": 12},
        legend_title_text="",
    )
    st.plotly_chart(fig, use_container_width=True)


def _removal_chart(df, group_col, title):
    if df.empty:
        st.info("No data yet for this breakdown.")
        return
    banded = analytics.add_bands(df)
    st.dataframe(banded, use_container_width=True, hide_index=True)
    fig = px.bar(
        banded,
        x=group_col,
        y="REMOVAL_RATE",
        color="BAND",
        category_orders={"BAND": BAND_ORDER},
        title=title,
        hover_data=["TOTAL", "REMOVED"],
    )
    _show_chart(fig)


def render_manager_dashboard(user: dict) -> None:
    from core.session_project import require_active_project

    page_header("Manager Analytics", "Live removal, override, and productivity metrics for Jason and leadership.")

    project = require_active_project()
    if not project:
        return

    with loading("Loading manager overview…"):
        records = load_records(project)
        score = analytics.project_quality_score(project, records=records)
    stats_bar([
        ("Active project", project),
        ("Total records", str(score["total"])),
        ("Removed", str(score["removed"])),
        ("Removal rate", f"{score['removal_rate']}%"),
        ("Quality score", str(score["quality_score"])),
    ])

    tabs = st.tabs(["Removal analytics", "Reviewers & cleaners", "Trends", "Productivity", "Quality alerts"])

    with tabs[0]:
        with loading("Preparing removal analytics…"):
            route_removals = analytics.removal_by(project, "ROUTE_SURVEYED_CODE", records=records)
            interviewer_removals = analytics.removal_by(project, "INTERV_INIT", records=records)
            reviewer_removals = analytics.removal_by(project, "FINAL_REVIEWER", records=records)
            cleaner_removals = analytics.removal_by(project, "FIRST_CLEANER", records=records)
        section_title("Removal rate by route")
        _removal_chart(route_removals, "ROUTE_SURVEYED_CODE", "Route removal rate")
        section_title("Removal rate by interviewer")
        _removal_chart(interviewer_removals, "INTERV_INIT", "Interviewer removal rate")
        section_title("Removal rate by reviewer")
        _removal_chart(reviewer_removals, "FINAL_REVIEWER", "Reviewer removal rate")
        section_title("Removal rate by 1st Cleaner")
        _removal_chart(cleaner_removals, "FIRST_CLEANER", "Cleaner removal rate")

    with tabs[1]:
        with loading("Analyzing reviewer and cleaner activity…"):
            overrides = analytics.reviewer_override_rate(project)
            cleaners = analytics.cleaner_modification_rate(project)
        section_title("Reviewer override rate (decision history)")
        info_strip("How often each reviewer changed Final_Usage when acting on records.")
        if overrides.empty:
            st.info("No reviewer decisions recorded yet.")
        else:
            st.dataframe(overrides, use_container_width=True, hide_index=True)
            _show_chart(px.bar(overrides, x="ACTOR", y="OVERRIDE_RATE", hover_data=["CHANGES", "REMOVED"], title="Reviewer override %"))
        section_title("Cleaner modification volume")
        info_strip("Field edits logged per cleaner from the audit trail.")
        if cleaners.empty:
            st.info("No cleaning edits recorded yet.")
        else:
            st.dataframe(cleaners, use_container_width=True, hide_index=True)
            _show_chart(px.bar(cleaners, x="ACTOR", y="FIELD_EDITS", hover_data=["RECORDS_TOUCHED"], title="Cleaner field edits"))

    with tabs[2]:
        with loading("Preparing weekly activity trends…"):
            trends = analytics.weekly_trends(project)
        if trends.empty:
            st.info("No history yet to chart trends.")
        else:
            _show_chart(px.line(trends, x="WEEK", y=["REMOVALS", "CLEANING_EDITS", "REVIEW_ACTIONS"], markers=True, title="Weekly activity"))
            st.dataframe(trends, use_container_width=True, hide_index=True)

    with tabs[3]:
        with loading("Preparing productivity analytics…"):
            prod = analytics.productivity(project)
        if prod.empty:
            st.info("No productivity data yet.")
        else:
            _show_chart(px.bar(prod, x="WEEK", y="ACTIONS", color="ACTOR", title="Weekly actions per user"))
            st.dataframe(prod, use_container_width=True, hide_index=True)

    with tabs[4]:
        if st.button("Recompute quality alerts"):
            with loading("Recomputing removal and reviewer quality alerts..."):
                quality.compute_quality_alerts(project)
            set_operation_flash("Quality alerts refreshed.")
            st.rerun()
        with loading("Loading quality alerts…"):
            alerts = quality.list_alerts(project)
        if alerts.empty:
            st.success("No quality alerts.")
        else:
            st.dataframe(
                alerts[["ALERT_TYPE", "SUBJECT", "METRIC_VALUE", "THRESHOLD", "SEVERITY", "MESSAGE"]],
                use_container_width=True,
                hide_index=True,
            )
