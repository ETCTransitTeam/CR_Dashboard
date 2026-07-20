from __future__ import annotations

import pandas as pd
import streamlit as st

from services import history as history_svc
from views.filters import filter_review_team_history, review_dashboard_record_ids
from views.ui import action_row, filter_panel, info_strip, loading, page_header, section_title


DISPLAY_COLS = ["CREATED_AT", "RECORD_ID", "ACTOR", "ACTOR_ROLE", "FIELD_NAME", "OLD_VALUE", "NEW_VALUE", "ACTION"]


def render_history_page(user: dict) -> None:
    role = user.get("ROLE") or user.get("role")
    is_cleaning = role == "cleaning"
    is_review = role == "review"

    if is_cleaning:
        subtitle = "Review recent edits and decisions made on cleaning records."
    elif is_review:
        subtitle = (
            "Review-team audit trail for records on **Combined Checks** and **Supervisor View Only**. "
            "Showing **review team changes only**."
        )
    else:
        subtitle = "Audit trail: who changed what, when, and why."
    page_header("Record History", subtitle)

    from core.session_project import require_active_project

    project = require_active_project()
    if not project:
        return

    with filter_panel("Filters", "Narrow history by record ID, actor, or action."):
        record_filter = st.text_input("Filter by record ID (optional)")

    with loading("Loading project history…"):
        history = history_svc.load_history(project, record_filter.strip() or None)
    if history.empty:
        st.info("No history recorded yet for this project.")
        return

    view = history.copy()
    if is_cleaning:
        view = view[view["ACTOR_ROLE"].astype(str).str.lower() == "cleaning"]
        if view.empty:
            st.info("No cleaning-team history recorded yet for this project.")
            return
    elif is_review:
        with loading("Preparing review-team history…"):
            dashboard_ids = review_dashboard_record_ids(project)
            view = filter_review_team_history(view, dashboard_ids)
        if view.empty:
            st.info("No review-team history for Combined Checks or Supervisor View records yet.")
            return
    else:
        with filter_panel("Actor & action", "Optional filters for admin and manager views."):
            c1, c2 = st.columns(2)
            actors = sorted(history["ACTOR"].dropna().astype(str).unique())
            actor_filter = c1.multiselect("Actor", actors)
            actions = sorted(history["ACTION"].dropna().astype(str).unique())
            action_filter = c2.multiselect("Action", actions)
        if actor_filter:
            view = view[view["ACTOR"].astype(str).isin(actor_filter)]
        if action_filter:
            view = view[view["ACTION"].astype(str).isin(action_filter)]

    view = view.sort_values("CREATED_AT", ascending=False)
    cols = [c for c in DISPLAY_COLS if c in view.columns]
    section_title("Audit trail")
    info_strip(f"{len(view)} history entries")
    st.dataframe(view[cols], use_container_width=True, hide_index=True)

    section_title("Export")
    with loading("Preparing the history CSV download..."):
        history_csv = view[cols].to_csv(index=False).encode("utf-8")
    with action_row():
        st.download_button(
            "Download history (CSV)",
            data=history_csv,
            file_name=f"{project}_record_history.csv",
            mime="text/csv",
        )
