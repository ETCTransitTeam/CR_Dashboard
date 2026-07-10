"""Thin Streamlit wrapper around services.sync for the OD freshness banner.

Detection (timestamp compare) is cheap and runs on render; the heavy pipeline
pull is only triggered explicitly by the user (Option C: manual refresh button).
"""

from __future__ import annotations

from datetime import datetime

import streamlit as st

from services.sync import mark_od_sync_seen, od_sync_status


def check_od_sync_available(project_name: str) -> dict:
    return od_sync_status(project_name)


def render_sync_banner(project_name: str) -> None:
    status = od_sync_status(project_name)
    if status["available"]:
        st.info(status["message"])
        if st.button("Pull latest records from shared sync", key=f"pull_od_sync_{project_name}"):
            st.session_state["trigger_pipeline"] = project_name
            mark_od_sync_seen(project_name, status.get("od_sync"))
            st.rerun()
    else:
        st.caption(status["message"])
