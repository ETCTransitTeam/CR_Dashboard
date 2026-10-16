from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from sam.config import (
    APP_NAME,
    DEFAULT_HUB,
    DEFAULT_MAX_HOURS,
    DEFAULT_MIN_HOURS,
    MAX_SWITCH_MINUTES,
    MIN_SWITCH_MINUTES,
)
from sam.exporting import assignment_labels, packet_bytes, packets_zip, workbook_bytes
from sam.suggest import (
    ALL_ROUTES,
    ANY_PLACE,
    connecting_blocks,
    cut_full_shift,
    cut_round_trip,
    describe_cut,
    eligible_ids,
    find_connect_id,
    find_start_id,
    first_open_block,
    is_garage,
    open_counts,
    explain_empty_batch,
    plan_batch,
    label_exists,
    last_assigned_row,
    propose_label,
    same_place,
    span_hours,
)
from sam.time_utils import (
    clean_token,
    clock_input_to_minutes,
    duration_hours,
    format_hours,
    format_time,
    subtract_minutes,
    time_to_minutes,
)
from sam.weekday import (
    auto_choice,
    block_sort_key,
    default_hub,
    has_type_column,
    inspect_workbook,
    load_sheet,
    next_assignment_number,
    open_regular_count,
    sample_frame,
    sample_workbook_bytes,
    split_bay_hubs,
)

SUGGESTED = "Suggested"
ASSIGNED = "Assigned"
GARAGE = "Garage"
OPEN = "Open"


def _seed(key: str, value) -> None:
    if key not in st.session_state:
        st.session_state[key] = value


def _selected_rows(state) -> list[int]:
    if state is None:
        return []
    selection = state.get("selection") if isinstance(state, dict) else getattr(state, "selection", None)
    if selection is None:
        return []
    rows = selection.get("rows") if isinstance(selection, dict) else getattr(selection, "rows", None)
    return list(rows or [])


def _clear_work() -> None:
    for key in (
        "frame",
        "notes",
        "suggestion",
        "undo",
        "revision",
        "excel_bytes",
        "excel_rev",
        "word_zip",
        "word_rev",
        "upload_bytes",
        "upload_name",
        "upload_token",
        "sheet_options",
        "builder_name",
        "builder_block",
        "builder_hub",
        "include_garage",
        "choose_any_block",
        "cut_note",
        "min_hours",
        "max_hours",
        "use_start_after",
        "start_after_time",
        "use_end_by",
        "end_by_time",
        "min_switch",
        "max_switch",
        "made_table",
        "flash",
        "_pick_token",
        "_clear_note",
        "packet_cache",
    ):
        st.session_state.pop(key, None)
    st.session_state["uploader_key"] = int(st.session_state.get("uploader_key", 0)) + 1


def _load_choice(option: dict[str, object]) -> None:
    frame = load_sheet(st.session_state["upload_bytes"], str(option["name"]), str(option["kind"]))
    st.session_state["frame"] = frame
    st.session_state["notes"] = {}
    st.session_state["suggestion"] = None
    st.session_state["undo"] = None
    st.session_state["revision"] = 0
    st.session_state["excel_bytes"] = None
    st.session_state["word_zip"] = None
    st.session_state["source_sheet"] = option["name"]
    st.session_state["source_kind"] = option["kind"]
    st.session_state["packet_cache"] = {}
    for key in (
        "builder_name",
        "builder_block",
        "builder_hub",
        "suggestion",
        "made_table",
        "_pick_token",
    ):
        st.session_state.pop(key, None)


def _inject_css() -> None:
    st.markdown(
        """
        <style>
        .stApp:has(.sam-mark) header[data-testid="stHeader"] {
            background: transparent;
        }
        .stApp:has(.sam-mark) [data-testid="stSidebar"],
        .stApp:has(.sam-mark) [data-testid="stSidebarCollapsedControl"] {
            display: none;
        }
        .stApp:has(.sam-mark) {
            background:
                radial-gradient(1200px 420px at 15% -10%, #fffaf0 0%, transparent 70%),
                radial-gradient(1000px 360px at 100% 0%, #f2fbf8 0%, transparent 65%),
                #F6F4EF;
        }
        .stApp:has(.sam-mark) .block-container {
            padding-top: 1.2rem;
            max-width: 1360px;
            padding-bottom: 4rem;
        }
        .stApp:has(.sam-mark) [data-testid="stToolbar"] {
            right: 1rem;
        }
        .stApp:has(.sam-mark) h1,
        .stApp:has(.sam-mark) h2,
        .stApp:has(.sam-mark) h3 {
            letter-spacing: -0.015em;
        }
        .stApp:has(.sam-mark) [data-testid="stVerticalBlockBorderWrapper"] {
            border-radius: 12px;
            border-color: #E4DDD2;
            box-shadow: none;
            background: #ffffff;
            overflow: visible;
        }
        .stApp:has(.sam-mark) [data-testid="stVerticalBlockBorderWrapper"]:has(.sam-title) {
            background: transparent;
            border: 0;
        }
        .stApp:has(.sam-mark) [data-testid="stMarkdownContainer"] p {
            line-height: 1.35;
        }
        .stApp:has(.sam-mark) label[data-testid="stWidgetLabel"] p {
            font-size: 0.93rem;
            font-weight: 600;
            color: #3f3a35;
        }
        .stApp:has(.sam-mark) [data-testid="stExpander"] {
            border-radius: 12px;
            border: 1px solid #E4DDD2;
            overflow: visible;
            background: #ffffff;
        }
        .stApp:has(.sam-mark) [data-testid="stExpander"] details {
            border: 0;
            border-radius: 12px;
            overflow: visible;
        }
        .stApp:has(.sam-mark) [data-testid="stExpander"] details summary {
            background: #ffffff;
            border-radius: 12px;
        }
        .stApp:has(.sam-mark) [data-testid="stFileUploader"] {
            background: transparent;
            border: 0;
            padding: 0;
        }
        .stApp:has(.sam-mark) [data-testid="stFileUploaderDropzone"] {
            min-height: 7.4rem;
            border: 2px dashed #D8D0C3;
            border-radius: 14px;
            background: #FCFBF8;
            padding: 0.8rem 1.1rem;
        }
        .stApp:has(.sam-mark) [data-testid="stFileUploaderDropzoneInstructions"] {
            visibility: hidden;
        }
        .stApp:has(.sam-mark) [data-testid="stFileUploaderDropzone"] button {
            border: 1px solid #0F6E56 !important;
            color: #0F6E56 !important;
            background: #ffffff !important;
            border-radius: 10px !important;
            min-height: 2.7rem;
            padding: 0.35rem 1.15rem !important;
            font-weight: 700;
        }
        .stApp:has(.sam-mark) .stElementContainer:has(.sam-upload-copy) {
            margin-bottom: -6.15rem;
            position: relative;
            z-index: 2;
            pointer-events: none;
        }
        .stApp:has(.sam-mark) [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #E4DDD2;
            border-radius: 12px;
            padding: 0.72rem 0.92rem;
            min-height: 104px;
            box-shadow: none;
        }
        .stApp:has(.sam-mark) [data-testid="stMetricLabel"] {
            color: #6A635A;
        }
        .stApp:has(.sam-mark) [data-testid="stMetricValue"] {
            color: #1C1917;
            font-weight: 700;
        }
        .stApp:has(.sam-mark) .stButton > button,
        .stApp:has(.sam-mark) .stDownloadButton > button {
            border-radius: 10px;
            border: 1px solid #DCD4C7;
            min-height: 2.72rem;
            font-weight: 600;
        }
        .stApp:has(.sam-mark) .stButton > button:hover,
        .stApp:has(.sam-mark) .stDownloadButton > button:hover {
            border-color: #c8bfaf;
        }
        .stApp:has(.sam-mark) .stButton > button[kind="primary"] {
            border-color: #0F6E56;
            box-shadow: none;
        }
        .stApp:has(.sam-mark) .stTextInput input,
        .stApp:has(.sam-mark) .stSelectbox div[data-baseweb="select"] > div,
        .stApp:has(.sam-mark) .stTimeInput input,
        .stApp:has(.sam-mark) .stNumberInput input {
            border-radius: 10px;
            min-height: 2.7rem;
        }
        .stApp:has(.sam-mark) .stCheckbox label {
            font-weight: 500;
            color: #3f3a35;
        }
        .stApp:has(.sam-mark) div[data-testid="stDataFrame"] {
            border: 1px solid #E4DDD2;
            border-radius: 12px;
            overflow: visible;
        }
        .sam-kicker {
            margin: 0;
            color: #0F6E56;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }
        .sam-title {
            margin: 0.15rem 0 0.2rem;
            font-size: 2.18rem;
            line-height: 1.1;
            color: #1C1917;
        }
        .sam-sub {
            margin: 0.28rem 0 0;
            color: #4c4741;
            font-size: 0.93rem;
            background: rgba(255, 255, 255, 0.78);
            border: 1px solid #E8E2D7;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            padding: 0.34rem 0.7rem;
        }
        .sam-intro {
            margin: 0.35rem 0 0;
            color: #655F57;
            font-size: 1rem;
        }
        .sam-banner {
            background: #ffffff;
            border: 1px solid #e7e2d8;
            border-left: 6px solid #0F6E56;
            border-radius: 16px;
            padding: 1rem 1.15rem 0.9rem;
            margin: 0.4rem 0 0.8rem;
        }
        .sam-banner h2 {
            margin: 0.15rem 0;
            font-size: 1.7rem;
            color: #1C1917;
        }
        .sam-banner p { margin: 0.15rem 0; color: #44403C; }
        .stApp:has(.sam-mark) [data-testid="stProgress"] [role="progressbar"] {
            height: 0.72rem;
            border-radius: 999px;
            background: #E7E2D8;
        }
        .stApp:has(.sam-mark) [data-testid="stProgress"] [role="progressbar"] > div {
            background: #0F6E56;
            border-radius: 999px;
        }
        .sam-warn {
            background: #FFF7ED;
            color: #9A3412;
            border-radius: 10px;
            padding: 0.45rem 0.7rem;
            margin-top: 0.45rem;
        }
        .sam-bay-alert {
            background: #FFF7ED;
            border: 1px solid #FDBA74;
            color: #9A3412;
            border-radius: 14px;
            padding: 0.9rem 1.05rem 1rem;
            margin: 0.15rem 0 1.15rem;
            line-height: 1.45;
        }
        .sam-bay-alert strong { color: #9A3412; }
        .sam-bay-alert p { margin: 0.35rem 0 0; }
        .sam-bay-alert ul { margin: 0.55rem 0 0; padding-left: 1.15rem; }
        .sam-bay-alert li { margin: 0.12rem 0; }
        .sam-steps {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
            margin: 1.15rem 0 1rem;
        }
        .sam-step {
            display: flex;
            align-items: flex-start;
            gap: 0.9rem;
            background: #ffffff;
            border: 1px solid #E5DFD5;
            border-radius: 15px;
            padding: 1.15rem 1.2rem;
            min-height: 7.1rem;
        }
        .sam-step-icon {
            width: 2.6rem;
            height: 2.6rem;
            border-radius: 999px;
            background: #E7F6F1;
            color: #0F6E56;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
        }
        .sam-step-icon svg { width: 1.25rem; height: 1.25rem; }
        .sam-step small {
            display: block;
            margin-bottom: 0.25rem;
            color: #0F6E56;
            font-size: 0.75rem;
            font-weight: 750;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .sam-step strong { display: block; margin-bottom: 0.3rem; font-size: 1rem; }
        .sam-step span { color: #6A635B; font-size: 0.9rem; line-height: 1.45; }
        .sam-upload-copy {
            display: flex;
            align-items: center;
            gap: 1rem;
            min-height: 5.4rem;
            padding: 0.35rem 11rem 0.2rem 1.15rem;
        }
        .sam-upload-icon {
            width: 3.3rem;
            height: 3.3rem;
            border-radius: 14px;
            background: #E7F6F1;
            color: #0F6E56;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
        }
        .sam-upload-icon svg { width: 1.7rem; height: 1.7rem; }
        .sam-upload-copy h2 {
            margin: 0 0 0.2rem;
            font-size: 1.25rem;
            color: #1C1917;
        }
        .sam-upload-copy p { margin: 0; color: #6A635B; font-size: 0.9rem; }
        .sam-requirements, .sam-next {
            display: flex;
            align-items: flex-start;
            gap: 0.55rem;
            margin-top: 0.85rem;
            font-size: 0.9rem;
            line-height: 1.5;
        }
        .sam-requirements { color: #625C54; }
        .sam-requirements svg, .sam-next svg { width: 1.05rem; height: 1.05rem; flex-shrink: 0; margin-top: 0.15rem; }
        .sam-requirements svg { color: #0F6E56; }
        .sam-next {
            align-items: center;
            margin-top: 0.75rem;
            margin-bottom: 1.1rem;
            padding: 0.8rem 1rem;
            border-radius: 12px;
            background: #EEF8F4;
            color: #315E50;
        }
        .sam-next svg { color: #174E3E; }
        .sam-next strong { color: #174E3E; }
        .sam-progress {
            display: flex;
            align-items: center;
            gap: 0.9rem;
            background: #ffffff;
            border: 1px solid #E8E2D7;
            border-radius: 14px;
            padding: 0.8rem 1.4rem;
            margin: 0.9rem 0 1rem;
        }
        .sam-progress-line {
            flex: 1;
            height: 2px;
            border-radius: 2px;
            background: #E4DDD2;
        }
        .sam-progress-line.done { background: #0F6E56; }
        .sam-progress-dot svg { width: 0.95rem; height: 0.95rem; }
        .sam-icon {
            width: 2.6rem;
            height: 2.6rem;
            border-radius: 999px;
            background: #E7F6F1;
            color: #0F6E56;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
        }
        .sam-icon svg { width: 1.25rem; height: 1.25rem; }
        .sam-stats {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.8rem;
            margin-bottom: 0.9rem;
        }
        .sam-stat {
            display: flex;
            align-items: center;
            gap: 0.85rem;
            background: #ffffff;
            border: 1px solid #E4DDD2;
            border-radius: 12px;
            padding: 0.9rem 1rem;
        }
        .sam-stat-label { margin: 0; color: #6A635A; font-size: 0.88rem; }
        .sam-stat-value { margin: 0.1rem 0 0; color: #1C1917; font-size: 1.55rem; font-weight: 700; line-height: 1.15; }
        .sam-review-head {
            display: flex;
            align-items: center;
            gap: 0.7rem;
            margin: 0.4rem 0 0.2rem;
        }
        .sam-review-head h2 { margin: 0; font-size: 1.45rem; color: #1C1917; }
        .sam-pill-warn {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.18rem 0.65rem;
            background: #FFF4E5;
            color: #9A5B00;
            border: 1px solid #F5D9A8;
            font-size: 0.8rem;
            font-weight: 700;
        }
        .stApp:has(.sam-mark) [data-testid="stColumn"]:has(.sam-downloads) {
            padding-left: 1.4rem;
        }
        .sam-count-badge {
            border-radius: 999px;
            padding: 0.12rem 0.6rem;
            background: #E7F6F1;
            color: #0F6E56;
            font-size: 0.8rem;
            font-weight: 700;
        }
        .sam-review-sub { margin: 0 0 0.7rem; color: #6A635A; font-size: 0.92rem; }
        .sam-review-card {
            background: #ffffff;
            border: 1px solid #E4DDD2;
            border-radius: 12px;
            padding: 1rem 1.1rem 0.85rem;
            margin-bottom: 0.9rem;
        }
        .sam-review-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.8rem;
            padding-bottom: 0.85rem;
            border-bottom: 1px solid #EFEAE2;
        }
        .sam-review-grid .sam-stat { border: 0; padding: 0; }
        .sam-review-foot {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            flex-wrap: wrap;
            padding-top: 0.75rem;
            color: #3F3A35;
            font-size: 0.93rem;
        }
        .sam-review-foot .sam-where { display: inline-flex; align-items: center; gap: 0.45rem; }
        .sam-review-foot .sam-where svg { width: 1.05rem; height: 1.05rem; color: #0F6E56; }
        .sam-routes { display: inline-flex; align-items: center; gap: 0.4rem; flex-wrap: wrap; }
        .sam-route {
            display: inline-flex;
            min-width: 2rem;
            justify-content: center;
            border-radius: 999px;
            padding: 0.15rem 0.6rem;
            background: #E7F6F1;
            color: #0F6E56;
            font-size: 0.82rem;
            font-weight: 700;
        }
        .sam-card-title { margin: 0 0 0.6rem; font-size: 1.05rem; color: #1C1917; }
        @media (max-width: 900px) {
            .sam-stats, .sam-review-grid { grid-template-columns: repeat(2, 1fr); }
        }
        .sam-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.75rem;
            margin-bottom: 0.35rem;
        }
        .sam-head h3 {
            margin: 0;
            font-size: 1.05rem;
            color: #1C1917;
        }
        .sam-chips {
            display: flex;
            justify-content: flex-end;
            gap: 0.4rem;
            flex-wrap: wrap;
        }
        .sam-chip {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.18rem 0.62rem;
            font-size: 0.78rem;
            font-weight: 700;
            line-height: 1.2;
            border: 1px solid transparent;
            white-space: nowrap;
        }
        .sam-chip.open {
            background: #E7F6F1;
            color: #0F6E56;
            border-color: #B7E4D6;
        }
        .sam-chip.garage {
            background: #F3F4F6;
            color: #4B5563;
            border-color: #E5E7EB;
        }
        .sam-chip.suggested {
            background: #D1FAE5;
            color: #065F46;
            border-color: #A7F3D0;
        }
        .sam-chip.assigned {
            background: #EEF2FF;
            color: #3730A3;
            border-color: #C7D2FE;
        }
        .sam-block-title {
            margin: 0;
            font-size: 1.15rem;
            color: #1C1917;
        }
        .sam-block-count {
            margin: 0.15rem 0 0;
            color: #6B6560;
            font-size: 0.92rem;
        }
        .sam-legend {
            display: flex;
            justify-content: flex-end;
            gap: 0.9rem;
            align-items: center;
            color: #3F3A35;
            font-size: 0.92rem;
            font-weight: 600;
        }
        .sam-legend span {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
        }
        .sam-dot {
            width: 0.55rem;
            height: 0.55rem;
            border-radius: 999px;
            display: inline-block;
        }
        .sam-dot.open { background: #1FA971; }
        .sam-dot.garage { background: #9CA3AF; }
        .sam-trip-wrap {
            max-height: 560px;
            overflow: auto;
            border: 1px solid #E4DDD2;
            border-radius: 12px;
        }
        .sam-trip-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.92rem;
        }
        .sam-trip-table th {
            position: sticky;
            top: 0;
            background: #F7F5F1;
            color: #6B6560;
            font-weight: 600;
            text-align: left;
            padding: 0.55rem 0.7rem;
            border-bottom: 1px solid #E4DDD2;
        }
        .sam-trip-table td {
            padding: 0.62rem 0.7rem;
            border-bottom: 1px solid #F0EBE3;
            color: #1C1917;
            vertical-align: middle;
        }
        .sam-trip-table tr.garage td { color: #6B7280; background: #FAFAFA; }
        .sam-trip-table tr.suggested td { background: #F3FBF7; }
        .sam-trip-table tr.assigned td { background: #F7F8FF; }
        .sam-status {
            display: inline-flex;
            border-radius: 999px;
            padding: 0.12rem 0.55rem;
            font-size: 0.78rem;
            font-weight: 700;
            border: 1px solid transparent;
        }
        .sam-status.open { background: #E7F8F1; color: #128A62; border-color: #B7E4D6; }
        .sam-status.garage { background: #F3F4F6; color: #6B7280; border-color: #E5E7EB; }
        .sam-status.suggested { background: #D1FAE5; color: #065F46; border-color: #A7F3D0; }
        .sam-status.assigned { background: #EEF2FF; color: #3730A3; border-color: #C7D2FE; }
        .sam-progress-item {
            display: flex;
            align-items: center;
            gap: 0.6rem;
            color: #7A746B;
            font-size: 0.9rem;
            font-weight: 600;
            white-space: nowrap;
        }
        .sam-progress-dot {
            width: 1.55rem;
            height: 1.55rem;
            border-radius: 999px;
            border: 1px solid #D5CDC0;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 0.8rem;
            background: #F8F6F1;
            color: #7A746B;
            flex-shrink: 0;
        }
        .sam-progress-item.active {
            color: #1C1917;
        }
        .sam-progress-item.done .sam-progress-dot {
            border-color: #0F6E56;
            background: #E6F6F1;
            color: #0F6E56;
        }
        .sam-progress-item.active .sam-progress-dot {
            border-color: #0F6E56;
            background: #0F6E56;
            color: #ffffff;
        }
        .sam-mark { display: none; }
        @media (max-width: 800px) {
            .sam-steps { grid-template-columns: 1fr; }
            .sam-progress { flex-wrap: wrap; }
            .sam-progress-line { display: none; }
            .sam-upload-copy { padding-right: 1.15rem; }
        }
        </style>
        <div class="sam-mark"></div>
        """,
        unsafe_allow_html=True,
    )


def _portal_bar() -> None:
    """Keep the OD login session: leave this tool without losing the rest of the dashboard."""
    from authentication.auth import PORTAL_OD, allowed_portals, logout

    user = st.session_state.get("user") or {}
    email = str(user.get("email", ""))
    role = str(user.get("role", ""))
    portals = allowed_portals(email, role)
    show_switch = len(portals) > 1
    show_od = PORTAL_OD in portals and not show_switch
    action_count = int(show_switch) + int(show_od) + 1
    columns = st.columns([6.4] + [1.4] * action_count, vertical_alignment="center")
    slot = 1
    if show_switch:
        with columns[slot]:
            if st.button("Switch Portal", key="sam_switch_portal", use_container_width=True):
                st.query_params["page"] = "portal_select"
                st.rerun()
        slot += 1
    if show_od:
        with columns[slot]:
            if st.button("OD Dashboard", key="sam_od_dashboard", use_container_width=True):
                st.query_params["page"] = "od_project_select"
                st.rerun()
        slot += 1
    with columns[slot]:
        if st.button("Logout", key="sam_logout", use_container_width=True):
            logout()


def _header(subtitle: str, *, plain: bool = False) -> None:
    subtitle_class = "sam-intro" if plain else "sam-sub"
    st.markdown(
        f"""
        <p class="sam-kicker">Field work</p>
        <h1 class="sam-title">{html.escape(APP_NAME)}</h1>
        <p class="{subtitle_class}">{html.escape(subtitle)}</p>
        """,
        unsafe_allow_html=True,
    )


_ICON_PATHS = {
    "check": '<path d="M20 6 9 17l-5-5"/>',
    "bus": '<path d="M8 6v6"/><path d="M15 6v6"/><path d="M2 12h19.6"/><path d="M18 18h3s.5-1.7.8-2.8c.1-.4.2-.8.2-1.2 0-.4-.1-.8-.2-1.2l-1.4-5C20.1 6.8 19.1 6 18 6H4a2 2 0 0 0-2 2v10h3"/><circle cx="7" cy="18" r="2"/><path d="M9 18h5"/><circle cx="16" cy="18" r="2"/>',
    "file": '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M10 13H8"/><path d="M16 17H8"/><path d="M16 13h-2"/>',
    "users": '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
    "hash": '<line x1="4" x2="20" y1="9" y2="9"/><line x1="4" x2="20" y1="15" y2="15"/><line x1="10" x2="8" y1="3" y2="21"/><line x1="16" x2="14" y1="3" y2="21"/>',
    "clock": '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
    "calendar": '<rect width="18" height="18" x="3" y="4" rx="2"/><path d="M16 2v4"/><path d="M8 2v4"/><path d="M3 10h18"/>',
    "layers": '<path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/><path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65"/><path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65"/>',
    "pin": '<path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z"/><circle cx="12" cy="10" r="3"/>',
    "upload": '<path d="M12 16V4"/><path d="m7 9 5-5 5 5"/><path d="M20 15v4a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2v-4"/>',
    "eye": '<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z"/><circle cx="12" cy="12" r="3"/>',
    "download": '<path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/>',
    "cloud": '<path d="M12 13v8"/><path d="M4 14.899A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.242"/><path d="m8 17 4-4 4 4"/>',
    "info": '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    "chevron": '<path d="m9 18 6-6-6-6"/>',
}


def _icon(name: str) -> str:
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
        f'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">{_ICON_PATHS[name]}</svg>'
    )


def _stat(icon: str, label: str, value: str) -> str:
    return (
        f'<div class="sam-stat"><span class="sam-icon">{_icon(icon)}</span><div>'
        f'<p class="sam-stat-label">{html.escape(label)}</p>'
        f'<p class="sam-stat-value">{html.escape(value)}</p></div></div>'
    )


def _workflow_progress(active_step: int) -> None:
    steps = ["Upload file", "Preview assignments", "Save & download"]
    chunks: list[str] = []
    for index, label in enumerate(steps, start=1):
        state = " active" if index == active_step else (" done" if index < active_step else "")
        dot = _icon("check") if index < active_step else str(index)
        if index > 1:
            line = " done" if index <= active_step else ""
            chunks.append(f'<div class="sam-progress-line{line}"></div>')
        chunks.append(
            f'<div class="sam-progress-item{state}"><span class="sam-progress-dot">{dot}</span><span>{html.escape(label)}</span></div>'
        )
    st.markdown(f'<div class="sam-progress">{"".join(chunks)}</div>', unsafe_allow_html=True)


def _summary(frame: pd.DataFrame, notes: dict) -> pd.DataFrame:
    records = []
    for label in assignment_labels(frame):
        matched = frame.loc[frame["Asn#"].map(clean_token).eq(label)]
        ids = [int(value) for value in matched["_id"].tolist()]
        ids = sorted(ids, key=lambda row_id: time_to_minutes(frame.loc[frame["_id"].eq(row_id), "Start Time"].iloc[0]) or 0)
        first = frame.loc[frame["_id"].eq(ids[0])].iloc[0]
        last = frame.loc[frame["_id"].eq(ids[-1])].iloc[0]
        blocks = list(dict.fromkeys(clean_token(frame.loc[frame["_id"].eq(row_id), "Block"].iloc[0]) for row_id in ids))
        records.append(
            {
                "Assignment": label,
                "Trips": len(ids),
                "Blocks": ", ".join(blocks),
                "From": format_time(first["Start Time"]),
                "To": format_time(last["End Time"]),
                "Hours": format_hours(duration_hours(first["Start Time"], last["End Time"])),
                "Notes": len(notes.get(label, [])),
                "_start": time_to_minutes(first["Start Time"]),
                "_hours": duration_hours(first["Start Time"], last["End Time"]),
            }
        )
    return pd.DataFrame(records)


_SUMMARY_COLUMNS = ["Assignment", "Trips", "Blocks", "From", "To", "Hours", "Notes"]
_WHEN_CHOICES = [
    "All",
    "Morning · before 12 PM",
    "Afternoon · 12 to 5 PM",
    "Evening · after 5 PM",
]


def _assignment_view(summary: pd.DataFrame) -> pd.DataFrame:
    if st.session_state.get("assign_when") not in _WHEN_CHOICES:
        st.session_state["assign_when"] = "All"
    _seed("assign_when", "All")
    _seed("assign_find", "")
    _seed("assign_length", "Any length")
    _seed("assign_notes", "All notes")
    token = "|".join(
        str(st.session_state.get(key) or "")
        for key in ("assign_when", "assign_find", "assign_length", "assign_notes")
    )
    if st.session_state.get("_assign_filter") != token:
        st.session_state["_assign_filter"] = token
        st.session_state.pop("made_table", None)
    if summary.empty:
        return summary
    view = summary
    when = st.session_state.get("assign_when") or "All"
    if when != "All":
        start = view["_start"]
        if when.startswith("Morning"):
            view = view.loc[start.lt(12 * 60)]
        elif when.startswith("Afternoon"):
            view = view.loc[start.ge(12 * 60) & start.lt(17 * 60)]
        elif when.startswith("Evening"):
            view = view.loc[start.ge(17 * 60)]
    query = clean_token(st.session_state.get("assign_find")).casefold()
    if query:
        assignment = view["Assignment"].map(lambda value: clean_token(value).casefold())
        blocks = view["Blocks"].map(lambda value: clean_token(value).casefold())
        view = view.loc[assignment.str.contains(query, regex=False) | blocks.str.contains(query, regex=False)]
    length = st.session_state.get("assign_length") or "Any length"
    if length != "Any length":
        hours = view["_hours"]
        if length == "Under 7 hours":
            view = view.loc[hours.lt(7)]
        elif length == "7 to 9 hours":
            view = view.loc[hours.ge(7) & hours.le(9)]
        elif length == "Over 9 hours":
            view = view.loc[hours.gt(9)]
    notes = st.session_state.get("assign_notes") or "All notes"
    if notes == "Has notes":
        view = view.loc[view["Notes"].gt(0)]
    elif notes == "No notes":
        view = view.loc[view["Notes"].eq(0)]
    return view.reset_index(drop=True)


def _block_table(frame: pd.DataFrame, block: str, suggested: set[int]) -> pd.DataFrame:
    matched = frame.loc[frame["Block"].map(clean_token).eq(clean_token(block))].copy()
    matched["_sort"] = matched["Start Time"].map(lambda value: time_to_minutes(value) if time_to_minutes(value) is not None else 99999)
    matched = matched.sort_values(["_sort", "_id"])
    rows = []
    for _, row in matched.iterrows():
        row_id = int(row["_id"])
        if row_id in suggested:
            status = SUGGESTED
        elif clean_token(row["Asn#"]):
            status = ASSIGNED
        elif is_garage(row["Type"]):
            status = GARAGE
        elif time_to_minutes(row["Start Time"]) is None:
            status = "No time"
        else:
            status = OPEN
        rows.append(
            {
                "Start": format_time(row["Start Time"]),
                "End": format_time(row["End Time"]),
                "Route": clean_token(row["Route"]),
                "Dir": clean_token(row["Direction"]),
                "From": clean_token(row["Start Location"]),
                "To": clean_token(row["End Location"]),
                "Type": clean_token(row["Type"]),
                "Assignment": clean_token(row["Asn#"]),
                "Status": status,
            }
        )
    return pd.DataFrame(rows)


def _trip_table_html(table: pd.DataFrame) -> str:
    status_class = {
        OPEN: "open",
        GARAGE: "garage",
        SUGGESTED: "suggested",
        ASSIGNED: "assigned",
    }
    headers = ["Start", "End", "Route", "Dir", "From", "To", "Status"]
    head = "".join(f"<th>{html.escape(name)}</th>" for name in headers)
    body: list[str] = []
    for _, row in table.iterrows():
        status = str(row["Status"])
        css = status_class.get(status, "open")
        cells = []
        for name in headers[:-1]:
            cells.append(f"<td>{html.escape(str(row[name]))}</td>")
        cells.append(
            f'<td><span class="sam-status {css}">{html.escape(status)}</span></td>'
        )
        body.append(f'<tr class="{css}">{"".join(cells)}</tr>')
    return (
        '<div class="sam-trip-wrap"><table class="sam-trip-table">'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div>"
    )


def _paint(table: pd.DataFrame):
    styles = {
        SUGGESTED: "background-color: #D1FAE5; color: #065F46; font-weight: 700",
        ASSIGNED: "background-color: #EEF2FF; color: #3730A3; font-weight: 700",
        GARAGE: "background-color: #F3F4F6; color: #4B5563; font-weight: 700",
        OPEN: "background-color: #E7F6F1; color: #0F6E56; font-weight: 700",
    }

    def paint_status(value: object) -> str:
        return styles.get(str(value), "")

    if table.empty or "Status" not in table.columns:
        return table
    return table.style.map(paint_status, subset=["Status"])


def _route_path(frame: pd.DataFrame, row_ids: list[int]) -> str:
    routes: list[str] = []
    for row_id in row_ids:
        route = clean_token(frame.loc[frame["_id"].eq(row_id), "Route"].iloc[0])
        if route and (not routes or routes[-1] != route):
            routes.append(route)
    return ", ".join(routes)


def _make_cut(
    frame: pd.DataFrame,
    *,
    block: str,
    hub: str,
    start_after: int | None,
    end_by: int | None,
    min_hours: float,
    max_hours: float,
    include_garage: bool,
    mode: str,
    connect: tuple[str, int] | None,
) -> tuple[dict | None, str | None]:
    ordered = eligible_ids(frame, block, include_garage)
    if not ordered:
        return None, "This block has no open trips left."
    if connect is not None:
        place, after_minutes = connect
        start_id = find_connect_id(
            frame,
            ordered,
            place,
            after_minutes,
            int(st.session_state.get("min_switch", MIN_SWITCH_MINUTES)),
            int(st.session_state.get("max_switch", MAX_SWITCH_MINUTES)),
        )
        if start_id is None:
            start_id = find_start_id(frame, ordered, place, after_minutes)
        if start_id is None:
            return None, f"No open trip on block {block} leaves {place} after {format_time(_minutes_to_dummy(after_minutes))}."
    else:
        start_id = find_start_id(frame, ordered, hub, start_after)
        if start_id is None:
            where = "the first open trip" if hub == ANY_PLACE else hub
            return None, f"No open trip on block {block} starts at {where}."
    if mode == "round":
        row_ids = cut_round_trip(frame, ordered, start_id, hub if connect is None else connect[0], max_hours, end_by)
    else:
        row_ids = cut_full_shift(frame, ordered, start_id, hub, min_hours, max_hours, end_by)
    if not row_ids:
        return None, "Nothing in this block fits the end time."
    return {
        "row_ids": row_ids,
        "eligible_ids": ordered,
        "block": clean_token(block),
        "mode": mode,
        "include_garage": include_garage,
        "hub": hub if connect is None else connect[0],
    }, None


def _minutes_to_dummy(minutes: int):
    hour, minute = divmod(int(minutes) % (24 * 60), 60)
    return pd.Timestamp(2000, 1, 1, hour, minute)


def _suggestion_is_current(suggestion: dict | None, block: str, include_garage: bool) -> bool:
    if not suggestion:
        return False
    return clean_token(suggestion.get("block")) == clean_token(block) and bool(suggestion.get("include_garage")) == bool(include_garage)


def _accept(frame: pd.DataFrame, suggestion: dict, label: str, note: str) -> None:
    row_ids = [int(value) for value in suggestion["row_ids"]]
    current = frame.loc[frame["_id"].isin(row_ids), ["_id", "Asn#"]]
    if current["Asn#"].map(clean_token).ne("").any():
        raise ValueError("One of these trips was already assigned. Suggest the shift again.")
    previous = {int(row["_id"]): clean_token(row["Asn#"]) for _, row in current.iterrows()}
    frame.loc[frame["_id"].isin(row_ids), "Asn#"] = label
    st.session_state["frame"] = frame
    added_note = False
    text = note.strip()
    if text:
        st.session_state["notes"].setdefault(label, []).append({"after_row_id": row_ids[-1], "text": text})
        added_note = True
    st.session_state["undo"] = {"ids": row_ids, "old": previous, "label": label, "note": added_note}
    st.session_state["suggestion"] = None
    st.session_state["revision"] = int(st.session_state.get("revision", 0)) + 1
    st.session_state["packet_cache"] = {}
    hours = span_hours(frame, row_ids)
    st.session_state["flash"] = (
        f"Saved {label}: {len(row_ids)} trips, {format_hours(hours)}."
    )
    st.session_state["_clear_note"] = True


def _accept_batch(frame: pd.DataFrame, plans: list[dict]) -> None:
    number = next_assignment_number(frame)
    previous: dict[int, str] = {}
    labels: list[str] = []
    for plan in plans:
        row_ids = [int(value) for value in plan["row_ids"]]
        current = frame.loc[frame["_id"].isin(row_ids), "Asn#"]
        if current.map(clean_token).ne("").any():
            raise ValueError("Some of these trips were already assigned. Generate the batch again.")
        label = str(number)
        number += 1
        labels.append(label)
        for row_id in row_ids:
            previous[row_id] = ""
        frame.loc[frame["_id"].isin(row_ids), "Asn#"] = label
    st.session_state["frame"] = frame
    st.session_state["undo"] = {"ids": list(previous), "old": previous, "label": "the batch", "note": False}
    st.session_state["suggestion"] = None
    st.session_state["batch_plan"] = None
    st.session_state["revision"] = int(st.session_state.get("revision", 0)) + 1
    st.session_state["packet_cache"] = {}
    first, last = labels[0], labels[-1]
    shown = first if first == last else f"{first}–{last}"
    st.session_state["flash"] = f"Saved {len(labels)} assignments ({shown})."
    st.session_state["_clear_note"] = True


def _undo(frame: pd.DataFrame) -> None:
    undo = st.session_state.get("undo")
    if not undo:
        return
    for row_id, previous in undo["old"].items():
        frame.loc[frame["_id"].eq(int(row_id)), "Asn#"] = previous
    if undo.get("note"):
        notes = st.session_state["notes"].get(undo["label"], [])
        if notes:
            notes.pop()
    st.session_state["frame"] = frame
    st.session_state["undo"] = None
    st.session_state["revision"] = int(st.session_state.get("revision", 0)) + 1
    st.session_state["packet_cache"] = {}
    st.session_state["flash"] = f"Undid {undo['label']}."


def _unassign(frame: pd.DataFrame, label: str) -> None:
    label = clean_token(label)
    if not label:
        return
    mask = frame["Asn#"].map(clean_token).eq(label)
    count = int(mask.sum())
    if count == 0:
        return
    frame.loc[mask, "Asn#"] = ""
    st.session_state["frame"] = frame
    notes = st.session_state.setdefault("notes", {})
    notes.pop(label, None)
    undo = st.session_state.get("undo") or {}
    if clean_token(undo.get("label")) == label:
        st.session_state["undo"] = None
    st.session_state["suggestion"] = None
    st.session_state["revision"] = int(st.session_state.get("revision", 0)) + 1
    st.session_state["packet_cache"] = {}
    st.session_state["_clear_selection"] = True
    st.session_state["_clear_builder_name"] = True
    st.session_state["flash"] = f"Unassigned {label}. {count} trips are open again."


def _bay_alert(hubs: list[dict[str, object]]) -> None:
    if not hubs:
        return
    shown = hubs[:4]
    example = shown[0]
    example_name = html.escape(str(example["names"][0]))
    station = html.escape(str(example["station"]))
    items: list[str] = []
    for hub in shown:
        numbers = ", ".join(str(number) for number in hub["bays"])
        items.append(
            f"<li><strong>{html.escape(str(hub['station']))}</strong> · bays {html.escape(numbers)}</li>"
        )
    extra = len(hubs) - len(shown)
    more = f"<p>And {extra} more {'station' if extra == 1 else 'stations'}.</p>" if extra else ""
    st.markdown(
        f"""
        <div class="sam-bay-alert">
            <strong>These bays may need to be one hub</strong>
            <p>This file uses a separate name for each bay. A shift only starts and ends at the exact name you pick, so one bay does not include the others at that station.</p>
            <p>If they are the same station, edit the file before uploading and give them one name. For example, change <strong>{example_name}</strong> to <strong>{station}</strong>.</p>
            <ul>{"".join(items)}</ul>
            {more}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _upload_screen() -> None:
    _header(
        "Build clear survey shifts from a Raw Weekday workbook, review every trip, then save and download.",
        plain=True,
    )
    _workflow_progress(1)
    st.markdown(
        f"""
        <div class="sam-steps">
            <div class="sam-step"><span class="sam-step-icon">{_icon("upload")}</span><div><small>Step 1</small><strong>Upload Raw Weekday</strong><span>Choose the Excel workbook used for the survey schedule.</span></div></div>
            <div class="sam-step"><span class="sam-step-icon">{_icon("eye")}</span><div><small>Step 2</small><strong>Preview the assignment</strong><span>Check the shift, trip itinerary, route and report location.</span></div></div>
            <div class="sam-step"><span class="sam-step-icon">{_icon("download")}</span><div><small>Step 3</small><strong>Save &amp; download</strong><span>Save the assignment and download the workbook or Word packets.</span></div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.container(border=True):
        st.markdown(
            f"""
            <div class="sam-upload-copy">
                <span class="sam-upload-icon">{_icon("cloud")}</span>
                <div>
                    <h2>Upload Raw Weekday file</h2>
                    <p>Drag and drop your Excel workbook here · XLSX or XLSM · Up to 50 MB</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        uploaded = st.file_uploader(
            "Weekday workbook",
            type=["xlsx", "xlsm"],
            key=f"uploader_{st.session_state.get('uploader_key', 0)}",
            label_visibility="collapsed",
        )
        if uploaded is not None:
            token = (uploaded.name, uploaded.size)
            if token != st.session_state.get("upload_token"):
                bar = st.progress(6, text="Reading the file…")
                data = uploaded.getvalue()

                def _tick(done: int, total: int, name: str) -> None:
                    bar.progress(12 + int(56 * done / max(total, 1)), text=f"Checking {name}…")

                try:
                    options = inspect_workbook(data, on_sheet=_tick)
                except Exception as exc:
                    bar.empty()
                    st.error(f"Could not read that workbook. {exc}")
                    return
                st.session_state["upload_bytes"] = data
                st.session_state["upload_name"] = uploaded.name
                st.session_state["upload_token"] = token
                st.session_state["sheet_options"] = options
                if not options:
                    bar.empty()
                    st.error(
                        "No usable sheet was found. Use the sample headers: Block, Route, Direction, "
                        "Start Time, Start Location, End Time, and End Location. Capital letters do not matter. "
                        "Asn# is added automatically when that column is missing."
                    )
                    return
                choice = auto_choice(options)
                if choice is not None:
                    bar.progress(78, text=f"Building {choice['name']}…")
                    try:
                        _load_choice(choice)
                    except Exception as exc:
                        bar.empty()
                        st.error(str(exc))
                        return
                    bar.progress(100, text="Opening assignments…")
                    st.rerun()
                bar.empty()
        st.markdown(
            f"""
            <div class="sam-requirements">{_icon("info")}<span>Headers can be any mix of capital and small letters. If <strong>Asn#</strong> is missing, a blank one is added. Extra columns, such as garage notes, can stay in the file.</span></div>
            <div class="sam-next">{_icon("chevron")}<span><strong>What happens next:</strong> We find the correct sheet, build the working copy and open the assignment workspace.</span></div>
            """,
            unsafe_allow_html=True,
        )
        with st.expander("View sample file"):
            st.caption("Match these headers. The sample can be downloaded and filled in.")
            st.dataframe(sample_frame(), use_container_width=True, hide_index=True)
            st.download_button(
                "Download sample file",
                data=sample_workbook_bytes(),
                file_name="Weekday_sample.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                icon=":material/download:",
                use_container_width=True,
            )

    options = st.session_state.get("sheet_options") or []
    if options and st.session_state.get("frame") is None:
        st.subheader("Which sheet should be the working copy?")
        st.caption("Raw Weekday starts blank. A Weekday sheet keeps assignment names that are already filled in.")
        for option in options:
            if option["kind"] == "raw":
                detail = f"{option['name']} · {int(option['rows']):,} rows · start with blank assignments"
            else:
                detail = f"{option['name']} · {int(option['rows']):,} rows · {int(option['assigned']):,} already assigned"
            if st.button(detail, key=f"pick_{option['name']}", use_container_width=True):
                bar = st.progress(20, text=f"Building {option['name']}…")
                try:
                    _load_choice(option)
                except Exception as exc:
                    bar.empty()
                    st.error(str(exc))
                    return
                bar.progress(100, text="Opening assignments…")
                st.rerun()


def _workspace() -> None:
    frame: pd.DataFrame = st.session_state["frame"]
    notes: dict = st.session_state.setdefault("notes", {})
    if st.session_state.pop("_clear_note", False):
        st.session_state["cut_note"] = ""
    if st.session_state.pop("_clear_selection", False):
        st.session_state.pop("made_table", None)
    if st.session_state.pop("_clear_builder_name", False):
        st.session_state["builder_name"] = ""
        st.session_state.pop("_pick_token", None)
    flash = st.session_state.pop("flash", None)

    summary = _summary(frame, notes)
    visible = _assignment_view(summary)
    picked = _selected_rows(st.session_state.get("made_table"))
    if picked and not visible.empty and picked[0] < len(visible):
        label = str(visible.iloc[picked[0]]["Assignment"])
        token = f"{label}:{picked[0]}"
        if token != st.session_state.get("_pick_token"):
            st.session_state["builder_name"] = label
            st.session_state["_pick_token"] = token
    _seed("builder_name", "")

    kind = "Raw Weekday" if st.session_state.get("source_kind") == "raw" else "Weekday"
    title_col, button_col = st.columns([5, 1], vertical_alignment="center")
    with title_col:
        _header(f"{st.session_state.get('upload_name', 'Workbook')} · {kind} sheet “{st.session_state.get('source_sheet', '')}”")
    with button_col:
        if st.button("New file", icon=":material/upload_file:", use_container_width=True):
            _clear_work()
            st.rerun()
    _workflow_progress(3 if not summary.empty else 2)
    if flash:
        st.success(flash)

    open_count = open_regular_count(frame)
    assigned_labels = assignment_labels(frame)
    st.markdown(
        '<div class="sam-stats">'
        + _stat("bus", "Trips", f"{len(frame):,}")
        + _stat("file", "Open regular", f"{open_count:,}")
        + _stat("users", "Assignments", f"{len(assigned_labels):,}")
        + _stat("hash", "Next number", str(next_assignment_number(frame)))
        + "</div>",
        unsafe_allow_html=True,
    )

    other_sheets = [
        option
        for option in (st.session_state.get("sheet_options") or [])
        if option.get("name") != st.session_state.get("source_sheet")
    ]
    if other_sheets:
        with st.expander("Other sheets in this workbook"):
            st.caption("Raw Weekday is the fresh start. Open Weekday only when you want to keep numbers already saved there.")
            for option in other_sheets:
                if option["kind"] == "raw":
                    detail = f"Use {option['name']} · blank assignments"
                else:
                    detail = f"Use {option['name']} · {int(option['assigned']):,} already assigned"
                if st.button(detail, key=f"switch_{option['name']}"):
                    _load_choice(option)
                    st.rerun()

    include_ready = has_type_column(frame)
    locations = frame["Start Location"].map(clean_token)
    location_choices = [ANY_PLACE] + [value for value in locations.value_counts().index.tolist() if value]
    _seed("builder_hub", default_hub(frame, DEFAULT_HUB) or (location_choices[1] if len(location_choices) > 1 else ANY_PLACE))
    _seed("include_garage", False)
    _seed("min_hours", float(DEFAULT_MIN_HOURS))
    _seed("max_hours", float(DEFAULT_MAX_HOURS))
    _seed("min_switch", int(MIN_SWITCH_MINUTES))
    _seed("max_switch", int(MAX_SWITCH_MINUTES))
    _seed("use_start_after", False)
    _seed("use_end_by", False)
    _seed("choose_any_block", False)

    name_now = clean_token(st.session_state.get("builder_name"))
    continuing = label_exists(frame, name_now)
    last = last_assigned_row(frame, name_now) if continuing else None
    include_garage = bool(st.session_state.get("include_garage"))
    bay_hubs = split_bay_hubs(pd.concat([frame["Start Location"], frame["End Location"]], ignore_index=True))
    _bay_alert(bay_hubs)

    left, right = st.columns([0.86, 1.14], gap="large")
    with left:
        st.pills("How to assign", ["One shift", "Many shifts"], key="work_mode", default="One shift")
        many = st.session_state.get("work_mode") == "Many shifts"
        if many:
            with st.container(border=True):
                st.markdown("**Generate many shifts**")
                st.caption("Creates every shift that starts and ends at the same place, within the hours you set. Each shift stays on one block. A single route only uses trips of that route that run one after another.")
                place_choices = [value for value in location_choices if value != ANY_PLACE]
                if place_choices and st.session_state.get("builder_hub") not in place_choices:
                    st.session_state["builder_hub"] = place_choices[0]
                st.selectbox("Start and end at", place_choices or location_choices, key="builder_hub")
                chosen_hub = str(st.session_state.get("builder_hub") or "")
                for bay_hub in bay_hubs:
                    if chosen_hub in bay_hub["names"]:
                        bay_numbers = ", ".join(str(number) for number in bay_hub["bays"])
                        st.caption(
                            f"Only this bay is included. {bay_hub['station']} also has bays {bay_numbers}. "
                            "Use one name in the file if they should be one hub."
                        )
                        break
                routes = sorted(
                    {clean_token(value) for value in frame["Route"].tolist() if clean_token(value)},
                    key=lambda value: (0, int(value)) if value.isdigit() else (1, value.casefold()),
                )
                route_options = [ALL_ROUTES, *routes]
                if st.session_state.get("batch_route") not in route_options:
                    st.session_state["batch_route"] = ALL_ROUTES
                st.selectbox("Routes", route_options, key="batch_route")
                hour_left, hour_right = st.columns(2)
                hour_left.number_input("No shorter than (hours)", min_value=0.0, max_value=24.0, step=0.5, key="min_hours")
                hour_right.number_input("No longer than (hours)", min_value=0.0, max_value=24.0, step=0.5, key="max_hours")
                if float(st.session_state.get("min_hours") or 0) > float(st.session_state.get("max_hours") or 0) > 0:
                    st.warning("The shortest length is greater than the longest length.")
                st.checkbox("Start after a clock time", key="use_start_after")
                if st.session_state.get("use_start_after"):
                    _seed("start_after_time", pd.Timestamp("05:00").time())
                    st.time_input("Start after", key="start_after_time", step=300)
                st.checkbox("End by a clock time", key="use_end_by")
                if st.session_state.get("use_end_by"):
                    _seed("end_by_time", pd.Timestamp("14:30").time())
                    st.time_input("End by", key="end_by_time", step=300)
                if st.button("Generate assignments", type="primary", icon=":material/auto_awesome:", use_container_width=True):
                    batch_start = clock_input_to_minutes(st.session_state.get("start_after_time")) if st.session_state.get("use_start_after") else None
                    batch_end = clock_input_to_minutes(st.session_state.get("end_by_time")) if st.session_state.get("use_end_by") else None
                    batch_hub = str(st.session_state.get("builder_hub") or "")
                    batch_route = str(st.session_state.get("batch_route") or ALL_ROUTES)
                    batch_min = float(st.session_state.get("min_hours") or 0)
                    batch_max = float(st.session_state.get("max_hours") or 0)
                    built = plan_batch(
                        frame,
                        batch_hub,
                        batch_route,
                        batch_min,
                        batch_max,
                        include_garage,
                        batch_start,
                        batch_end,
                    )
                    st.session_state["batch_plan"] = built
                    st.session_state["batch_empty_reason"] = (
                        ""
                        if built
                        else explain_empty_batch(
                            frame,
                            batch_hub,
                            batch_route,
                            batch_min,
                            batch_max,
                            include_garage,
                            batch_start,
                            batch_end,
                        )
                    )
                    st.rerun()
                empty_reason = st.session_state.get("batch_empty_reason") or ""
                if empty_reason and not st.session_state.get("batch_plan"):
                    st.warning(empty_reason)
        else:
            with st.container(border=True):
                st.markdown("**Cut a shift**")
                st.text_input(
                    "Assignment name",
                    key="builder_name",
                    placeholder="Blank uses the next number",
                    help="Type a name such as Aisha 1 to keep adding pieces to the same person. Leave it blank for 1, 2, 3.",
                )
                name_now = clean_token(st.session_state.get("builder_name"))
                continuing = label_exists(frame, name_now)
                last = last_assigned_row(frame, name_now) if continuing else None
                if include_ready:
                    st.checkbox(
                        "Include garage moves",
                        key="include_garage",
                        help="Pull-out, pull-in, and deadhead. Leave this off for a normal shift. Turn it on for express.",
                    )
                include_garage = bool(st.session_state.get("include_garage")) if include_ready else True
                connect_options: list[dict[str, object]] = []
                if continuing and last is not None:
                    end_minutes = time_to_minutes(last["End Time"]) or 0
                    st.caption(
                        f"{name_now} is at {clean_token(last['End Location'])} at {format_time(last['End Time'])}."
                    )
                    st.checkbox("Choose any block", key="choose_any_block")
                    if not st.session_state.get("choose_any_block"):
                        connect_options = connecting_blocks(
                            frame,
                            clean_token(last["End Location"]),
                            end_minutes,
                            include_garage,
                            int(st.session_state["min_switch"]),
                            int(st.session_state["max_switch"]),
                        )
                else:
                    st.selectbox("Start at", location_choices, key="builder_hub")

                blocks = sorted(
                    {clean_token(value) for value in frame["Block"].tolist() if clean_token(value)},
                    key=block_sort_key,
                )
                connect_map = {str(item["block"]): item for item in connect_options}
                if continuing and last is not None and not st.session_state.get("choose_any_block"):
                    block_options = [str(item["block"]) for item in connect_options]
                else:
                    block_options = blocks
                if block_options and st.session_state.get("builder_block") not in block_options:
                    if st.session_state.get("builder_block") is None and not continuing:
                        st.session_state["builder_block"] = first_open_block(
                            frame,
                            block_options,
                            include_garage,
                            str(st.session_state.get("builder_hub") or ANY_PLACE),
                            float(st.session_state.get("min_hours") or 0),
                            float(st.session_state.get("max_hours") or 0),
                        )
                    else:
                        st.session_state["builder_block"] = block_options[0]

                open_by_block = open_counts(frame, include_garage)

                def block_label(block: str) -> str:
                    item = connect_map.get(block)
                    if item is not None:
                        return (
                            f"{block}   ·   {format_time(item['start'])}   ·   route {item['route']}"
                            f"   ·   {int(item['gap'])} min"
                        )
                    return f"{block}   ·   {open_by_block.get(block, 0)} open"

                if not block_options:
                    st.info("No open trip leaves that place inside the switch window. Widen it under More options, or choose any block.")
                    selected_block = ""
                else:
                    selected_block = st.selectbox("Block", block_options, key="builder_block", format_func=block_label)

                with st.expander("More options"):
                    hour_left, hour_right = st.columns(2)
                    min_hours = hour_left.number_input("No shorter than (hours)", min_value=0.0, max_value=24.0, step=0.5, key="min_hours")
                    max_hours = hour_right.number_input("No longer than (hours)", min_value=0.0, max_value=24.0, step=0.5, key="max_hours")
                    if not continuing:
                        st.checkbox("Start after a clock time", key="use_start_after")
                        if st.session_state.get("use_start_after"):
                            _seed("start_after_time", pd.Timestamp("05:00").time())
                            st.time_input("Start after", key="start_after_time", step=300)
                    st.checkbox("End by a clock time", key="use_end_by")
                    if st.session_state.get("use_end_by"):
                        _seed("end_by_time", pd.Timestamp("14:30").time())
                        st.time_input("End by", key="end_by_time", step=300)
                    gap_left, gap_right = st.columns(2)
                    gap_left.number_input("Minutes to change buses, at least", min_value=0, max_value=240, step=5, key="min_switch")
                    gap_right.number_input("Minutes to change buses, at most", min_value=0, max_value=240, step=5, key="max_switch")

                min_hours = float(st.session_state["min_hours"])
                max_hours = float(st.session_state["max_hours"])
                if min_hours > 0 and max_hours > 0 and min_hours > max_hours:
                    st.warning("The shortest length is greater than the longest length.")
                start_after = clock_input_to_minutes(st.session_state.get("start_after_time")) if st.session_state.get("use_start_after") and not continuing else None
                end_by = clock_input_to_minutes(st.session_state.get("end_by_time")) if st.session_state.get("use_end_by") else None
                hub = st.session_state.get("builder_hub") or ANY_PLACE

                if continuing and last is not None:
                    rule = (
                        f"Next piece of {name_now}: a round trip that leaves "
                        f"{clean_token(last['End Location'])} after {format_time(last['End Time'])}."
                    )
                else:
                    where = "any place" if hub == ANY_PLACE else str(hub)
                    rule = (
                        f"Full shift on block {selected_block or '—'}: open trips starting at {where}, "
                        f"about {min_hours:g}–{max_hours:g} hours, ending back there."
                    )
                st.caption(rule)

                connect = None
                if continuing and last is not None:
                    connect = (clean_token(last["End Location"]), time_to_minutes(last["End Time"]) or 0)

                suggest_full = False
                suggest_round = False
                suggest_next = False
                if continuing:
                    suggest_next = st.button(
                        "Suggest next piece",
                        type="primary",
                        use_container_width=True,
                        disabled=not selected_block,
                    )
                else:
                    suggest_full = st.button("Suggest full shift", type="primary", use_container_width=True, disabled=not selected_block)
                    suggest_round = st.button("Suggest round trip", use_container_width=True, disabled=not selected_block)

                if selected_block and (suggest_full or suggest_round or suggest_next):
                    mode = "round" if suggest_round or suggest_next else "full"
                    built, error = _make_cut(
                        frame,
                        block=selected_block,
                        hub=hub,
                        start_after=start_after,
                        end_by=end_by,
                        min_hours=min_hours,
                        max_hours=max_hours,
                        include_garage=include_garage,
                        mode=mode,
                        connect=connect if suggest_next else None,
                    )
                    if error:
                        st.session_state["suggestion"] = None
                        st.error(error)
                    else:
                        st.session_state["suggestion"] = built

    suggestion = st.session_state.get("suggestion")
    active = suggestion if _suggestion_is_current(suggestion, st.session_state.get("builder_block", ""), include_garage) else None
    suggested_ids = set(active["row_ids"]) if active else set()

    with right:
        if many:
            plans = st.session_state.get("batch_plan") or []
            with st.container(border=True):
                st.markdown(
                    f'<div class="sam-review-head" style="margin-top:0"><h3 class="sam-block-title">Ready to save</h3>'
                    f'<span class="sam-count-badge">{len(plans)} shifts</span></div>',
                    unsafe_allow_html=True,
                )
                if st.session_state.get("batch_plan") is None:
                    st.caption("Set the place, route, and hours, then generate. The shifts will show up here before anything is saved.")
                elif not plans:
                    reason = st.session_state.get("batch_empty_reason") or (
                        "Nothing open fits those rules. Try a shorter minimum, a later end time, or all routes."
                    )
                    st.warning(reason)
                else:
                    preview_rows = []
                    next_number = next_assignment_number(frame)
                    for index, plan in enumerate(plans):
                        ids = [int(value) for value in plan["row_ids"]]
                        first = frame.loc[frame["_id"].eq(ids[0])].iloc[0]
                        last_row = frame.loc[frame["_id"].eq(ids[-1])].iloc[0]
                        preview_rows.append(
                            {
                                "#": next_number + index,
                                "From": format_time(first["Start Time"]),
                                "To": format_time(last_row["End Time"]),
                                "Hours": format_hours(span_hours(frame, ids)),
                                "Trips": len(ids),
                                "Block": plan["block"],
                                "Routes": _route_path(frame, ids),
                            }
                        )
                    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True, height=min(420, 38 + 35 * len(preview_rows)))
                    st.caption("These are not saved yet. Each shift stays on one block and ends where it started.")
                    if st.button("Save all these shifts", type="primary", icon=":material/check:", use_container_width=True):
                        try:
                            _accept_batch(frame, plans)
                        except Exception as exc:
                            st.error(str(exc))
                        else:
                            st.rerun()
        else:
            with st.container(border=True):
                block_name = st.session_state.get("builder_block") or "—"
                if not block_name or block_name == "—":
                    st.markdown("**Block**")
                    st.caption("Choose a block to see its day.")
                else:
                    table = _block_table(frame, block_name, suggested_ids)
                    counts = table["Status"].value_counts() if not table.empty else {}
                    open_n = int(counts.get(OPEN, 0))
                    garage_n = int(counts.get(GARAGE, 0))
                    st.markdown(
                        f"""
                        <div class="sam-head">
                            <div>
                                <h3 class="sam-block-title">Block {html.escape(str(block_name))}</h3>
                                <p class="sam-block-count">{open_n} open trips · {garage_n} garage moves</p>
                            </div>
                            <div class="sam-legend">
                                <span><i class="sam-dot open"></i>Open</span>
                                <span><i class="sam-dot garage"></i>Garage</span>
                            </div>
                        </div>
                        {_trip_table_html(table)}
                        """,
                        unsafe_allow_html=True,
                    )

    if active:
        row_ids = [int(value) for value in active["row_ids"]]
        first = frame.loc[frame["_id"].eq(row_ids[0])].iloc[0]
        last_row = frame.loc[frame["_id"].eq(row_ids[-1])].iloc[0]
        hours = span_hours(frame, row_ids)
        label = propose_label(frame, st.session_state.get("builder_name"))
        warnings = describe_cut(
            frame,
            row_ids,
            str(active.get("hub") or ANY_PLACE),
            float(st.session_state["min_hours"]),
            float(st.session_state["max_hours"]),
            str(active.get("mode") or "full"),
            str(active.get("block")),
        )
        if label_exists(frame, label):
            previous = last_assigned_row(frame, label)
            if previous is not None:
                gap = (time_to_minutes(first["Start Time"]) or 0) - (time_to_minutes(previous["End Time"]) or 0)
                if gap < 0:
                    gap += 24 * 60
                if not same_place(first["Start Location"], previous["End Location"]):
                    warnings.append(
                        f"This piece starts at {clean_token(first['Start Location'])}, "
                        f"and {label} ended at {clean_token(previous['End Location'])}."
                    )
                elif gap < int(st.session_state["min_switch"]):
                    warnings.append(f"Only {gap} minutes to change buses.")
                elif gap > int(st.session_state["max_switch"]):
                    warnings.append(f"{gap} minutes between the last trip and this piece.")
        verb = "Added to" if label_exists(frame, label) else "Saved as"
        routes = [value for value in (_route_path(frame, row_ids) or "").split(", ") if value]
        route_chips = "".join(f'<span class="sam-route">{html.escape(value)}</span>' for value in routes) or "—"
        shift_time = f"{format_time(first['Start Time'])} – {format_time(last_row['End Time'])}"
        st.markdown(
            f"""
            <div class="sam-review-head">
                <h2>Review assignment {html.escape(label)}</h2>
                <span class="sam-pill-warn">Not saved yet</span>
            </div>
            <p class="sam-review-sub">Check the trips and adjust the shift before saving. {html.escape(verb)} {html.escape(label)}.</p>
            <div class="sam-review-card">
                <div class="sam-review-grid">
                    {_stat("clock", "Shift duration", format_hours(hours) or "—")}
                    {_stat("calendar", "Shift time", shift_time)}
                    {_stat("bus", "Trips", str(len(row_ids)))}
                    {_stat("layers", "Block", f"Block {active['block']}")}
                </div>
                <div class="sam-review-foot">
                    <span class="sam-where">{_icon("pin")}Report at {html.escape(clean_token(first['Start Location']))} · {html.escape(subtract_minutes(first['Start Time'], 15))}</span>
                    <span class="sam-routes">Routes {route_chips}</span>
                </div>
                {''.join(f'<div class="sam-warn">{html.escape(item)}</div>' for item in warnings)}
            </div>
            """,
            unsafe_allow_html=True,
        )
        preview_rows = _block_table(frame, str(active["block"]), set(row_ids))
        preview_rows = preview_rows.loc[preview_rows["Status"].eq(SUGGESTED), ["Start", "End", "Route", "Dir", "From", "To"]]
        with st.container(border=True):
            st.markdown('<p class="sam-card-title"><strong>Trip itinerary</strong></p>', unsafe_allow_html=True)
            st.dataframe(
                preview_rows.rename(columns={"Dir": "Direction"}),
                use_container_width=True,
                hide_index=True,
                height=min(460, 40 + 35 * max(len(preview_rows), 1)),
            )
            st.text_input("Note for the packet", key="cut_note", placeholder="Switch bus, break, survey the queue")
            st.caption("This note appears in the Word packet.")
            action_a, action_b, action_c = st.columns([1, 1, 2])
        if action_a.button("Shorter", use_container_width=True, disabled=len(row_ids) <= 1):
            active["row_ids"] = row_ids[:-1]
            st.session_state["suggestion"] = active
            st.rerun()
        eligible = [int(value) for value in active["eligible_ids"]]
        can_grow = active["row_ids"][-1] in eligible and eligible.index(int(active["row_ids"][-1])) < len(eligible) - 1
        if action_b.button("Longer", use_container_width=True, disabled=not can_grow):
            position = eligible.index(int(active["row_ids"][-1]))
            active["row_ids"] = [int(value) for value in active["row_ids"]] + [eligible[position + 1]]
            st.session_state["suggestion"] = active
            st.rerun()
        if action_c.button("Save this cut", type="primary", use_container_width=True):
            try:
                _accept(frame, active, label, str(st.session_state.get("cut_note") or ""))
            except Exception as exc:
                st.error(str(exc))
            else:
                st.rerun()
    elif suggestion:
        st.caption("The block or garage setting changed. Suggest the shift again.")

    with st.container(border=True):
        saved_count = f"{len(visible)} of {len(summary)}" if len(visible) != len(summary) else f"{len(summary)} saved"
        st.markdown(
            f'<div class="sam-review-head" style="margin-top:0"><h3 class="sam-block-title">Saved assignments</h3>'
            f'<span class="sam-count-badge">{saved_count}</span></div>',
            unsafe_allow_html=True,
        )
        board_left, board_right = st.columns([1.45, 1])
        with board_left:
            if summary.empty:
                st.caption("Saved shifts will show up here. Select one to keep adding to that person.")
                undo_slot = st.container()
            else:
                st.pills("Starts", _WHEN_CHOICES, key="assign_when")
                find_col, length_col, notes_col = st.columns(3)
                find_col.text_input(
                    "Find assignment",
                    key="assign_find",
                    placeholder="Name or block",
                    label_visibility="collapsed",
                )
                length_col.selectbox(
                    "Length",
                    ["Any length", "Under 7 hours", "7 to 9 hours", "Over 9 hours"],
                    key="assign_length",
                    label_visibility="collapsed",
                )
                notes_col.selectbox(
                    "Notes",
                    ["All notes", "Has notes", "No notes"],
                    key="assign_notes",
                    label_visibility="collapsed",
                )
                if visible.empty:
                    st.caption("No assignments match these filters.")
                else:
                    st.dataframe(
                        visible.loc[:, _SUMMARY_COLUMNS],
                        use_container_width=True,
                        hide_index=True,
                        height=38 + 35 * len(visible) if len(visible) <= 9 else 360,
                        selection_mode="single-row",
                        on_select="rerun",
                        key="made_table",
                    )
                st.caption("Tick a row to continue that assignment or unassign it.")
                if not visible.empty and len(visible) != len(summary):
                    shown_labels = [str(value) for value in visible["Assignment"]]
                    st.download_button(
                        f"Download these {len(shown_labels)}",
                        data=packets_zip(frame, notes, shown_labels),
                        file_name="selected_assignments.zip",
                        mime="application/zip",
                        type="primary",
                        icon=":material/download:",
                        use_container_width=True,
                    )
                selected_label = ""
                if picked and picked[0] < len(visible):
                    selected_label = str(visible.iloc[picked[0]]["Assignment"])
                unassign_slot, undo_slot = st.columns(2)
                if unassign_slot.button(
                    "Unassign selected assignment",
                    icon=":material/delete:",
                    disabled=not selected_label,
                    use_container_width=True,
                ):
                    _unassign(frame, selected_label)
                    st.rerun()
            if st.session_state.get("undo") and undo_slot.button(
                "Undo last save", icon=":material/undo:", use_container_width=True
            ):
                _undo(frame)
                st.rerun()
        with board_right:
            st.markdown(
                '<p class="sam-card-title sam-downloads" style="margin-bottom:0.1rem"><strong>Downloads</strong></p>'
                '<p class="sam-review-sub">Notes are printed on the Word packet. The Weekday sheet stays a trip grid.</p>',
                unsafe_allow_html=True,
            )
            excel = _excel_download(frame)
            st.download_button(
                "Download Weekday workbook",
                data=excel,
                file_name="Weekday.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                icon=":material/download:",
                use_container_width=True,
            )
            current = clean_token(st.session_state.get("builder_name"))
            if current and label_exists(frame, current):
                packet = _packet_download(frame, current, notes)
                st.download_button(
                    f"Download {current}",
                    data=packet,
                    file_name=f"{_file_piece(current)}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    icon=":material/description:",
                    use_container_width=True,
                )
            if assigned_labels:
                if st.button("Prepare all Word packets", icon=":material/article:", use_container_width=True):
                    try:
                        st.session_state["word_zip"] = packets_zip(frame, notes)
                        st.session_state["word_rev"] = st.session_state.get("revision", 0)
                    except Exception as exc:
                        st.error(str(exc))
                if st.session_state.get("word_zip") and st.session_state.get("word_rev") == st.session_state.get("revision", 0):
                    st.download_button(
                        "Download all packets",
                        data=st.session_state["word_zip"],
                        file_name="assignment_packets.zip",
                        mime="application/zip",
                        icon=":material/folder_zip:",
                        use_container_width=True,
                    )
            st.markdown(
                '<div class="sam-warn" style="margin-bottom:1.1rem">Download the workbook before you close this page. The sheet lives only in this session.</div>',
                unsafe_allow_html=True,
            )


def _excel_download(frame: pd.DataFrame) -> bytes:
    revision = st.session_state.get("revision", 0)
    if st.session_state.get("excel_rev") != revision or not st.session_state.get("excel_bytes"):
        st.session_state["excel_bytes"] = workbook_bytes(frame)
        st.session_state["excel_rev"] = revision
    return st.session_state["excel_bytes"]


def _packet_download(frame: pd.DataFrame, label: str, notes: dict) -> bytes:
    revision = st.session_state.get("revision", 0)
    cache = st.session_state.setdefault("packet_cache", {})
    key = f"{revision}:{label}"
    if key not in cache:
        cache[key] = packet_bytes(frame, label, notes)
    return cache[key]


def _file_piece(label: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_ " else "_" for ch in label).strip()
    return cleaned.replace(" ", "_") or "assignment"


def render_field_assignments_page() -> None:
    """OD entry for ?page=field_assignments. Page config stays on the main dashboard app."""
    _inject_css()
    _portal_bar()
    if st.session_state.get("frame") is None:
        _upload_screen()
    else:
        _workspace()
