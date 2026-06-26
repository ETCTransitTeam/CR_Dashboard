"""Field Assignments portal page (RunCut upload, assign, export)."""
from __future__ import annotations

import html

import streamlit as st

from field_assignments.core.assign import fill_assignment_numbers
from field_assignments.core.export_docs import export_reports_zip
from field_assignments.core.storage import list_workbook_versions, load_workbook_version, save_workbook_version
from field_assignments.core.summary import route_coverage_summary, style_coverage_dataframe
from field_assignments.core.time_utils import parse_assignment_filter
from field_assignments.core.workbook import workbook_options


def _workbook_bytes() -> bytes | None:
    return st.session_state.get("fa_workbook_bytes")


def _set_workbook(uploaded, sheet_name: str | None = None) -> None:
    st.session_state["fa_workbook_bytes"] = uploaded.getvalue()
    st.session_state["fa_workbook_name"] = uploaded.name
    st.session_state["fa_sheet_name"] = sheet_name or None
    try:
        st.session_state["fa_workbook_options"] = workbook_options(
            st.session_state["fa_workbook_bytes"],
            st.session_state["fa_sheet_name"],
        )
    except Exception as exc:
        st.session_state.pop("fa_workbook_options", None)
        raise exc


def _display_name_for_portal(user: dict) -> str:
    name = str(user.get("username") or user.get("name") or user.get("email") or "User")
    return name.split("@", 1)[0].replace(".", " ").title() if "@" in name else name


def _user_initials(user: dict) -> str:
    name = str(user.get("username") or user.get("name") or user.get("email") or "User")
    if "@" in name:
        name = name.split("@", 1)[0]
    parts = [part for part in name.replace(".", " ").replace("_", " ").split() if part]
    if not parts:
        return "U"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _inject_page_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp:has(.fa-page-marker) {
            background:
                radial-gradient(circle at top right, rgba(11, 107, 203, 0.08), transparent 28%),
                linear-gradient(180deg, #e8f1fa 0%, #f5f9fc 48%, #ffffff 100%);
        }
        .stApp:has(.fa-page-marker) .block-container {
            padding-top: 1.25rem;
            padding-bottom: 2.75rem;
            max-width: 1120px;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {
            margin-bottom: 0.85rem;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab-list"] {
            gap: 0.35rem;
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #d4e0ec;
            border-radius: 14px;
            padding: 0.4rem;
            box-shadow: 0 4px 18px rgba(15, 45, 75, 0.07);
            backdrop-filter: blur(6px);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab"] {
            height: 2.55rem;
            border-radius: 10px;
            color: #516579;
            font-weight: 650;
            padding: 0 1.05rem;
            transition: all 0.15s ease;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab"]:hover {
            background: #f0f7fd;
            color: #0b6bcb;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [aria-selected="true"] {
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff !important;
            box-shadow: 0 6px 16px rgba(11, 107, 203, 0.28);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
            background-color: transparent !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab-panel"] {
            padding-top: 1.15rem;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(255, 255, 255, 0.96);
            border-color: #d7e3ef !important;
            border-radius: 16px !important;
            box-shadow: 0 10px 28px rgba(18, 52, 86, 0.07);
            padding: 0.95rem 1rem 1rem 1rem;
            transition: box-shadow 0.15s ease, transform 0.15s ease;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            box-shadow: 0 14px 32px rgba(18, 52, 86, 0.09);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stMetric"] {
            background: linear-gradient(180deg, #f8fbff 0%, #ffffff 100%);
            border: 1px solid #dce8f3;
            border-radius: 14px;
            padding: 0.85rem 1rem;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.85), 0 3px 10px rgba(18, 52, 86, 0.05);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stMetric"] label {
            color: #6b7f92;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #173044;
            font-weight: 800;
            font-size: 1.35rem;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stFileUploader"] section {
            border: 1.5px dashed #9ec3e4 !important;
            background: linear-gradient(180deg, #f8fbff 0%, #f3f8fd 100%) !important;
            border-radius: 14px !important;
            min-height: 96px;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTextInput"] input,
        .stApp:has(.fa-page-marker) div[data-testid="stSelectbox"] > div > div,
        .stApp:has(.fa-page-marker) div[data-testid="stTimeInput"] input {
            border-radius: 10px !important;
            border-color: #d7e3ef !important;
            background: #fbfdff !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stButton"] > button[kind="primary"],
        .stApp:has(.fa-page-marker) div[data-testid="stDownloadButton"] > button[kind="primary"] {
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%) !important;
            border: none !important;
            border-radius: 10px !important;
            font-weight: 700 !important;
            box-shadow: 0 8px 18px rgba(11, 107, 203, 0.24) !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stButton"] > button[kind="primary"]:hover,
        .stApp:has(.fa-page-marker) div[data-testid="stDownloadButton"] > button[kind="primary"]:hover {
            box-shadow: 0 10px 22px rgba(11, 107, 203, 0.32) !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stAlert"] {
            border-radius: 12px;
            border-width: 1px;
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro {
            position: relative;
            overflow: hidden;
            background: linear-gradient(120deg, #0657c9 0%, #0877db 52%, #22b8e6 100%);
            border-radius: 16px;
            padding: 1.2rem 1.35rem;
            margin-bottom: 1rem;
            color: #ffffff;
            box-shadow: 0 14px 30px rgba(11, 107, 203, 0.22);
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro:before {
            content: "";
            position: absolute;
            right: -2rem;
            top: -2rem;
            width: 8rem;
            height: 8rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.12);
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro:after {
            content: "";
            position: absolute;
            right: 3.5rem;
            bottom: -2.5rem;
            width: 6rem;
            height: 6rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.08);
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro h3 {
            position: relative;
            margin: 0;
            font-size: 1.22rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: -0.01em;
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro p {
            position: relative;
            margin: 0.4rem 0 0 0;
            color: #e8f6ff;
            font-size: 0.93rem;
            line-height: 1.5;
            max-width: 760px;
        }
        .stApp:has(.fa-page-marker) .fa-step-title {
            display: flex;
            align-items: center;
            gap: 0.65rem;
            margin: 0 0 0.85rem 0;
            padding-bottom: 0.65rem;
            border-bottom: 1px solid #edf2f7;
            font-size: 1.02rem;
            font-weight: 700;
            color: #173044;
        }
        .stApp:has(.fa-page-marker) .fa-step-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 1.75rem;
            height: 1.75rem;
            border-radius: 999px;
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff;
            font-size: 0.8rem;
            font-weight: 800;
            flex-shrink: 0;
            box-shadow: 0 4px 10px rgba(11, 107, 203, 0.22);
        }
        .stApp:has(.fa-page-marker) .fa-workflow-strip {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.65rem;
            margin-bottom: 1rem;
        }
        .stApp:has(.fa-page-marker) .fa-workflow-item {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #dbe7f2;
            border-radius: 12px;
            padding: 0.75rem 0.85rem;
            box-shadow: 0 4px 14px rgba(18, 52, 86, 0.05);
        }
        .stApp:has(.fa-page-marker) .fa-workflow-item strong {
            display: block;
            color: #173044;
            font-size: 0.86rem;
            margin-bottom: 0.15rem;
        }
        .stApp:has(.fa-page-marker) .fa-workflow-item span {
            color: #6b7f92;
            font-size: 0.78rem;
            line-height: 1.35;
        }
        .stApp:has(.fa-page-marker) .fa-version-row {
            display: grid;
            grid-template-columns: 2.2fr 1fr 1.2fr;
            gap: 0.75rem;
            align-items: center;
            padding: 0.75rem 0.85rem;
            margin-bottom: 0.55rem;
            background: #f8fbff;
            border: 1px solid #e1ebf4;
            border-radius: 12px;
        }
        .stApp:has(.fa-page-marker) .fa-version-row .fa-version-name {
            color: #173044;
            font-weight: 650;
            font-size: 0.9rem;
        }
        .stApp:has(.fa-page-marker) .fa-version-row .fa-version-meta {
            color: #6b7f92;
            font-size: 0.82rem;
        }
        .stApp:has(.fa-page-marker) .fa-header-user-wrap {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 0.65rem;
            margin-bottom: 0.55rem;
        }
        .stApp:has(.fa-page-marker) .fa-header-avatar {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 2.35rem;
            height: 2.35rem;
            border-radius: 999px;
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff;
            font-size: 0.74rem;
            font-weight: 800;
            box-shadow: 0 4px 14px rgba(11, 107, 203, 0.28);
            border: 2px solid #ffffff;
        }
        .stApp:has(.fa-page-marker) .fa-header-user-text {
            text-align: right;
        }
        .stApp:has(.fa-page-marker) .fa-header-title-row {
            display: flex;
            align-items: center;
            gap: 0.8rem;
        }
        .stApp:has(.fa-page-marker) .fa-header-app-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 2.65rem;
            height: 2.65rem;
            border-radius: 14px;
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff;
            font-size: 1.15rem;
            box-shadow: 0 8px 18px rgba(11, 107, 203, 0.24);
            flex-shrink: 0;
        }
        .fa-page-marker,
        .fa-header-marker {
            display: none;
        }
        @media (max-width: 768px) {
            .stApp:has(.fa-page-marker) .fa-workflow-strip {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _workflow_strip() -> None:
    st.markdown(
        """
        <div class="fa-workflow-strip">
            <div class="fa-workflow-item">
                <strong>1. Load workbook</strong>
                <span>Upload RunCut Excel and load route/location dropdowns.</span>
            </div>
            <div class="fa-workflow-item">
                <strong>2. Build assignments</strong>
                <span>Apply up to 10 rules to fill blank Asn# rows.</span>
            </div>
            <div class="fa-workflow-item">
                <strong>3. Export reports</strong>
                <span>Generate Word documents and download ZIP or Excel.</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _tab_intro(title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="fa-tab-intro">
            <h3>{html.escape(title)}</h3>
            <p>{html.escape(description)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _step_title(step: int, title: str) -> None:
    st.markdown(
        f"""
        <div class="fa-step-title">
            <span class="fa-step-badge">{step}</span>
            <span>{html.escape(title)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_page_header() -> None:
    from authentication.auth import is_super_admin, logout

    st.markdown(
        """
        <style>
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) {
            background: linear-gradient(135deg, #dceeff 0%, #eaf4fc 38%, #f3f9ff 100%);
            border: 1px solid #c5daf0;
            border-radius: 18px;
            padding: 1.05rem 1.2rem 1rem 1.2rem;
            margin-bottom: 0.85rem;
            box-shadow: 0 12px 30px rgba(11, 107, 203, 0.1);
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-actions) > div[data-testid="stVerticalBlock"] {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 0 !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-actions) div[data-testid="stHorizontalBlock"]:has(.fa-header-btn-switch) {
            gap: 0.5rem !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="stHorizontalBlock"] {
            align-items: center !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) h1 {
            font-size: 1.75rem;
            font-weight: 800;
            margin: 0;
            padding: 0;
            line-height: 1.15;
            color: #173044;
            letter-spacing: -0.02em;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="stCaptionContainer"] p {
            margin-top: 0.35rem;
            color: #6b7f92;
            font-size: 0.92rem;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) .fa-header-user-name {
            font-weight: 700;
            font-size: 0.92rem;
            color: #173044;
            margin: 0;
            line-height: 1.2;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) .fa-header-user-role {
            color: #6b7f92;
            font-size: 0.78rem;
            margin: 0.1rem 0 0 0;
            line-height: 1.2;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="stButton"] {
            margin-bottom: 0 !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button,
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button[kind="secondary"] {
            min-height: 2.15rem;
            height: 2.15rem;
            min-width: 7.1rem;
            padding: 0 0.75rem;
            font-size: 0.78rem;
            font-weight: 700;
            white-space: nowrap !important;
            border-radius: 10px;
            border: 1px solid #93c5fd !important;
            background: linear-gradient(180deg, #ffffff 0%, #eff6ff 100%) !important;
            color: #1d4ed8 !important;
            box-shadow: 0 2px 8px rgba(37, 99, 235, 0.12) !important;
            transition: all 0.15s ease;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button p,
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button p {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
            font-size: 0.78rem !important;
            line-height: 1 !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button:hover {
            background: linear-gradient(180deg, #dbeafe 0%, #bfdbfe 100%) !important;
            border-color: #60a5fa !important;
            color: #1e40af !important;
            box-shadow: 0 4px 12px rgba(37, 99, 235, 0.18) !important;
            transform: translateY(-1px);
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button,
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button[kind="secondary"] {
            min-height: 2.15rem;
            height: 2.15rem;
            min-width: 5.2rem;
            padding: 0 0.75rem;
            font-size: 0.78rem;
            font-weight: 700;
            white-space: nowrap !important;
            border-radius: 10px;
            border: 1px solid #cbd5e1 !important;
            background: rgba(255, 255, 255, 0.92) !important;
            color: #475569 !important;
            box-shadow: 0 2px 8px rgba(15, 23, 42, 0.05) !important;
            transition: all 0.15s ease;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button:hover {
            background: #fff5f5 !important;
            border-color: #fca5a5 !important;
            color: #dc2626 !important;
            box-shadow: 0 4px 12px rgba(220, 38, 38, 0.12) !important;
            transform: translateY(-1px);
        }
        .fa-header-btn-switch,
        .fa-header-btn-logout,
        .fa-header-actions {
            display: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    user = st.session_state.get("user", {})
    display_name = _display_name_for_portal(user)
    initials = _user_initials(user)
    email = str(user.get("email", ""))

    st.markdown('<div class="fa-header-marker"></div>', unsafe_allow_html=True)
    title_col, actions_col = st.columns([5.4, 2.8], vertical_alignment="center")
    with title_col:
        st.markdown(
            """
            <div class="fa-header-title-row">
                <span class="fa-header-app-icon">▤</span>
                <div>
                    <h1 style="margin:0;padding:0;font-size:1.75rem;font-weight:800;color:#173044;letter-spacing:-0.02em;">
                        Survey Assignment Manager
                    </h1>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.caption("Upload a RunCut workbook, assign surveyors, and export Word assignment reports.")
    with actions_col:
        st.markdown('<div class="fa-header-actions"></div>', unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class="fa-header-user-wrap">
                <div class="fa-header-user-text">
                    <p class="fa-header-user-name">{html.escape(display_name)}</p>
                    <p class="fa-header-user-role">Administrator</p>
                </div>
                <span class="fa-header-avatar">{html.escape(initials)}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        switch_col, logout_col = st.columns([1.35, 1], gap="small")
        with switch_col:
            st.markdown('<div class="fa-header-btn-switch"></div>', unsafe_allow_html=True)
            if is_super_admin(email):
                if st.button("Switch Portal", type="secondary"):
                    st.query_params["page"] = "admin_portal_select"
                    st.rerun()
            elif st.button("OD Dashboard", type="secondary"):
                st.query_params["page"] = "main"
                st.rerun()
        with logout_col:
            st.markdown('<div class="fa-header-btn-logout"></div>', unsafe_allow_html=True)
            if st.button("Logout", type="secondary"):
                logout()


def _render_export_tab() -> None:
    _tab_intro(
        "Create Reports",
        "Upload a workbook with assignment numbers already filled, then export Word documents.",
    )

    with st.container(border=True):
        _step_title(1, "Upload workbook")
        uploaded = st.file_uploader("RunCut Excel file (.xlsx)", type=["xlsx"], key="fa_export_upload")
        sheet_name = st.text_input("Sheet name (blank = active sheet)", key="fa_export_sheet")
        assignment_filter = st.text_input(
            "Assignments (optional)",
            placeholder="All, or enter 1,2,9",
            key="fa_export_filter",
            help="Leave blank to export every assignment with a nonblank Asn#.",
        )

        if uploaded is not None:
            try:
                _set_workbook(uploaded, sheet_name or None)
                opts = st.session_state.get("fa_workbook_options", {})
                st.success(f"Loaded {uploaded.name}")
                c1, c2, c3 = st.columns(3)
                c1.metric("Sheet", str(opts.get("sheet", "")))
                c2.metric("Max Asn#", int(opts.get("max_assignment", 0)))
                c3.metric("Blank rows", int(opts.get("blank_rows", 0)))
            except Exception as exc:
                st.error(str(exc))

    with st.container(border=True):
        _step_title(2, "Generate documents")
        if st.button("Generate Word Documents", type="primary", key="fa_export_btn", use_container_width=True):
            data = _workbook_bytes()
            if not data:
                st.error("Upload a RunCut workbook first.")
                return
            try:
                wanted = parse_assignment_filter(assignment_filter)
                zip_bytes, summaries = export_reports_zip(data, st.session_state.get("fa_sheet_name"), wanted)
                st.session_state["fa_last_export_summary"] = summaries
                st.session_state["fa_last_export_zip"] = zip_bytes
                st.success(f"Exported {len(summaries)} Word document(s).")
            except Exception as exc:
                st.error(str(exc))

        zip_bytes = st.session_state.get("fa_last_export_zip")
        if zip_bytes:
            st.download_button(
                "Download ZIP",
                data=zip_bytes,
                file_name="RunCut_Assignment_Documents.zip",
                mime="application/zip",
                type="primary",
                use_container_width=True,
            )

    summaries = st.session_state.get("fa_last_export_summary")
    if summaries:
        with st.container(border=True):
            _step_title(3, "Export summary")
            st.dataframe(summaries, use_container_width=True, hide_index=True)


def _render_assign_tab() -> None:
    _tab_intro(
        "Create Assignments",
        "Load route dropdowns, define assignment rules, and fill blank Asn# rows in the workbook.",
    )

    with st.container(border=True):
        _step_title(1, "Load workbook")
        uploaded = st.file_uploader("RunCut Excel file (.xlsx)", type=["xlsx"], key="fa_assign_upload")
        sheet_name = st.text_input("Sheet name (blank = active sheet)", key="fa_assign_sheet")
        st.caption("Upload first to load dropdowns for route and locations.")

        if st.button("Load Assignment Dropdowns", key="fa_load_dropdowns", type="primary", use_container_width=True):
            if uploaded is None:
                st.error("Choose an .xlsx workbook first.")
                return
            try:
                _set_workbook(uploaded, sheet_name or None)
                st.session_state["fa_rules_loaded"] = True
                st.success("Dropdowns loaded.")
            except Exception as exc:
                st.error(str(exc))

    if uploaded is not None and not st.session_state.get("fa_rules_loaded"):
        try:
            _set_workbook(uploaded, sheet_name or None)
        except Exception:
            pass

    opts = st.session_state.get("fa_workbook_options")
    if not opts or not st.session_state.get("fa_rules_loaded"):
        st.info("Upload a workbook and click **Load Assignment Dropdowns** to begin.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Blank Asn# rows", int(opts["blank_rows"]))
    c2.metric("Next Asn#", int(opts["next_assignment"]))
    c3.metric("Sheet", str(opts["sheet"]))
    st.caption("Fill up to 10 rule rows; each checked row creates one new assignment number.")

    with st.container(border=True):
        _step_title(2, "Assignment settings")
        set_col1, set_col2 = st.columns(2)
        with set_col1:
            tolerance = st.selectbox(
                "Time tolerance",
                options=[
                    ("Exact times", 0),
                    ("+/- 5 minutes", 5),
                    ("+/- 10 minutes", 10),
                    ("+/- 15 minutes", 15),
                    ("+/- 20 minutes", 20),
                    ("+/- 30 minutes", 30),
                ],
                format_func=lambda item: item[0],
                index=3,
                key="fa_tolerance",
            )[1]
        with set_col2:
            scan_order = st.selectbox(
                "Scan order",
                options=[("Top to bottom", "top_to_bottom"), ("Bottom to top", "bottom_to_top")],
                format_func=lambda item: item[0],
                key="fa_scan_order",
            )[1]

    route_start = opts.get("route_start_locations") or {}
    route_end = opts.get("route_end_locations") or {}
    blocks = ["(All blocks)"] + list(opts.get("blocks") or [])
    routes = list(opts.get("routes") or [])

    rules_list: list[dict[str, str]] = []
    with st.container(border=True):
        _step_title(3, "Assignment rules")
        st.caption(
            "Start Location and Start From/To find the first row. "
            "Final End Location and End From/To find the last row."
        )
        for index in range(1, 11):
            use_key = f"fa_rule_{index}_use"
            st.checkbox(f"Use rule {index}", value=(index == 1), key=use_key)
            if not st.session_state.get(use_key):
                continue

            with st.container(border=True):
                st.markdown(f"**Rule {index}**")
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    block_sel = st.selectbox("Block", blocks, key=f"fa_rule_{index}_block")
                with c2:
                    route_sel = st.selectbox("Route", [""] + routes, key=f"fa_rule_{index}_route")
                with c3:
                    start_locs = route_start.get(route_sel, []) if route_sel else list(opts.get("start_locations") or [])
                    start_loc = st.selectbox("Start location", [""] + start_locs, key=f"fa_rule_{index}_start_loc")
                with c4:
                    end_locs = route_end.get(route_sel, []) if route_sel else list(opts.get("end_locations") or [])
                    end_loc = st.selectbox("Final end location", [""] + end_locs, key=f"fa_rule_{index}_end_loc")

                t1, t2, t3, t4 = st.columns(4)
                with t1:
                    start_from = st.time_input("Start from", key=f"fa_rule_{index}_start_from")
                with t2:
                    start_to = st.time_input("Start to", key=f"fa_rule_{index}_start_to")
                with t3:
                    end_from = st.time_input("End from", key=f"fa_rule_{index}_end_from")
                with t4:
                    end_to = st.time_input("End to", key=f"fa_rule_{index}_end_to")

            if not route_sel or not start_loc or not end_loc:
                st.warning(f"Rule {index}: fill route and both locations.")
                continue

            rules_list.append(
                {
                    "block": "" if block_sel == "(All blocks)" else block_sel,
                    "route": route_sel,
                    "start_location": start_loc,
                    "end_location": end_loc,
                    "start_from": start_from.strftime("%H:%M"),
                    "start_to": start_to.strftime("%H:%M"),
                    "shift_from": end_from.strftime("%H:%M"),
                    "shift_to": end_to.strftime("%H:%M"),
                    "scan_order": scan_order,
                    "tolerance": str(tolerance),
                }
            )

    with st.container(border=True):
        _step_title(4, "Create output files")
        if st.button("Fill Assignment Numbers", type="primary", key="fa_fill_btn", use_container_width=True):
            data = _workbook_bytes()
            if not data:
                st.error("Upload and load a workbook first.")
                return
            if not rules_list:
                st.error("Enable and complete at least one rule row.")
                return
            try:
                updated_bytes, results = fill_assignment_numbers(
                    data, st.session_state.get("fa_sheet_name"), rules_list
                )
                st.session_state["fa_workbook_bytes"] = updated_bytes
                st.session_state["fa_workbook_options"] = workbook_options(
                    updated_bytes, st.session_state.get("fa_sheet_name")
                )
                zip_bytes, summaries = export_reports_zip(updated_bytes, st.session_state.get("fa_sheet_name"))
                st.session_state["fa_last_export_summary"] = summaries
                st.session_state["fa_last_export_zip"] = zip_bytes
                st.session_state["fa_updated_workbook"] = updated_bytes
                parts = [f"assignment {item['assignment']} on {item['count']} row(s)" for item in results]
                st.success(f"Created {len(results)} assignment(s): " + "; ".join(parts) + ".")
            except Exception as exc:
                st.error(str(exc))

    updated_bytes = st.session_state.get("fa_updated_workbook")
    zip_bytes = st.session_state.get("fa_last_export_zip")
    if updated_bytes or zip_bytes:
        d1, d2 = st.columns(2)
        with d1:
            if updated_bytes:
                st.download_button(
                    "Download updated Excel",
                    data=updated_bytes,
                    file_name="RunCut_Assignments_Updated.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
        with d2:
            if zip_bytes:
                st.download_button(
                    "Download Word reports ZIP",
                    data=zip_bytes,
                    file_name="RunCut_Assignment_Documents.zip",
                    mime="application/zip",
                    use_container_width=True,
                )

    summaries = st.session_state.get("fa_last_export_summary")
    if summaries:
        with st.container(border=True):
            _step_title(5, "Export summary")
            st.dataframe(summaries, use_container_width=True, hide_index=True)


def _render_summary_tab() -> None:
    _tab_intro(
        "Summary & Versions",
        "Review route coverage and manage saved workbook versions for this project.",
    )

    data = _workbook_bytes()
    with st.container(border=True):
        _step_title(1, "Route coverage")
        if data:
            try:
                df = route_coverage_summary(data, st.session_state.get("fa_sheet_name"))
                st.dataframe(style_coverage_dataframe(df), use_container_width=True, hide_index=True)
            except Exception as exc:
                st.error(str(exc))
        else:
            st.info("Upload a workbook on the Export or Assign tab to see route coverage.")

    with st.container(border=True):
        _step_title(2, "Version history")
        label = st.text_input(
            "Project / city label for S3 versions",
            value=st.session_state.get("selected_project", "general"),
            key="fa_version_label",
        )

        if data and st.button(
            "Save current workbook version to S3",
            key="fa_save_version",
            type="primary",
            use_container_width=True,
        ):
            user = st.session_state.get("user", {})
            meta = save_workbook_version(
                data,
                label=label,
                original_filename=st.session_state.get("fa_workbook_name", "runcut.xlsx"),
                uploaded_by=str(user.get("email", "")),
                sheet_name=st.session_state.get("fa_sheet_name"),
            )
            if meta:
                st.success(f"Saved version (max Asn# {meta.get('max_asn')}) at {meta.get('uploaded_at')}.")
            else:
                st.warning("S3 bucket not configured (`bucket_name` in .env). Version not saved.")

        versions = list_workbook_versions(label)
        if not versions:
            st.caption("No saved versions yet for this label.")
            return

        for idx, version in enumerate(versions):
            row_col, load_col = st.columns([5, 1])
            with row_col:
                st.markdown(
                    f"""
                    <div class="fa-version-row">
                        <div>
                            <div class="fa-version-name">{html.escape(str(version.get("original_filename", "workbook")))}</div>
                            <div class="fa-version-meta">Max Asn#: {html.escape(str(version.get("max_asn", "?")))}</div>
                        </div>
                        <div class="fa-version-meta">{html.escape(str(version.get("uploaded_at", "")))}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with load_col:
                if st.button("Load", key=f"fa_load_version_{idx}", use_container_width=True):
                    loaded = load_workbook_version(version.get("xlsx_key", ""))
                    if loaded:
                        st.session_state["fa_workbook_bytes"] = loaded
                        st.session_state["fa_workbook_name"] = version.get("original_filename", "runcut.xlsx")
                        st.session_state["fa_sheet_name"] = version.get("sheet")
                        st.session_state["fa_workbook_options"] = workbook_options(
                            loaded, st.session_state.get("fa_sheet_name")
                        )
                        st.session_state["fa_rules_loaded"] = True
                        st.success("Version loaded into session.")
                        st.rerun()
                    else:
                        st.error("Could not load that version from S3.")


def _workbook_status_banner() -> None:
    data = _workbook_bytes()
    if not data:
        return
    opts = st.session_state.get("fa_workbook_options") or {}
    with st.container(border=True):
        st.markdown("**Current workbook in session**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("File", str(st.session_state.get("fa_workbook_name", "Loaded")))
        c2.metric("Sheet", str(opts.get("sheet", "—")))
        c3.metric("Max Asn#", int(opts.get("max_assignment", 0)))
        c4.metric("Blank rows", int(opts.get("blank_rows", 0)))


def render_field_assignments_page() -> None:
    """Main entry for ?page=field_assignments."""
    _inject_page_styles()
    st.markdown('<div class="fa-page-marker"></div>', unsafe_allow_html=True)
    _render_page_header()
    _workflow_strip()
    _workbook_status_banner()

    export_tab, assign_tab, summary_tab = st.tabs(
        ["📄 Export Reports", "✏️ Create Assignments", "📊 Summary & Versions"]
    )
    with export_tab:
        _render_export_tab()
    with assign_tab:
        _render_assign_tab()
    with summary_tab:
        _render_summary_tab()
