from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_RCD_ROOT = Path(__file__).resolve().parent
_RCD_ON_PATH = False


def _ensure_rcd_path() -> None:
    global _RCD_ON_PATH
    root = str(_RCD_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
        _RCD_ON_PATH = True


def _clear_rcd_boot_flag() -> None:
    st.session_state.pop("rcd_boot_complete", None)


def _render_boot_splash(message: str = "Opening your Review Cycle workspace…") -> None:
    """Opaque full-screen splash so portal UI is never visible under RCD CSS."""
    safe = (
        message.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    st.markdown(
        f"""
        <style>
        header[data-testid="stHeader"],
        [data-testid="stToolbar"],
        [data-testid="stSidebar"],
        [data-testid="stSidebarCollapsedControl"] {{
            display: none !important;
        }}
        .main .block-container,
        .stMainBlockContainer,
        div[data-testid="stMainBlockContainer"] {{
            max-width: 100% !important;
            padding: 0 !important;
        }}
        .rcd-boot-splash {{
            position: fixed;
            inset: 0;
            z-index: 2147483000;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 14px;
            background: #F8FAFC;
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, sans-serif;
            color: #0F172A;
        }}
        .rcd-boot-splash__title {{
            margin: 0;
            font-size: 20px;
            font-weight: 700;
            letter-spacing: -0.02em;
        }}
        .rcd-boot-splash__msg {{
            margin: 0;
            font-size: 14px;
            color: #64748B;
        }}
        .rcd-boot-splash__spinner {{
            width: 28px;
            height: 28px;
            border: 3px solid #DBEAFE;
            border-top-color: #2563EB;
            border-radius: 50%;
            animation: rcd-boot-spin 0.8s linear infinite;
        }}
        @keyframes rcd-boot-spin {{
            to {{ transform: rotate(360deg); }}
        }}
        </style>
        <div class="rcd-boot-splash" role="status" aria-live="polite">
            <div class="rcd-boot-splash__spinner" aria-hidden="true"></div>
            <p class="rcd-boot-splash__title">Review Cycle Dashboard</p>
            <p class="rcd-boot-splash__msg">{safe}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_review_cycle(od_user: dict, rcd_role: str) -> None:
    """Render Review Cycle Dashboard inside the unified OD Collection app."""
    _ensure_rcd_path()

    from rc_auth.access import allowed_pages, is_cleaning_head
    from core.config import REVIEW_CYCLE_SCHEMA, env
    from core.schema import bootstrap_database, refresh_projects_if_due, schema_is_ready
    from views.admin import render_admin_page
    from views.cleaning import render_cleaning_page
    from views.demographic import render_demographic_page
    from views.demographic_config import render_demographic_config_page
    from views.field import render_field_page
    from views.history import render_history_page
    from views.manager_dashboard import render_manager_dashboard
    from views.project_dashboard import render_project_dashboard
    from views.review import render_review_page
    from views.reviewer_stats import render_reviewer_stats_page
    from views.supervisor import render_supervisor_page
    from views.sync_admin import render_sync_admin_page
    from views.ui import (
        inject_global_css,
        set_header_context,
        sidebar_account_label,
        sidebar_brand,
        sidebar_nav_label,
        render_operation_flash,
    )

    PAGE_HANDLERS = {
        "project_dashboard": ("Project Dashboard", render_project_dashboard),
        "cleaning": ("Elvis_Review", render_cleaning_page),
        "review": ("Combined Checks", render_review_page),
        "supervisor": ("Supervisor View Only", render_supervisor_page),
        "history": ("Record History", render_history_page),
        "admin": ("Admin Approval", render_admin_page),
        "demographic_config": ("Demographic Flag Config", render_demographic_config_page),
        "demographic": ("Demographic Review", render_demographic_page),
        "field": ("Field Team", render_field_page),
        "manager_dashboard": ("Manager Analytics", render_manager_dashboard),
        "reviewer_stats": ("Reviewer Stats", render_reviewer_stats_page),
        "sync_admin": ("Sync & Admin", render_sync_admin_page),
    }

    email = str(od_user.get("email") or od_user.get("EMAIL") or "")
    username = str(od_user.get("username") or od_user.get("name") or email)
    role = str(rcd_role or "").lower()

    user = {
        "EMAIL": email,
        "email": email,
        "ROLE": role,
        "role": role,
        "name": username,
        "username": username,
    }

    # Boot phase: cover the previous portal page and finish schema checks BEFORE
    # injecting RCD global CSS (which otherwise washes out leftover portal UI).
    schema_ready = bool(st.session_state.get("rcd_schema_ready"))
    if not st.session_state.get("rcd_boot_complete"):
        _render_boot_splash(
            "Opening your Review Cycle workspace…"
            if not schema_ready
            else "Loading Review Cycle…"
        )
        if not schema_ready:
            schema_ready = schema_is_ready()
            if schema_ready:
                st.session_state["rcd_schema_ready"] = True
            else:
                bootstrap_key = f"rcd_bootstrap_done_{REVIEW_CYCLE_SCHEMA}"
                if not st.session_state.get(bootstrap_key):
                    database = env("SNOWFLAKE_DATABASE") or "(database not set)"
                    try:
                        result = bootstrap_database()
                        st.session_state[bootstrap_key] = True
                        st.session_state["rcd_schema_ready"] = True
                        st.session_state["rcd_boot_flash"] = (
                            f"Your Review Cycle workspace is ready with "
                            f"**{result['projects_seeded']}** project(s)."
                        )
                    except Exception as exc:
                        st.session_state["rcd_boot_error"] = (
                            f"Could not initialize `{database}.{REVIEW_CYCLE_SCHEMA}`. "
                            f"Check Snowflake grants and APP_CONFIG_SCHEMA in .env. Detail: {exc}"
                        )
                else:
                    st.session_state["rcd_boot_error"] = (
                        f"Review Cycle schema `{REVIEW_CYCLE_SCHEMA}` is not ready. "
                        "A super admin can run **Initialize / migrate database schema** "
                        "from Sync & Admin."
                    )
        st.session_state["rcd_boot_complete"] = True
        st.rerun()
        return

    inject_global_css(auth_mode=False)

    boot_error = st.session_state.pop("rcd_boot_error", None)
    if boot_error:
        st.error(boot_error)
        if st.button("← Back to portal selection", key="rcd_boot_err_back"):
            _clear_rcd_boot_flag()
            st.query_params["page"] = "portal_select"
            st.rerun()
        return

    boot_flash = st.session_state.pop("rcd_boot_flash", None)
    if boot_flash:
        st.success(boot_flash)

    if not st.session_state.get("rcd_schema_ready"):
        st.error(
            f"Review Cycle schema `{REVIEW_CYCLE_SCHEMA}` is not ready. "
            "A super admin can run **Initialize / migrate database schema** from Sync & Admin."
        )
        if st.button("← Back to portal selection", key="rcd_schema_back"):
            _clear_rcd_boot_flag()
            st.query_params["page"] = "portal_select"
            st.rerun()
        return

    pages = allowed_pages(role)
    if role == "cleaning" and not is_cleaning_head(user):
        pages = [page for page in pages if page != "history"]
    labels = [PAGE_HANDLERS[key][0] for key in pages if key in PAGE_HANDLERS]
    keys = [key for key in pages if key in PAGE_HANDLERS]
    if not keys:
        st.error("No Review Cycle pages are configured for your role.")
        if st.button("← Back to portal selection", key="rcd_nopages_back"):
            _clear_rcd_boot_flag()
            st.query_params["page"] = "portal_select"
            st.rerun()
        return

    with st.sidebar:
        sidebar_brand()
        from core.session_project import render_sidebar_project_selector

        render_sidebar_project_selector()
        sidebar_nav_label()
        selected_label = st.radio("Navigation", labels, label_visibility="collapsed", key="rcd_nav_radio")
        st.markdown('<div class="ref-sidebar-divider"></div>', unsafe_allow_html=True)
        sidebar_account_label()
        if len(_allowed_portals_for_od_user(od_user)) > 1:
            if st.button("Switch Portal", use_container_width=True, type="secondary"):
                _clear_rcd_boot_flag()
                st.query_params["page"] = "portal_select"
                st.rerun()
        if st.button("Sign out", use_container_width=True, type="secondary"):
            from authentication.auth import logout

            _clear_rcd_boot_flag()
            logout()

    # Keep APP_CONFIG projects current without blocking page loads or sign-out.
    refresh_projects_if_due()

    selected_key = keys[labels.index(selected_label)]
    set_header_context(
        user_name=username,
        role=role,
        unread=_unread_count_for_user(user),
        user=user,
        email=email,
    )
    from views.ui.notifications import handle_pending_notification_actions

    handle_pending_notification_actions(user)
    render_operation_flash()
    PAGE_HANDLERS[selected_key][1](user)


def _unread_count_for_user(user: dict) -> int:
    from views.ui.notifications import unread_count_for_user

    try:
        return int(unread_count_for_user(user))
    except Exception:
        return 0


def _allowed_portals_for_od_user(od_user: dict) -> list[str]:
    from authentication.auth import allowed_portals

    return allowed_portals(str(od_user.get("email", "")), od_user.get("role"))
