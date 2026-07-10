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


def render_review_cycle(od_user: dict, rcd_role: str) -> None:
    """Render Review Cycle Dashboard inside the unified OD Collection app."""
    _ensure_rcd_path()

    from rc_auth.access import allowed_pages
    from core.config import REVIEW_CYCLE_SCHEMA, env
    from core.schema import bootstrap_database, ensure_migrations, repair_timestamp_columns, schema_is_ready
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
    from views.ui import inject_global_css, set_header_context, sidebar_account_label, sidebar_brand, sidebar_nav_label

    PAGE_HANDLERS = {
        "project_dashboard": ("Project Dashboard", render_project_dashboard),
        "cleaning": ("Elvis_Review", render_cleaning_page),
        "review": ("Combined Checks", render_review_page),
        "supervisor": ("Supervisor View Only", render_supervisor_page),
        "history": ("Record History", render_history_page),
        "admin": ("Admin Approval", render_admin_page),
        "demographic": ("Demographic Review", render_demographic_page),
        "demographic_config": ("Demographic Flag Config", render_demographic_config_page),
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

    inject_global_css(auth_mode=False)

    if not schema_is_ready():
        bootstrap_key = f"rcd_bootstrap_done_{REVIEW_CYCLE_SCHEMA}"
        if not st.session_state.get(bootstrap_key):
            database = env("SNOWFLAKE_DATABASE") or "(database not set)"
            with st.spinner(f"Setting up Review Cycle schema `{database}.{REVIEW_CYCLE_SCHEMA}`..."):
                try:
                    result = bootstrap_database()
                    st.session_state[bootstrap_key] = True
                    st.success(
                        f"Initialized **{result['database']}.{result['schema']}** "
                        f"with **{result['projects_seeded']}** project(s) from APP_CONFIG."
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(
                        f"Could not initialize `{database}.{REVIEW_CYCLE_SCHEMA}`. "
                        f"Check Snowflake grants and APP_CONFIG_SCHEMA in .env. Detail: {exc}"
                    )
                    if st.button("← Back to portal selection"):
                        st.query_params["page"] = "portal_select"
                        st.rerun()
                    return
        else:
            st.error(
                f"Review Cycle schema `{REVIEW_CYCLE_SCHEMA}` is not ready. "
                "A super admin can run **Initialize / migrate database schema** from Sync & Admin."
            )
            if st.button("← Back to portal selection"):
                st.query_params["page"] = "portal_select"
                st.rerun()
            return

    try:
        if not st.session_state.get("schema_migrations_applied"):
            ensure_migrations()
            st.session_state["schema_migrations_applied"] = True
            st.session_state["timestamp_columns_repaired"] = True
        elif not st.session_state.get("timestamp_columns_repaired"):
            repair_timestamp_columns()
            st.session_state["timestamp_columns_repaired"] = True
    except Exception as exc:
        st.session_state["schema_migration_error"] = str(exc)

    pages = allowed_pages(role)
    labels = [PAGE_HANDLERS[key][0] for key in pages if key in PAGE_HANDLERS]
    keys = [key for key in pages if key in PAGE_HANDLERS]
    if not keys:
        st.error("No Review Cycle pages are configured for your role.")
        if st.button("← Back to portal selection"):
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
                st.query_params["page"] = "portal_select"
                st.rerun()
        if st.button("Sign out", use_container_width=True, type="secondary"):
            from authentication.auth import logout

            logout()

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
