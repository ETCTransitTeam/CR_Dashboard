"""Session-scoped active project for the Review Cycle Dashboard.

One project selection in the sidebar applies to every RCD page for the
current Streamlit session / user.
"""

from __future__ import annotations

import streamlit as st

ACTIVE_PROJECT_KEY = "rcd_active_project"
_PENDING_PROJECT_KEY = "_rcd_pending_active_project"


def list_project_names() -> list[str]:
    from core.projects import list_projects

    projects = list_projects()
    if projects.empty or "PROJECT_NAME" not in projects.columns:
        return []
    return [str(name) for name in projects["PROJECT_NAME"].tolist() if str(name).strip()]


def get_active_project() -> str | None:
    """Return the session-selected project, or the first available project.

    Never writes ``rcd_active_project`` here — that key belongs to the sidebar
    selectbox and Streamlit forbids mutating it after the widget is created.
    """
    names = list_project_names()
    if not names:
        return None
    current = st.session_state.get(ACTIVE_PROJECT_KEY)
    if current in names:
        return str(current)
    return names[0]


def set_active_project(project_name: str | None) -> None:
    """Request an active-project change.

    If the sidebar selectbox has already been rendered this run, the change is
    deferred to the next run via a pending key (applied before the widget).
    """
    if not project_name:
        return
    names = list_project_names()
    if project_name not in names:
        return
    if st.session_state.get(ACTIVE_PROJECT_KEY) == project_name:
        return
    # Always stage via pending so we never fight the instantiated widget key.
    st.session_state[_PENDING_PROJECT_KEY] = project_name


def render_sidebar_project_selector() -> str | None:
    """Render the shared project dropdown in the left sidebar."""
    names = list_project_names()
    st.markdown('<div class="ref-sidebar-project-heading">Active project</div>', unsafe_allow_html=True)
    if not names:
        st.caption("No projects configured.")
        return None

    # Apply deferred selection BEFORE the selectbox is created.
    pending = st.session_state.pop(_PENDING_PROJECT_KEY, None)
    if pending in names:
        st.session_state[ACTIVE_PROJECT_KEY] = pending
    elif st.session_state.get(ACTIVE_PROJECT_KEY) not in names:
        st.session_state[ACTIVE_PROJECT_KEY] = names[0]

    selected = st.selectbox(
        "Active project",
        options=names,
        key=ACTIVE_PROJECT_KEY,
        label_visibility="collapsed",
        help="Applies to every Review Cycle page for this session.",
    )
    return str(selected) if selected else None


def require_active_project(*, empty_title: str = "No project selected") -> str | None:
    """Return active project or show a short empty state and None."""
    from views.ui import empty_state

    project = get_active_project()
    if project:
        return project
    empty_state(
        empty_title,
        "Choose a project in the left sidebar. The selection applies to all Review Cycle pages.",
    )
    return None
