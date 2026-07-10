"""Reference-style action buttons (presentation wrappers around Streamlit buttons)."""

from __future__ import annotations

import streamlit as st


def primary_button(label: str, *, key: str | None = None, use_container_width: bool = False) -> bool:
    """Primary action — Streamlit callback preserved, reference styling via CSS."""
    return st.button(
        label,
        key=key,
        type="primary",
        use_container_width=use_container_width,
    )


def secondary_button(label: str, *, key: str | None = None, use_container_width: bool = False) -> bool:
    """Secondary action — Streamlit callback preserved, reference styling via CSS."""
    return st.button(
        label,
        key=key,
        type="secondary",
        use_container_width=use_container_width,
    )


def ghost_button(label: str, *, key: str | None = None, use_container_width: bool = False) -> bool:
    """Tertiary/ghost action."""
    return st.button(
        label,
        key=key,
        type="secondary",
        use_container_width=use_container_width,
    )
