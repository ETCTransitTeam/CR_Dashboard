"""Streamlit read-through cache for Snowflake queries and heavy transforms.

Mutations should call ``bump_data_cache()`` so the next rerun sees fresh data.
Non-Streamlit entry points (validate.py, CLI pipeline) bypass this module.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

# Fallback TTL if cache version is not bumped after a write.
_DEFAULT_TTL = 300
_STATIC_TTL = 600


def in_streamlit_runtime() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


def cache_version() -> int:
    if not in_streamlit_runtime():
        return 0
    return int(st.session_state.get("data_cache_version", 0))


def bump_data_cache() -> None:
    """Invalidate cached Snowflake reads after data mutations."""
    if not in_streamlit_runtime():
        return
    st.session_state["data_cache_version"] = cache_version() + 1
    # Drop long-lived read connections so the next SELECT cannot reuse a stale session.
    try:
        from core.snowflake_conn import _invalidate_read_connections

        _invalidate_read_connections()
    except Exception:
        pass
    # Clear every read-through cache so editable grids remount on fresh DB values.
    for cached in (
        cached_fetch_df,
        cached_load_records,
        cached_load_records_for_projects,
        cached_load_combined_checks,
        cached_load_assignments,
        cached_load_record,
        cached_records_to_elvis_review,
    ):
        try:
            cached.clear()
        except Exception:
            pass


@st.cache_data(show_spinner=False, ttl=_DEFAULT_TTL)
def cached_fetch_df(_version: int, query: str, params: tuple[Any, ...], schema: str | None) -> pd.DataFrame:
    from core.snowflake_conn import fetch_df

    return fetch_df(query, list(params) if params else None, schema)


def fetch_df_cached(query: str, params=None, schema: str | None = None) -> pd.DataFrame:
    if in_streamlit_runtime():
        return cached_fetch_df(cache_version(), query, tuple(params or ()), schema)
    from core.snowflake_conn import fetch_df

    return fetch_df(query, params, schema)


@st.cache_data(show_spinner=False, ttl=_DEFAULT_TTL)
def cached_load_records(
    _version: int,
    project_name: str | None,
    only_new: bool,
    final_usage: str | None,
) -> pd.DataFrame:
    from core.data_access import _load_records_uncached

    return _load_records_uncached(project_name, only_new, final_usage)


@st.cache_data(show_spinner=False, ttl=_DEFAULT_TTL)
def cached_load_records_for_projects(
    _version: int,
    project_names: tuple[str, ...],
    only_new: bool,
    final_usage: str | None,
) -> pd.DataFrame:
    from core.data_access import _load_records_for_projects_uncached

    return _load_records_for_projects_uncached(list(project_names), only_new, final_usage)


@st.cache_data(show_spinner=False, ttl=_DEFAULT_TTL)
def cached_load_combined_checks(
    _version: int,
    project_name: str | None,
    flagged_only: bool,
) -> pd.DataFrame:
    from core.data_access import _load_combined_checks_uncached

    return _load_combined_checks_uncached(project_name, flagged_only)


@st.cache_data(show_spinner=False, ttl=_DEFAULT_TTL)
def cached_load_assignments(
    _version: int,
    assigned_to: str | None,
    team: str | None,
    project_name: str | None,
    include_deferred: bool,
) -> pd.DataFrame:
    from core.data_access import _load_assignments_uncached

    return _load_assignments_uncached(assigned_to, team, project_name, include_deferred)


@st.cache_data(show_spinner=False, ttl=_DEFAULT_TTL)
def cached_load_record(_version: int, project_name: str, record_id: str) -> pd.DataFrame:
    from core.data_access import _load_record_uncached

    return _load_record_uncached(project_name, record_id)


@st.cache_data(show_spinner=False, ttl=_STATIC_TTL)
def cached_list_projects(_version: int, active_only: bool) -> pd.DataFrame:
    from core.projects import _list_projects_uncached

    return _list_projects_uncached(active_only)


@st.cache_data(show_spinner=False, ttl=_STATIC_TTL)
def cached_get_project(_version: int, project_name: str) -> dict[str, Any] | None:
    from core.projects import _get_project_uncached

    return _get_project_uncached(project_name)


def clear_project_cache() -> None:
    """Make project additions and configuration changes visible immediately."""
    cached_list_projects.clear()
    cached_get_project.clear()


@st.cache_data(show_spinner=False, ttl=60)
def cached_unread_count(_version: int, recipient: str) -> int:
    from services import notifications as notify_svc

    return notify_svc.unread_count(recipient)


@st.cache_data(show_spinner=False, ttl=60)
def cached_list_notifications(
    _version: int,
    recipient: str,
    unread_only: bool,
    limit: int,
) -> pd.DataFrame:
    from services import notifications as notify_svc

    return notify_svc.list_notifications(recipient, unread_only=unread_only, limit=limit)


@st.cache_data(show_spinner=False, ttl=_DEFAULT_TTL)
def cached_records_to_elvis_review(_version: int, records_hash: str, records: pd.DataFrame) -> pd.DataFrame:
    from core.data_access import _records_to_elvis_review_uncached

    return _records_to_elvis_review_uncached(records)


def records_cache_key(records: pd.DataFrame) -> str:
    """Stable hash for caching transforms keyed on record identity."""
    if records.empty:
        return "empty"
    cols = [c for c in ("PROJECT_NAME", "RECORD_ID", "UPDATED_AT") if c in records.columns]
    if not cols:
        return f"{len(records)}"
    subset = records[cols].astype(str)
    return f"{len(records)}:{pd.util.hash_pandas_object(subset, index=True).sum()}"
