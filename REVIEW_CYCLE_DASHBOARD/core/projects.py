from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from core.config import APP_CONFIG_SCHEMA, env, fq_table
from core.snowflake_conn import execute, fetch_df

_SYNC_TS_COLUMNS = frozenset({"LAST_PULL_TS", "LAST_OD_SYNC_SEEN", "LAST_KINGELVIS_EXPORT_TS"})

# Keep in sync with authentication.auth.FRONTEND_HIDDEN_PROJECTS (OD Dashboard dropdown).
FRONTEND_HIDDEN_PROJECTS = frozenset({
    "LACMTA_FEEDER",
    "ACTRANSIT",
    "SALEM",
    "PARKCITY",
})


def is_frontend_visible_project(project_name: str) -> bool:
    return (project_name or "").strip() not in FRONTEND_HIDDEN_PROJECTS


def _active_app_config_names() -> set[str] | None:
    """Live active project names from APP_CONFIG (same source as OD Dashboard)."""
    try:
        df = fetch_df(
            f"""
            SELECT PROJECT_NAME
            FROM {fq_table('PROJECT_CONFIGS', APP_CONFIG_SCHEMA)}
            WHERE IS_ACTIVE = TRUE
            """,
            schema="PUBLIC",
        )
    except Exception:
        return None
    if df.empty or "PROJECT_NAME" not in df.columns:
        return set()
    return {str(name).strip() for name in df["PROJECT_NAME"].tolist() if str(name).strip()}


def _filter_od_visible_projects(df: pd.DataFrame) -> pd.DataFrame:
    """Match OD Dashboard dropdown: active in APP_CONFIG and not frontend-hidden."""
    if df.empty or "PROJECT_NAME" not in df.columns:
        return df
    names = df["PROJECT_NAME"].astype(str)
    visible = names.map(is_frontend_visible_project)
    filtered = df.loc[visible].copy()
    app_names = _active_app_config_names()
    if app_names is not None:
        filtered = filtered.loc[
            filtered["PROJECT_NAME"].astype(str).str.strip().isin(app_names)
        ].copy()
    return filtered.reset_index(drop=True)


def list_projects(active_only: bool = True) -> pd.DataFrame:
    from core.streamlit_cache import cached_list_projects, cache_version, in_streamlit_runtime

    if in_streamlit_runtime():
        return cached_list_projects(cache_version(), active_only)
    return _list_projects_uncached(active_only)


def _list_projects_uncached(active_only: bool = True) -> pd.DataFrame:
    where = "WHERE IS_ACTIVE = TRUE" if active_only else ""
    df = fetch_df(
        f"SELECT * FROM {fq_table('PROJECTS')} {where} ORDER BY PROJECT_NAME",
        schema="PUBLIC",
    )
    if active_only:
        return _filter_od_visible_projects(df)
    return df


def get_project(project_name: str | None) -> dict[str, Any] | None:
    if not project_name:
        return None
    from core.streamlit_cache import cached_get_project, cache_version, in_streamlit_runtime

    if in_streamlit_runtime():
        return cached_get_project(cache_version(), project_name)
    return _get_project_uncached(project_name)


def _get_project_uncached(project_name: str) -> dict[str, Any] | None:
    df = fetch_df(
        f"SELECT * FROM {fq_table('PROJECTS')} WHERE PROJECT_NAME = %s",
        (project_name,),
        schema="PUBLIC",
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def get_od_last_sync(project_name: str) -> datetime | None:
    project = get_project(project_name)
    if not project or not project.get("BASE_SCHEMA"):
        return None
    schema = project["BASE_SCHEMA"]
    database = env("SNOWFLAKE_DATABASE")
    try:
        df = fetch_df(
            f"SELECT LAST_SYNC_DATE FROM {database}.{schema}.LAST_SURVEY_DATE LIMIT 1",
            schema="PUBLIC",
        )
    except Exception:
        return None
    if df.empty:
        return None
    value = df.iloc[0]["LAST_SYNC_DATE"]
    if pd.isna(value):
        return None
    return pd.to_datetime(value).to_pydatetime()


def get_sync_state(project_name: str) -> dict[str, Any] | None:
    df = fetch_df(
        f"SELECT * FROM {fq_table('SYNC_STATE')} WHERE PROJECT_NAME = %s",
        (project_name,),
        schema="PUBLIC",
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def _sync_ts_sql(column: str, value: Any) -> tuple[str, list[Any]]:
    if value is None:
        return f"{column} = NULL", []
    if isinstance(value, datetime):
        return f"{column} = TO_TIMESTAMP_NTZ(%s)", [value.strftime("%Y-%m-%d %H:%M:%S")]
    if isinstance(value, pd.Timestamp):
        return f"{column} = TO_TIMESTAMP_NTZ(%s)", [value.strftime("%Y-%m-%d %H:%M:%S")]
    return f"{column} = TO_TIMESTAMP_NTZ(%s)", [str(value)]


def upsert_sync_state(project_name: str, **fields) -> None:
    existing = get_sync_state(project_name)
    if existing is None:
        cols = ["PROJECT_NAME"]
        exprs = ["%s"]
        params: list[Any] = [project_name]
        for key, value in fields.items():
            cols.append(key)
            if key in _SYNC_TS_COLUMNS:
                exprs.append("TO_TIMESTAMP_NTZ(%s)")
                if isinstance(value, (datetime, pd.Timestamp)):
                    params.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    params.append(str(value) if value is not None else None)
            else:
                exprs.append("%s")
                params.append(value)
        cols.append("UPDATED_AT")
        exprs.append("CURRENT_TIMESTAMP()::TIMESTAMP_NTZ")
        execute(
            f"INSERT INTO {fq_table('SYNC_STATE')} ({', '.join(cols)}) VALUES ({', '.join(exprs)})",
            tuple(params),
            schema="PUBLIC",
        )
        return
    set_parts: list[str] = []
    params: list[Any] = []
    for key, value in fields.items():
        if key in _SYNC_TS_COLUMNS:
            clause, clause_params = _sync_ts_sql(key, value)
        else:
            clause, clause_params = f"{key} = %s", [value]
        set_parts.append(clause)
        params.extend(clause_params)
    set_parts.append("UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ")
    params.append(project_name)
    execute(
        f"UPDATE {fq_table('SYNC_STATE')} SET {', '.join(set_parts)} WHERE PROJECT_NAME = %s",
        tuple(params),
        schema="PUBLIC",
    )
