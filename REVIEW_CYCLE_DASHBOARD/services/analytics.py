"""Manager analytics computed live from Snowflake.

These read RECORDS / COMBINED_CHECKS / DECISION_HISTORY directly so the numbers
reflect the current state of the database rather than a stale pipeline snapshot.
"""

from __future__ import annotations

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA
from core.data_access import load_records
from core.streamlit_cache import fetch_df_cached

GROUP_LABELS = {
    "ROUTE_SURVEYED_CODE": "Route",
    "INTERV_INIT": "Interviewer",
    "FINAL_REVIEWER": "Reviewer",
    "FIRST_CLEANER": "1st Cleaner",
}


def _is_remove(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower().eq("remove")


def removal_by(
    project_name: str,
    group_col: str,
    records: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Removal counts and rate grouped by a record column."""
    if records is None:
        records = load_records(project_name)
    if records.empty or group_col not in records.columns:
        return pd.DataFrame(columns=[group_col, "TOTAL", "REMOVED", "REMOVAL_RATE"])
    df = records.copy()
    df["_REMOVED"] = _is_remove(df["FINAL_USAGE"]).astype(int)
    df[group_col] = df[group_col].fillna("(blank)").astype(str)
    grouped = (
        df.groupby(group_col)
        .agg(TOTAL=("RECORD_ID", "count"), REMOVED=("_REMOVED", "sum"))
        .reset_index()
    )
    grouped["REMOVAL_RATE"] = (grouped["REMOVED"] / grouped["TOTAL"] * 100).round(2)
    return grouped.sort_values("REMOVAL_RATE", ascending=False)


def removal_band(rate: float, low: float = 5.0, mid: float = 10.0, high: float = 15.0) -> str:
    if rate >= high:
        return "15%+"
    if rate >= mid:
        return "10%+"
    if rate >= low:
        return "5%+"
    return "<5%"


def add_bands(df: pd.DataFrame, rate_col: str = "REMOVAL_RATE") -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["BAND"] = out[rate_col].apply(removal_band)
    return out


def reviewer_override_rate(project_name: str) -> pd.DataFrame:
    """How often each actor changed Final_Usage, and how often to Remove."""
    history = fetch_df_cached(
        f"""
        SELECT ACTOR, NEW_VALUE
        FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s AND FIELD_NAME = 'Final_Usage'
        """,
        (project_name,),
    )
    if history.empty:
        return pd.DataFrame(columns=["ACTOR", "CHANGES", "REMOVED", "OVERRIDE_RATE"])
    history["_REMOVED"] = _is_remove(history["NEW_VALUE"]).astype(int)
    grouped = (
        history.groupby("ACTOR")
        .agg(CHANGES=("NEW_VALUE", "count"), REMOVED=("_REMOVED", "sum"))
        .reset_index()
    )
    grouped["OVERRIDE_RATE"] = (grouped["REMOVED"] / grouped["CHANGES"] * 100).round(2)
    return grouped.sort_values("CHANGES", ascending=False)


def cleaner_modification_rate(project_name: str) -> pd.DataFrame:
    """Field-change volume per cleaner (records touched and total field edits)."""
    history = fetch_df_cached(
        f"""
        SELECT ACTOR, RECORD_ID
        FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s AND ACTOR_ROLE = 'cleaning'
        """,
        (project_name,),
    )
    if history.empty:
        return pd.DataFrame(columns=["ACTOR", "FIELD_EDITS", "RECORDS_TOUCHED"])
    grouped = (
        history.groupby("ACTOR")
        .agg(FIELD_EDITS=("RECORD_ID", "count"), RECORDS_TOUCHED=("RECORD_ID", "nunique"))
        .reset_index()
    )
    return grouped.sort_values("FIELD_EDITS", ascending=False)


def project_quality_score(project_name: str, records: pd.DataFrame | None = None) -> dict[str, float]:
    if records is None:
        records = load_records(project_name)
    total = len(records)
    if total == 0:
        return {"total": 0, "removed": 0, "removal_rate": 0.0, "quality_score": 100.0}
    removed = int(_is_remove(records["FINAL_USAGE"]).sum())
    removal_rate = round(removed / total * 100, 2)
    return {
        "total": total,
        "removed": removed,
        "removal_rate": removal_rate,
        "quality_score": round(100 - removal_rate, 2),
    }


def status_summary(project_name: str, records: pd.DataFrame | None = None) -> dict[str, int]:
    """Counts used by the Project Dashboard cards."""
    if records is None:
        records = load_records(project_name)
    if records.empty:
        return {"total": 0, "new": 0, "cleaned": 0, "reviewed": 0, "removed": 0, "pending": 0}
    usage = records["FINAL_USAGE"].fillna("").astype(str).str.strip()
    removed = int(usage.str.lower().eq("remove").sum())
    used = int(usage.str.lower().eq("use").sum())
    decided = int((usage != "").sum())
    is_new = int(records["IS_NEW"].fillna(False).astype(bool).sum()) if "IS_NEW" in records.columns else 0
    reviewer = records["FINAL_REVIEWER"].fillna("").astype(str).str.strip() if "FINAL_REVIEWER" in records.columns else pd.Series([], dtype=str)
    reviewed = int((reviewer != "").sum()) if len(reviewer) else 0
    total = len(records)
    return {
        "total": total,
        "new": is_new,
        "cleaned": used,
        "reviewed": reviewed,
        "removed": removed,
        "pending": total - decided,
    }


def weekly_trends(project_name: str) -> pd.DataFrame:
    """Weekly counts of removals and cleaning edits from the audit trail."""
    history = fetch_df_cached(
        f"""
        SELECT CREATED_AT, FIELD_NAME, NEW_VALUE, ACTOR_ROLE
        FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s
        """,
        (project_name,),
    )
    if history.empty:
        return pd.DataFrame(columns=["WEEK", "REMOVALS", "CLEANING_EDITS", "REVIEW_ACTIONS"])
    history["CREATED_AT"] = pd.to_datetime(history["CREATED_AT"], errors="coerce")
    history = history.dropna(subset=["CREATED_AT"])
    history["WEEK"] = history["CREATED_AT"].dt.to_period("W").dt.start_time
    history["_REMOVAL"] = (
        history["FIELD_NAME"].eq("Final_Usage") & _is_remove(history["NEW_VALUE"])
    ).astype(int)
    history["_CLEANING"] = history["ACTOR_ROLE"].eq("cleaning").astype(int)
    history["_REVIEW"] = history["ACTOR_ROLE"].isin(["review", "manager", "admin"]).astype(int)
    trends = (
        history.groupby("WEEK")
        .agg(
            REMOVALS=("_REMOVAL", "sum"),
            CLEANING_EDITS=("_CLEANING", "sum"),
            REVIEW_ACTIONS=("_REVIEW", "sum"),
        )
        .reset_index()
        .sort_values("WEEK")
    )
    return trends


def productivity(project_name: str) -> pd.DataFrame:
    """Weekly action counts per actor (reviewer/cleaner productivity)."""
    history = fetch_df_cached(
        f"""
        SELECT CREATED_AT, ACTOR, ACTOR_ROLE
        FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s
        """,
        (project_name,),
    )
    if history.empty:
        return pd.DataFrame(columns=["WEEK", "ACTOR", "ACTOR_ROLE", "ACTIONS"])
    history["CREATED_AT"] = pd.to_datetime(history["CREATED_AT"], errors="coerce")
    history = history.dropna(subset=["CREATED_AT"])
    history["WEEK"] = history["CREATED_AT"].dt.to_period("W").dt.start_time
    grouped = (
        history.groupby(["WEEK", "ACTOR", "ACTOR_ROLE"])
        .size()
        .reset_index(name="ACTIONS")
        .sort_values("WEEK")
    )
    return grouped
