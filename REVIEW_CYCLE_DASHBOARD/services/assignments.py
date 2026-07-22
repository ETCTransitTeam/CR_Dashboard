"""Smart assignment engine.

Builds priority-ordered work queues and assigns batches of records to cleaners
or reviewers. Priority blends flag severity, record age, and whether a record was
already reviewed but still fails its checks (those sink to the bottom).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA
from core.data_access import (
    assign_records,
    defer_assignment,
    load_assignments,
    load_combined_checks,
    load_records,
    release_assignments,
    unassign_records,
)
from core.od_users import cleaning_assignee_options as od_cleaning_assignee_options
from core.od_users import team_members as od_team_members
from core.snowflake_conn import fetch_df

__all__ = [
    "assign_records",
    "defer_assignment",
    "release_assignments",
    "unassign_records",
    "reviewer_load",
    "build_priority_queue",
    "pull_next",
    "active_record_ids",
    "team_members",
    "cleaning_assignee_options",
    "complete_assignment",
    "set_assignment_status",
]


def team_members(team: str) -> list[str]:
    """Active staff (display name) who staff a given workflow team (from OD user_table)."""
    return od_team_members(team)


def cleaning_assignee_options(*, include_privileged: bool = False) -> list[str]:
    """Cleaning assign-to roster; privileged = admins/super admins (super-admin UI only)."""
    return od_cleaning_assignee_options(include_privileged=include_privileged)


def set_assignment_status(project_name: str, record_id: str, status: str, team: str | None = None) -> int:
    clauses = ["PROJECT_NAME = %s", "RECORD_ID = %s", "STATUS = 'assigned'"]
    params: list[Any] = [project_name, str(record_id)]
    if team:
        clauses.append("TEAM = %s")
        params.append(team)
    from core.snowflake_conn import execute

    execute(
        f"UPDATE {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS SET STATUS = %s WHERE {' AND '.join(clauses)}",
        tuple([status] + params),
    )
    return 1


def complete_assignment(project_name: str, record_id: str, team: str = "cleaning") -> int:
    return set_assignment_status(project_name, record_id, "completed", team=team)


def active_record_ids(project_name: str, team: str | None = None) -> set[str]:
    """Record IDs that currently have an active (assigned) assignment."""
    clauses = ["PROJECT_NAME = %s", "STATUS = 'assigned'"]
    params: list[Any] = [project_name]
    if team:
        clauses.append("TEAM = %s")
        params.append(team)
    df = fetch_df(
        f"SELECT RECORD_ID FROM {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS WHERE {' AND '.join(clauses)}",
        tuple(params),
    )
    if df.empty:
        return set()
    return set(df["RECORD_ID"].astype(str))


def reviewer_load(project_name: str, team: str | None = None) -> pd.DataFrame:
    """Count of active assignments per user."""
    clauses = ["PROJECT_NAME = %s", "STATUS = 'assigned'"]
    params: list[Any] = [project_name]
    if team:
        clauses.append("TEAM = %s")
        params.append(team)
    return fetch_df(
        f"""
        SELECT ASSIGNED_TO, COUNT(*) AS OPEN_RECORDS
        FROM {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
        WHERE {' AND '.join(clauses)}
        GROUP BY ASSIGNED_TO
        ORDER BY OPEN_RECORDS DESC
        """,
        tuple(params),
    )


def build_priority_queue(
    project_name: str,
    limit: int | None = None,
    exclude_active: bool = True,
    team: str = "review",
) -> pd.DataFrame:
    """Return flagged records ordered by review priority.

    Priority score (higher = reviewed sooner):
      + flag severity (SUM_ALL_CHECKS)
      + record age in days (older records first)
      - large penalty if already 2X-reviewed yet still failing checks
    """
    checks = load_combined_checks(project_name)
    if checks.empty:
        return pd.DataFrame()
    checks = checks[pd.to_numeric(checks["SUM_ALL_CHECKS"], errors="coerce").fillna(0) > 0].copy()
    if checks.empty:
        return pd.DataFrame()

    records = load_records(project_name)
    if not records.empty:
        meta = records[["RECORD_ID", "FINAL_USAGE", "INGESTED_AT", "ROUTE_SURVEYED_CODE", "INTERV_INIT"]].copy()
        checks = checks.merge(meta, on="RECORD_ID", how="left")

    if exclude_active:
        active = active_record_ids(project_name, team=team)
        if active:
            checks = checks[~checks["RECORD_ID"].astype(str).isin(active)]
    if checks.empty:
        return pd.DataFrame()

    severity = pd.to_numeric(checks["SUM_ALL_CHECKS"], errors="coerce").fillna(0)
    ingested = pd.to_datetime(checks.get("INGESTED_AT"), errors="coerce")
    age_days = (datetime.utcnow() - ingested).dt.days.fillna(0).clip(lower=0)

    already_reviewed = (
        checks.get("TWO_X_REVIEWED_FLAG").notna()
        & (checks.get("TWO_X_REVIEWED_FLAG").astype(str).str.strip() != "")
        if "TWO_X_REVIEWED_FLAG" in checks.columns
        else pd.Series(False, index=checks.index)
    )

    checks["PRIORITY_SCORE"] = severity * 10 + age_days - already_reviewed.astype(int) * 100000
    checks = checks.sort_values("PRIORITY_SCORE", ascending=False)
    if limit:
        checks = checks.head(limit)
    return checks


def pull_next(
    project_name: str,
    assigned_to: str,
    count: int,
    team: str = "review",
    priority_base: int = 100,
) -> list[str]:
    """Assign the next ``count`` highest-priority unassigned records to a user."""
    queue = build_priority_queue(project_name, limit=count, exclude_active=True, team=team)
    if queue.empty:
        return []
    ids = queue["RECORD_ID"].astype(str).tolist()
    for offset, record_id in enumerate(ids):
        assign_records(project_name, [record_id], assigned_to, team=team, priority=priority_base + offset)
    return ids
