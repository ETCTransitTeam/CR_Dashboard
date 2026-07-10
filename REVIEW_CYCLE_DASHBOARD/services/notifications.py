"""Notification Center service.

Lightweight per-user notifications backed by REVIEW_CYCLE.NOTIFICATIONS:
new assignment, review completed, admin approval required, sync completed, etc.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA
from core.od_users import admin_recipients
from core.snowflake_conn import execute, fetch_df_optional

# Canonical notification types.
NEW_ASSIGNMENT = "new_assignment"
REVIEW_COMPLETED = "review_completed"
ADMIN_APPROVAL_REQUIRED = "admin_approval_required"
SYNC_COMPLETED = "sync_completed"
DATA_QUALITY_ALERT = "data_quality_alert"


def notify(
    recipient: str,
    ntype: str,
    message: str,
    project_name: str | None = None,
    record_id: str | None = None,
) -> None:
    if not recipient:
        return
    execute(
        f"""
        INSERT INTO {REVIEW_CYCLE_SCHEMA}.NOTIFICATIONS
        (RECIPIENT, NTYPE, MESSAGE, PROJECT_NAME, RECORD_ID, IS_READ)
        VALUES (%s, %s, %s, %s, %s, FALSE)
        """,
        (recipient, ntype, message[:4000], project_name, record_id),
    )


def notify_many(recipients: list[str], ntype: str, message: str, project_name: str | None = None) -> None:
    for recipient in recipients:
        notify(recipient, ntype, message, project_name=project_name)


def unread_count(recipient: str) -> int:
    if not recipient:
        return 0
    df = fetch_df_optional(
        f"SELECT COUNT(*) AS CNT FROM {REVIEW_CYCLE_SCHEMA}.NOTIFICATIONS WHERE RECIPIENT = %s AND IS_READ = FALSE",
        (recipient,),
    )
    if df.empty:
        return 0
    return int(df.iloc[0]["CNT"])


def list_notifications(recipient: str, unread_only: bool = False, limit: int = 100) -> pd.DataFrame:
    clauses = ["RECIPIENT = %s"]
    params: list[Any] = [recipient]
    if unread_only:
        clauses.append("IS_READ = FALSE")
    return fetch_df_optional(
        f"""
        SELECT * FROM {REVIEW_CYCLE_SCHEMA}.NOTIFICATIONS
        WHERE {' AND '.join(clauses)}
        ORDER BY CREATED_AT DESC
        LIMIT {int(limit)}
        """,
        tuple(params),
    )


def mark_read(notification_id: int) -> None:
    execute(
        f"UPDATE {REVIEW_CYCLE_SCHEMA}.NOTIFICATIONS SET IS_READ = TRUE WHERE NOTIFICATION_ID = %s",
        (notification_id,),
    )


def mark_all_read(recipient: str) -> None:
    execute(
        f"UPDATE {REVIEW_CYCLE_SCHEMA}.NOTIFICATIONS SET IS_READ = TRUE WHERE RECIPIENT = %s AND IS_READ = FALSE",
        (recipient,),
    )


def admins() -> list[str]:
    """Display names / emails of super admins and OD ADMIN users."""
    return admin_recipients()
