"""Uniform field-level audit trail.

Every mutation a user makes to a record should go through this service so that
each changed field produces a DECISION_HISTORY row (old/new/actor/role/when).
That history powers the record tooltip, the Record History page, and analytics.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA
from core.data_access import _load_record_uncached
from core.snowflake_conn import execute, fetch_df

HISTORY_COLUMN_HELP = (
    "Decision timeline: e.g. Kesar 1/20/2026 2:00:00am - Use ; Tosia 2/1/2026 9:05:00am - Remove"
)

EMPTY_HISTORY_TOOLTIP = "No decision history yet."


def _format_history_timestamp(value: Any) -> str:
    """Match product format: ``1/20/2026 2:00:00am`` (no leading zeros, no space before am/pm)."""
    ts = pd.to_datetime(value)
    if pd.isna(ts):
        return ""
    hour12 = ts.hour % 12 or 12
    ampm = "am" if ts.hour < 12 else "pm"
    return f"{ts.month}/{ts.day}/{ts.year} {hour12}:{ts.minute:02d}:{ts.second:02d}{ampm}"


def _format_history_detail(row: pd.Series) -> str:
    """Prefer the new value (Use/Remove/etc.); fall back to action."""
    new_value = row.get("NEW_VALUE")
    if new_value is not None and not (isinstance(new_value, float) and pd.isna(new_value)):
        text = str(new_value).strip()
        if text:
            return text
    action = str(row.get("ACTION") or "").strip()
    if action:
        return action
    field = str(row.get("FIELD_NAME") or "").strip()
    return field or "updated"


def coerce_bool(value: Any) -> bool:
    """Normalize Snowflake/pandas boolean values."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("true", "1", "yes", "t")


def coerce_bool_series(series: pd.Series) -> pd.Series:
    return series.map(coerce_bool)

# Maps Elvis_Review payload field names to typed RECORDS columns so they stay
# queryable alongside the JSON payload.
FIELD_TO_COLUMN: dict[str, str] = {
    "Final_Usage": "FINAL_USAGE",
    "FINAL_REVIEWER": "FINAL_REVIEWER",
    "SUPERVISOR_COMMENT": "SUPERVISOR_COMMENT",
    "1st Cleaner": "FIRST_CLEANER",
    "REASON FOR REMOVAL": "REASON_FOR_REMOVAL",
    "ROUTE_SURVEYEDCode": "ROUTE_SURVEYED_CODE",
    "INTERV_INIT": "INTERV_INIT",
    "ElvisStatus": "ELVIS_STATUS",
}


def _parse_payload(value: Any) -> dict[str, Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _norm(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _invalidate_read_cache() -> None:
    try:
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
    except Exception:
        pass


def log_changes(
    project_name: str,
    record_id: str,
    changes: Iterable[tuple[str, Any, Any]],
    actor: str,
    actor_role: str,
    action: str,
) -> int:
    """Persist a batch of (field, old, new) changes to DECISION_HISTORY."""
    changes = list(changes)
    if not changes:
        return 0
    values_sql = ", ".join(
        ["(%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)"] * len(changes)
    )
    params: list[Any] = []
    for field, old_value, new_value in changes:
        params.extend(
            [
                project_name,
                record_id,
                field,
                _norm(old_value)[:4000],
                _norm(new_value)[:4000],
                action,
                actor,
                actor_role,
            ]
        )
    execute(
        f"""
        INSERT INTO {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        (PROJECT_NAME, RECORD_ID, FIELD_NAME, OLD_VALUE, NEW_VALUE, ACTION, ACTOR, ACTOR_ROLE, CREATED_AT)
        VALUES {values_sql}
        """,
        tuple(params),
    )
    return len(changes)


def apply_record_update(
    project_name: str,
    record_id: str,
    field_updates: dict[str, Any],
    actor: str,
    actor_role: str,
    action: str = "Edit",
    *,
    editable_only: bool = True,
) -> int:
    """Apply edits to a record's payload + typed columns and log every change.

    Returns the number of fields that actually changed.
    """
    current = _load_record_uncached(project_name, record_id)
    if current.empty:
        return 0
    row = current.iloc[0]
    payload = _parse_payload(row.get("RECORD_PAYLOAD"))
    if editable_only:
        from views.record_fields import filter_editable_updates

        field_updates = filter_editable_updates(field_updates)

    changes: list[tuple[str, Any, Any]] = []
    column_updates: dict[str, Any] = {}
    for field, new_value in field_updates.items():
        old_value = payload.get(field)
        if _norm(old_value) == _norm(new_value):
            continue
        payload[field] = new_value
        changes.append((field, old_value, new_value))
        col = FIELD_TO_COLUMN.get(field)
        if col:
            column_updates[col] = new_value

    if not changes:
        return 0

    set_parts = [
        "RECORD_PAYLOAD = TRY_PARSE_JSON(%s)",
        "UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ",
        "UPDATED_BY = %s",
        "IS_NEW = FALSE",
    ]
    params: list[Any] = [json.dumps(payload, default=str), actor]
    for col, val in column_updates.items():
        set_parts.append(f"{col} = %s")
        params.append(val)
    params.extend([project_name, record_id])
    execute(
        f"""
        UPDATE {REVIEW_CYCLE_SCHEMA}.RECORDS
        SET {', '.join(set_parts)}
        WHERE PROJECT_NAME = %s AND RECORD_ID = %s
        """,
        tuple(params),
    )
    log_changes(project_name, record_id, changes, actor, actor_role, action)
    _invalidate_read_cache()
    return len(changes)


def set_final_usage(
    project_name: str,
    record_id: str,
    final_usage: str,
    actor: str,
    actor_role: str,
    action: str | None = None,
    reason: str | None = None,
) -> int:
    updates: dict[str, Any] = {"Final_Usage": final_usage}
    if reason is not None:
        updates["REASON FOR REMOVAL"] = reason
    return apply_record_update(
        project_name,
        record_id,
        updates,
        actor,
        actor_role,
        action=action or final_usage,
    )


def set_admin_approved(
    project_name: str,
    record_id: str,
    actor: str,
    actor_role: str,
    approved: bool = True,
    note: str | None = None,
) -> None:
    from core.schema import _ensure_combined_checks_escalated

    _ensure_combined_checks_escalated()
    current = fetch_df(
        f"SELECT 1 FROM {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS WHERE PROJECT_NAME = %s AND RECORD_ID = %s",
        (project_name, record_id),
    )
    if current.empty:
        execute(
            f"""
            INSERT INTO {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
            (PROJECT_NAME, RECORD_ID, SUM_ALL_CHECKS, ADMIN_APPROVED, ESCALATED, UPDATED_AT)
            VALUES (%s, %s, 0, %s, FALSE, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
            """,
            (project_name, record_id, approved),
        )
    else:
        execute(
            f"""
            UPDATE {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
            SET ADMIN_APPROVED = %s,
                ESCALATED = CASE WHEN %s THEN FALSE ELSE ESCALATED END,
                UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            WHERE PROJECT_NAME = %s AND RECORD_ID = %s
            """,
            (approved, approved, project_name, record_id),
        )
    log_changes(
        project_name,
        record_id,
        [("ADMIN_APPROVED", (not approved), approved)],
        actor,
        actor_role,
        action=(note or ("Admin-Approve" if approved else "Admin-Reject")),
    )
    _invalidate_read_cache()


def set_two_x_review(
    project_name: str,
    record_id: str,
    reviewer: str,
    flag: str,
    actor: str,
    actor_role: str,
) -> None:
    from core.schema import repair_combined_checks_reviewer_columns

    repair_combined_checks_reviewer_columns()
    reviewer_text = str(reviewer or "").strip()
    flag_text = str(flag or "").strip()

    current = fetch_df(
        f"SELECT 1 FROM {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS WHERE PROJECT_NAME = %s AND RECORD_ID = %s",
        (project_name, record_id),
    )
    if current.empty:
        execute(
            f"""
            INSERT INTO {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
            (PROJECT_NAME, RECORD_ID, SUM_ALL_CHECKS, ADMIN_APPROVED, ESCALATED,
             TWO_X_REVIEWED_BY, TWO_X_REVIEWED_FLAG, UPDATED_AT)
            VALUES (%s, %s, 0, FALSE, FALSE, %s, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
            """,
            (project_name, record_id, reviewer_text or None, flag_text or None),
        )
    else:
        execute(
            f"""
            UPDATE {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
            SET TWO_X_REVIEWED_BY = %s,
                TWO_X_REVIEWED_FLAG = %s,
                UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            WHERE PROJECT_NAME = %s AND RECORD_ID = %s
            """,
            (reviewer_text or None, flag_text or None, project_name, record_id),
        )
    log_changes(
        project_name,
        record_id,
        [("2x_REVIEWED_FLAG", "", flag_text), ("2x_REVIEWED_BY", "", reviewer_text)],
        actor,
        actor_role,
        action="2X-Review",
    )
    _invalidate_read_cache()


def load_escalated_record_ids(project_name: str) -> set[str]:
    """Record IDs whose most recent Escalate action is still active (NEW_VALUE=yes)."""
    history = fetch_df(
        f"""
        SELECT RECORD_ID, NEW_VALUE, CREATED_AT
        FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s AND ACTION = 'Escalate'
        ORDER BY CREATED_AT DESC
        """,
        (project_name,),
    )
    if history.empty:
        return set()
    history = history.sort_values("CREATED_AT", ascending=False)
    latest = history.drop_duplicates(subset=["RECORD_ID"], keep="first")
    active = latest[latest["NEW_VALUE"].astype(str).str.strip().str.lower() == "yes"]
    return set(active["RECORD_ID"].astype(str).tolist())


def sync_escalated_flags_from_history(project_name: str) -> int:
    """Backfill COMBINED_CHECKS.ESCALATED for escalations logged before the column existed."""
    from core.schema import _ensure_combined_checks_escalated

    _ensure_combined_checks_escalated()
    ids = load_escalated_record_ids(project_name)
    if not ids:
        return 0
    id_list = sorted(ids)
    placeholders = ", ".join(["%s"] * len(id_list))
    existing = fetch_df(
        f"""
        SELECT RECORD_ID, ESCALATED FROM {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
        WHERE PROJECT_NAME = %s AND RECORD_ID IN ({placeholders})
        """,
        (project_name, *id_list),
    )
    existing_ids = set(existing["RECORD_ID"].astype(str)) if not existing.empty else set()
    missing = [rid for rid in id_list if rid not in existing_ids]
    updated = 0
    if missing:
        params: list = []
        values_sql = []
        for rid in missing:
            values_sql.append("(%s, %s, 0, FALSE, TRUE, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)")
            params.extend([project_name, rid])
        execute(
            f"""
            INSERT INTO {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
            (PROJECT_NAME, RECORD_ID, SUM_ALL_CHECKS, ADMIN_APPROVED, ESCALATED, UPDATED_AT)
            VALUES {', '.join(values_sql)}
            """,
            tuple(params),
        )
        updated += len(missing)
    if not existing.empty:
        needs_flag = existing[
            ~existing["ESCALATED"].map(lambda v: coerce_bool(v) if v is not None else False)
        ]
        if not needs_flag.empty:
            reset_ids = needs_flag["RECORD_ID"].astype(str).tolist()
            ph = ", ".join(["%s"] * len(reset_ids))
            execute(
                f"""
                UPDATE {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
                SET ESCALATED = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                WHERE PROJECT_NAME = %s AND RECORD_ID IN ({ph})
                """,
                (project_name, *reset_ids),
            )
            updated += len(reset_ids)
    return updated


def repair_admin_approved_flags(project_name: str) -> int:
    """Reset falsely approved rows (NaN ingest bug) where no admin-approve history exists."""
    count_df = fetch_df(
        f"""
        SELECT COUNT(*) AS CNT FROM {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS cc
        WHERE cc.PROJECT_NAME = %s
          AND cc.ADMIN_APPROVED = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY dh
              WHERE dh.PROJECT_NAME = cc.PROJECT_NAME
                AND dh.RECORD_ID = cc.RECORD_ID
                AND dh.FIELD_NAME = 'ADMIN_APPROVED'
                AND dh.ACTION IN ('Admin-Approve', 'Admin-Reject')
          )
        """,
        (project_name,),
    )
    to_reset = int(count_df.iloc[0]["CNT"]) if not count_df.empty else 0
    if to_reset <= 0:
        return 0
    execute(
        f"""
        UPDATE {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS cc
        SET ADMIN_APPROVED = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        WHERE cc.PROJECT_NAME = %s
          AND cc.ADMIN_APPROVED = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY dh
              WHERE dh.PROJECT_NAME = cc.PROJECT_NAME
                AND dh.RECORD_ID = cc.RECORD_ID
                AND dh.FIELD_NAME = 'ADMIN_APPROVED'
                AND dh.ACTION IN ('Admin-Approve', 'Admin-Reject')
          )
        """,
        (project_name,),
    )
    return to_reset


def load_history(project_name: str, record_id: str | None = None) -> pd.DataFrame:
    if record_id is not None:
        return fetch_df(
            f"""
            SELECT * FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
            WHERE PROJECT_NAME = %s AND RECORD_ID = %s
            ORDER BY CREATED_AT ASC
            """,
            (project_name, record_id),
        )
    return fetch_df(
        f"""
        SELECT * FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s
        ORDER BY CREATED_AT DESC
        """,
        (project_name,),
    )


def load_field_history_for_records(
    project_name: str,
    record_ids: list[str],
    field_names: list[str],
    *,
    actor_roles: list[str] | None = None,
    empty_message: str = EMPTY_HISTORY_TOOLTIP,
) -> dict[str, dict[str, str]]:
    """Per-record, per-field formatted history for grid cell tooltips."""
    if not record_ids or not field_names:
        return {}
    placeholders = ", ".join(["%s"] * len(record_ids))
    role_clause = ""
    params: list = [project_name, *record_ids]
    if actor_roles:
        role_placeholders = ", ".join(["%s"] * len(actor_roles))
        role_clause = f" AND ACTOR_ROLE IN ({role_placeholders})"
        params.extend(actor_roles)
    history = fetch_df(
        f"""
        SELECT * FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s AND RECORD_ID IN ({placeholders}){role_clause}
        ORDER BY RECORD_ID, CREATED_AT ASC
        """,
        tuple(params),
    )
    empty_text = empty_message or EMPTY_HISTORY_TOOLTIP
    out: dict[str, dict[str, str]] = {str(rid): {} for rid in record_ids}
    if history.empty:
        for rid in out:
            out[rid] = {field: empty_text for field in field_names}
        return out

    for rid, group in history.groupby(history["RECORD_ID"].astype(str), sort=False):
        rid = str(rid)
        out.setdefault(rid, {})
        for field in field_names:
            out[rid][field] = format_field_history_tooltip(group, field, empty_message=empty_text)
    for rid in list(out):
        for field in field_names:
            out[rid].setdefault(field, empty_text)
    return out


def _history_rows_for_field(history_df: pd.DataFrame, field_name: str) -> pd.DataFrame:
    if history_df is None or history_df.empty:
        return pd.DataFrame()
    if field_name == "Final_Usage":
        mask = (history_df["FIELD_NAME"].astype(str) == "Final_Usage") | (
            history_df["ACTION"].astype(str).isin(["Use", "Remove"])
        )
        return history_df[mask]
    return history_df[history_df["FIELD_NAME"].astype(str) == field_name]


def format_field_history_tooltip(
    history_df: pd.DataFrame,
    field_name: str,
    *,
    empty_message: str = EMPTY_HISTORY_TOOLTIP,
) -> str:
    """Timeline for one field, e.g. Final_Usage: ``Kesar 1/20/2026 2:00:00am - Use ; ...``."""
    subset = _history_rows_for_field(history_df, field_name)
    if subset.empty:
        return empty_message or EMPTY_HISTORY_TOOLTIP
    return format_history_tooltip(subset)


def load_history_for_records(
    project_name: str,
    record_ids: list[str],
    *,
    actor_roles: list[str] | None = None,
    empty_message: str = EMPTY_HISTORY_TOOLTIP,
) -> dict[str, str]:
    """Batch-load formatted history tooltips for a set of record IDs."""
    if not record_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(record_ids))
    role_clause = ""
    params: list = [project_name, *record_ids]
    if actor_roles:
        role_placeholders = ", ".join(["%s"] * len(actor_roles))
        role_clause = f" AND ACTOR_ROLE IN ({role_placeholders})"
        params.extend(actor_roles)
    history = fetch_df(
        f"""
        SELECT * FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s AND RECORD_ID IN ({placeholders}){role_clause}
        ORDER BY RECORD_ID, CREATED_AT ASC
        """,
        tuple(params),
    )
    empty_text = empty_message or EMPTY_HISTORY_TOOLTIP
    if history.empty:
        return {str(rid): empty_text for rid in record_ids}
    out: dict[str, str] = {}
    for rid, group in history.groupby(history["RECORD_ID"].astype(str), sort=False):
        out[str(rid)] = format_history_tooltip(group)
    for rid in record_ids:
        out.setdefault(str(rid), empty_text)
    return out


def set_combined_check_fields(
    project_name: str,
    record_id: str,
    updates: dict[str, Any],
    actor: str,
    actor_role: str,
) -> int:
    """Update COMBINED_CHECKS reviewer/admin fields and log changes."""
    from core.schema import repair_combined_checks_reviewer_columns

    repair_combined_checks_reviewer_columns()
    current = fetch_df(
        f"SELECT * FROM {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS WHERE PROJECT_NAME = %s AND RECORD_ID = %s",
        (project_name, record_id),
    )
    if current.empty:
        return 0
    row = current.iloc[0]
    changes: list[tuple[str, Any, Any]] = []
    set_parts: list[str] = []
    params: list[Any] = []

    field_map = {
        "ADMIN_APPROVED": "ADMIN_APPROVED",
        "2x_REVIEWED_BY": "TWO_X_REVIEWED_BY",
        "2x_REVIEWED_FLAG": "TWO_X_REVIEWED_FLAG",
    }
    for ui_field, db_col in field_map.items():
        if ui_field not in updates:
            continue
        new_val = updates[ui_field]
        if ui_field == "ADMIN_APPROVED":
            new_val = bool(new_val)
        elif ui_field in ("2x_REVIEWED_BY", "2x_REVIEWED_FLAG"):
            new_val = str(new_val or "").strip() or None
        old_val = row.get(db_col)
        if _norm(old_val) == _norm(new_val):
            continue
        set_parts.append(f"{db_col} = %s")
        params.append(new_val)
        changes.append((ui_field, old_val, new_val))

    if not changes:
        return 0

    set_parts.append("UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ")
    params.extend([project_name, record_id])
    execute(
        f"""
        UPDATE {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
        SET {', '.join(set_parts)}
        WHERE PROJECT_NAME = %s AND RECORD_ID = %s
        """,
        tuple(params),
    )
    log_changes(project_name, record_id, changes, actor, actor_role, action="Edit-Checks")
    _invalidate_read_cache()
    return len(changes)


def set_escalated(
    project_name: str,
    record_id: str,
    actor: str,
    actor_role: str,
    escalated: bool = True,
) -> None:
    from core.schema import _ensure_combined_checks_escalated

    _ensure_combined_checks_escalated()
    current = fetch_df(
        f"SELECT 1 FROM {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS WHERE PROJECT_NAME = %s AND RECORD_ID = %s",
        (project_name, record_id),
    )
    if current.empty:
        execute(
            f"""
            INSERT INTO {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
            (PROJECT_NAME, RECORD_ID, SUM_ALL_CHECKS, ADMIN_APPROVED, ESCALATED, UPDATED_AT)
            VALUES (%s, %s, 0, FALSE, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
            """,
            (project_name, record_id, escalated),
        )
    else:
        execute(
            f"""
            UPDATE {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
            SET ESCALATED = %s, UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            WHERE PROJECT_NAME = %s AND RECORD_ID = %s
            """,
            (escalated, project_name, record_id),
        )
    log_changes(
        project_name,
        record_id,
        [("escalated", "", "yes" if escalated else "no")],
        actor,
        actor_role,
        action="Escalate",
    )
    _invalidate_read_cache()


def format_history_tooltip(history_df: pd.DataFrame) -> str:
    """Cumulative timeline: ``Kesar 1/20/2026 2:00:00am - Use ; Tosia 2/1/2026 9:05:00am - Remove``."""
    if history_df is None or history_df.empty:
        return EMPTY_HISTORY_TOOLTIP

    view = history_df.copy()
    if "CREATED_AT" in view.columns:
        view = view.sort_values("CREATED_AT", ascending=True, kind="mergesort")

    parts: list[str] = []
    for _, row in view.iterrows():
        actor = str(row.get("ACTOR") or "Unknown").strip() or "Unknown"
        ts = _format_history_timestamp(row.get("CREATED_AT"))
        detail = _format_history_detail(row)
        if ts:
            parts.append(f"{actor} {ts} - {detail}")
        else:
            parts.append(f"{actor} - {detail}")
    return " ; ".join(parts) if parts else EMPTY_HISTORY_TOOLTIP
