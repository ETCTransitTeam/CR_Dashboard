from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from snowflake.connector.errors import ProgrammingError

from core.config import APP_CONFIG_SCHEMA, REVIEW_CYCLE_SCHEMA, env, fq_table
from core.snowflake_conn import cursor, execute, fetch_df, merge_upsert

BOOTSTRAP_SCHEMA = "PUBLIC"


def _qualified(name: str) -> str:
    database = env("SNOWFLAKE_DATABASE")
    return f"{database}.{REVIEW_CYCLE_SCHEMA}.{name}"


def schema_is_ready() -> bool:
    try:
        with cursor(schema=BOOTSTRAP_SCHEMA) as cur:
            cur.execute(f"SELECT 1 FROM {_qualified('PROJECTS')} LIMIT 1")
        return True
    except ProgrammingError:
        return False
    except Exception:
        return False


def init_schema() -> None:
    database = env("SNOWFLAKE_DATABASE")
    if not database:
        raise RuntimeError("SNOWFLAKE_DATABASE is not set in .env")

    ddl_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {database}.{REVIEW_CYCLE_SCHEMA}",
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("PROJECTS")} (
            PROJECT_NAME VARCHAR(255) NOT NULL PRIMARY KEY,
            BASE_SCHEMA VARCHAR(255),
            ELVIS_DATABASE VARCHAR(255),
            ELVIS_TABLE VARCHAR(255),
            MAIN_DATABASE VARCHAR(255),
            MAIN_TABLE VARCHAR(255),
            DETAILS_FILE_NAME VARCHAR(512),
            CR_FILE_NAME VARCHAR(512),
            KINGELVIS_FILE_NAME VARCHAR(512),
            ELVIS_PROJECT_NAME VARCHAR(255),
            PIPELINE_PROJECT_CODE VARCHAR(255),
            IS_ACTIVE BOOLEAN DEFAULT TRUE,
            SYNCED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("RECORDS")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            RECORD_ID VARCHAR(64) NOT NULL,
            BATCH_ID VARCHAR(64),
            INGESTED_AT TIMESTAMP_NTZ,
            IS_NEW BOOLEAN DEFAULT FALSE,
            FINAL_USAGE VARCHAR(64),
            FINAL_REVIEWER VARCHAR(255),
            ROUTE_SURVEYED_CODE VARCHAR(255),
            INTERV_INIT VARCHAR(64),
            ELVIS_STATUS VARCHAR(255),
            SUPERVISOR_COMMENT VARCHAR(4000),
            FIRST_CLEANER VARCHAR(255),
            REASON_FOR_REMOVAL VARCHAR(2000),
            RECORD_PAYLOAD VARIANT,
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_BY VARCHAR(255),
            PRIMARY KEY (PROJECT_NAME, RECORD_ID)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("COMBINED_CHECKS")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            RECORD_ID VARCHAR(64) NOT NULL,
            BATCH_ID VARCHAR(64),
            TRADITIONAL_CHECK NUMBER DEFAULT 0,
            OD_DISTANCE_CHECK NUMBER DEFAULT 0,
            TRANSFER_DISTANCE_CHECK NUMBER DEFAULT 0,
            STOPLISTVALIDATION_CHECK NUMBER DEFAULT 0,
            TWO_X_REVIEW_CHECK NUMBER DEFAULT 0,
            SUM_ALL_CHECKS NUMBER DEFAULT 0,
            ADMIN_APPROVED BOOLEAN DEFAULT FALSE,
            ESCALATED BOOLEAN DEFAULT FALSE,
            TWO_X_REVIEWED_BY VARCHAR(255),
            TWO_X_REVIEWED_FLAG VARCHAR(255),
            CHECK_PAYLOAD VARIANT,
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (PROJECT_NAME, RECORD_ID)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DECISION_HISTORY")} (
            HISTORY_ID NUMBER AUTOINCREMENT,
            PROJECT_NAME VARCHAR(255) NOT NULL,
            RECORD_ID VARCHAR(64) NOT NULL,
            FIELD_NAME VARCHAR(255) NOT NULL,
            OLD_VALUE VARCHAR(4000),
            NEW_VALUE VARCHAR(4000),
            ACTION VARCHAR(255),
            ACTOR VARCHAR(255),
            ACTOR_ROLE VARCHAR(64),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("ASSIGNMENTS")} (
            ASSIGNMENT_ID NUMBER AUTOINCREMENT,
            PROJECT_NAME VARCHAR(255) NOT NULL,
            RECORD_ID VARCHAR(64) NOT NULL,
            ASSIGNED_TO VARCHAR(255),
            TEAM VARCHAR(64),
            STATUS VARCHAR(64) DEFAULT 'assigned',
            PRIORITY NUMBER DEFAULT 100,
            ASSIGNED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            DEFER_UNTIL TIMESTAMP_NTZ,
            NOTES VARCHAR(2000)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("REVIEWER_STATS")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            STAT_TYPE VARCHAR(128) NOT NULL,
            STAT_KEY VARCHAR(512) NOT NULL,
            METRIC_NAME VARCHAR(255) NOT NULL,
            METRIC_VALUE FLOAT,
            METRIC_TEXT VARCHAR(4000),
            BATCH_ID VARCHAR(64),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("SYNC_STATE")} (
            PROJECT_NAME VARCHAR(255) NOT NULL PRIMARY KEY,
            LAST_PULL_TS TIMESTAMP_NTZ,
            LAST_OD_SYNC_SEEN TIMESTAMP_NTZ,
            LAST_KINGELVIS_EXPORT_TS TIMESTAMP_NTZ,
            LAST_PIPELINE_STATUS VARCHAR(64),
            LAST_PIPELINE_MESSAGE VARCHAR(4000),
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("ORIGINAL_RECORDS")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            RECORD_ID VARCHAR(64) NOT NULL,
            RECORD_PAYLOAD VARIANT,
            CAPTURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (PROJECT_NAME, RECORD_ID)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("NOTIFICATIONS")} (
            NOTIFICATION_ID NUMBER AUTOINCREMENT,
            RECIPIENT VARCHAR(255) NOT NULL,
            NTYPE VARCHAR(64) NOT NULL,
            MESSAGE VARCHAR(4000),
            PROJECT_NAME VARCHAR(255),
            RECORD_ID VARCHAR(64),
            IS_READ BOOLEAN DEFAULT FALSE,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("QUALITY_ALERTS")} (
            ALERT_ID NUMBER AUTOINCREMENT,
            PROJECT_NAME VARCHAR(255) NOT NULL,
            ALERT_TYPE VARCHAR(64) NOT NULL,
            SUBJECT VARCHAR(512),
            METRIC_VALUE FLOAT,
            THRESHOLD FLOAT,
            SEVERITY VARCHAR(32),
            MESSAGE VARCHAR(4000),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_CHECKS")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            RECORD_ID VARCHAR(64) NOT NULL,
            CHECK_TYPE VARCHAR(64) NOT NULL,
            STATUS VARCHAR(32),
            DETAIL VARCHAR(4000),
            CATEGORY VARCHAR(64),
            SEVERITY VARCHAR(32),
            RULE_VERSION NUMBER DEFAULT 1,
            DETAIL_JSON VARIANT,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (PROJECT_NAME, RECORD_ID, CHECK_TYPE)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_FLAG_DEFINITIONS")} (
            FLAG_KEY VARCHAR(128) NOT NULL PRIMARY KEY,
            LABEL VARCHAR(255) NOT NULL,
            CATEGORY VARCHAR(64) NOT NULL,
            DESCRIPTION VARCHAR(4000),
            SEVERITY VARCHAR(32) DEFAULT 'medium',
            RULE_KIND VARCHAR(64) DEFAULT 'declarative',
            DEFAULT_ENABLED BOOLEAN DEFAULT TRUE,
            DEFAULT_PARAMS VARIANT,
            MESSAGE_TEMPLATE VARCHAR(4000),
            DISPLAY_ORDER NUMBER DEFAULT 100,
            VERSION NUMBER DEFAULT 1,
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_FLAG_PROJECT_CONFIG")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            FLAG_KEY VARCHAR(128) NOT NULL,
            IS_ENABLED BOOLEAN DEFAULT TRUE,
            LABEL_OVERRIDE VARCHAR(255),
            DESCRIPTION_OVERRIDE VARCHAR(4000),
            SEVERITY_OVERRIDE VARCHAR(32),
            MESSAGE_TEMPLATE_OVERRIDE VARCHAR(4000),
            PARAMS VARIANT,
            FIELD_ALIASES VARIANT,
            VERSION NUMBER DEFAULT 1,
            UPDATED_BY VARCHAR(255),
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (PROJECT_NAME, FLAG_KEY)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_FLAG_AUDIT")} (
            AUDIT_ID NUMBER AUTOINCREMENT,
            PROJECT_NAME VARCHAR(255),
            FLAG_KEY VARCHAR(128),
            ACTION VARCHAR(64),
            OLD_CONFIG VARIANT,
            NEW_CONFIG VARIANT,
            ACTOR VARCHAR(255),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
    ]

    with cursor(schema=BOOTSTRAP_SCHEMA) as cur:
        for statement in ddl_statements:
            cur.execute(statement)



def _additive_table_ddl() -> list[str]:
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("NOTIFICATIONS")} (
            NOTIFICATION_ID NUMBER AUTOINCREMENT,
            RECIPIENT VARCHAR(255) NOT NULL,
            NTYPE VARCHAR(64) NOT NULL,
            MESSAGE VARCHAR(4000),
            PROJECT_NAME VARCHAR(255),
            RECORD_ID VARCHAR(64),
            IS_READ BOOLEAN DEFAULT FALSE,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("QUALITY_ALERTS")} (
            ALERT_ID NUMBER AUTOINCREMENT,
            PROJECT_NAME VARCHAR(255) NOT NULL,
            ALERT_TYPE VARCHAR(64) NOT NULL,
            SUBJECT VARCHAR(512),
            METRIC_VALUE FLOAT,
            THRESHOLD FLOAT,
            SEVERITY VARCHAR(32),
            MESSAGE VARCHAR(4000),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_CHECKS")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            RECORD_ID VARCHAR(64) NOT NULL,
            CHECK_TYPE VARCHAR(64) NOT NULL,
            STATUS VARCHAR(32),
            DETAIL VARCHAR(4000),
            CATEGORY VARCHAR(64),
            SEVERITY VARCHAR(32),
            RULE_VERSION NUMBER DEFAULT 1,
            DETAIL_JSON VARIANT,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (PROJECT_NAME, RECORD_ID, CHECK_TYPE)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_FLAG_DEFINITIONS")} (
            FLAG_KEY VARCHAR(128) NOT NULL PRIMARY KEY,
            LABEL VARCHAR(255) NOT NULL,
            CATEGORY VARCHAR(64) NOT NULL,
            DESCRIPTION VARCHAR(4000),
            SEVERITY VARCHAR(32) DEFAULT 'medium',
            RULE_KIND VARCHAR(64) DEFAULT 'declarative',
            DEFAULT_ENABLED BOOLEAN DEFAULT TRUE,
            DEFAULT_PARAMS VARIANT,
            MESSAGE_TEMPLATE VARCHAR(4000),
            DISPLAY_ORDER NUMBER DEFAULT 100,
            VERSION NUMBER DEFAULT 1,
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_FLAG_PROJECT_CONFIG")} (
            PROJECT_NAME VARCHAR(255) NOT NULL,
            FLAG_KEY VARCHAR(128) NOT NULL,
            IS_ENABLED BOOLEAN DEFAULT TRUE,
            LABEL_OVERRIDE VARCHAR(255),
            DESCRIPTION_OVERRIDE VARCHAR(4000),
            SEVERITY_OVERRIDE VARCHAR(32),
            MESSAGE_TEMPLATE_OVERRIDE VARCHAR(4000),
            PARAMS VARIANT,
            FIELD_ALIASES VARIANT,
            VERSION NUMBER DEFAULT 1,
            UPDATED_BY VARCHAR(255),
            UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (PROJECT_NAME, FLAG_KEY)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qualified("DEMOGRAPHIC_FLAG_AUDIT")} (
            AUDIT_ID NUMBER AUTOINCREMENT,
            PROJECT_NAME VARCHAR(255),
            FLAG_KEY VARCHAR(128),
            ACTION VARCHAR(64),
            OLD_CONFIG VARIANT,
            NEW_CONFIG VARIANT,
            ACTOR VARCHAR(255),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
    ]


_TIMESTAMP_TYPES = frozenset({"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"})

# Columns that must be TIMESTAMP_NTZ (may have been created as NUMBER by write_pandas).
_TIMESTAMP_COLUMN_MAP: dict[str, list[str]] = {
    "RECORDS": ["INGESTED_AT", "UPDATED_AT"],
    "COMBINED_CHECKS": ["UPDATED_AT"],
    "DECISION_HISTORY": ["CREATED_AT"],
    "ASSIGNMENTS": ["ASSIGNED_AT", "DEFER_UNTIL"],
    "REVIEWER_STATS": ["CREATED_AT", "UPDATED_AT"],
    "SYNC_STATE": ["LAST_PULL_TS", "LAST_OD_SYNC_SEEN", "LAST_KINGELVIS_EXPORT_TS", "UPDATED_AT"],
    "ORIGINAL_RECORDS": ["CAPTURED_AT"],
    "NOTIFICATIONS": ["CREATED_AT"],
    "QUALITY_ALERTS": ["CREATED_AT"],
    "DEMOGRAPHIC_CHECKS": ["CREATED_AT"],
    "DEMOGRAPHIC_FLAG_DEFINITIONS": ["UPDATED_AT"],
    "DEMOGRAPHIC_FLAG_PROJECT_CONFIG": ["UPDATED_AT"],
    "DEMOGRAPHIC_FLAG_AUDIT": ["CREATED_AT"],
    "PROJECTS": ["SYNCED_AT"],
}

_TIMESTAMP_DEFAULTS = frozenset(
    {
        "CREATED_AT",
        "UPDATED_AT",
        "ASSIGNED_AT",
        "CAPTURED_AT",
        "SYNCED_AT",
    }
)


def _column_data_type(table_name: str, column_name: str) -> str | None:
    database = env("SNOWFLAKE_DATABASE")
    if not database:
        return None
    df = fetch_df(
        f"""
        SELECT DATA_TYPE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (REVIEW_CYCLE_SCHEMA.upper(), table_name.upper(), column_name.upper()),
        schema="PUBLIC",
    )
    if df.empty:
        return None
    return str(df.iloc[0]["DATA_TYPE"]).upper()


def _column_exists(table_name: str, column_name: str) -> bool:
    return _column_data_type(table_name, column_name) is not None


def _repair_timestamp_column(table_name: str, column_name: str) -> bool:
    """Convert a mis-typed column (e.g. NUMBER) to TIMESTAMP_NTZ."""
    data_type = _column_data_type(table_name, column_name)
    if data_type is None or data_type in _TIMESTAMP_TYPES:
        return False

    qtable = _qualified(table_name)
    tmp = f"{column_name}_TS_MIG"
    if _column_exists(table_name, tmp):
        execute(f"ALTER TABLE {qtable} DROP COLUMN {tmp}", schema=BOOTSTRAP_SCHEMA)

    execute(f"ALTER TABLE {qtable} ADD COLUMN {tmp} TIMESTAMP_NTZ", schema=BOOTSTRAP_SCHEMA)
    execute(f"UPDATE {qtable} SET {tmp} = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ", schema=BOOTSTRAP_SCHEMA)
    execute(f"ALTER TABLE {qtable} DROP COLUMN {column_name}", schema=BOOTSTRAP_SCHEMA)
    execute(f"ALTER TABLE {qtable} RENAME COLUMN {tmp} TO {column_name}", schema=BOOTSTRAP_SCHEMA)
    if column_name in _TIMESTAMP_DEFAULTS:
        try:
            execute(
                f"ALTER TABLE {qtable} ALTER COLUMN {column_name} SET DEFAULT CURRENT_TIMESTAMP()",
                schema=BOOTSTRAP_SCHEMA,
            )
        except ProgrammingError:
            pass
    return True


def repair_timestamp_columns() -> list[str]:
    """Fix timestamp columns created with the wrong Snowflake data type."""
    repaired: list[str] = []
    for table_name, columns in _TIMESTAMP_COLUMN_MAP.items():
        if not table_exists(table_name):
            continue
        for column_name in columns:
            try:
                if _repair_timestamp_column(table_name, column_name):
                    repaired.append(f"{table_name}.{column_name}")
            except ProgrammingError:
                continue
    return repaired


def _is_varchar_type(data_type: str) -> bool:
    dt = data_type.upper()
    return dt.startswith("VARCHAR") or dt in {"TEXT", "STRING"}


def _repair_varchar_column(table_name: str, column_name: str, size: int = 255) -> bool:
    """Convert a mis-typed column (e.g. NUMBER) to VARCHAR."""
    if not table_exists(table_name):
        return False
    data_type = _column_data_type(table_name, column_name)
    qtable = _qualified(table_name)
    if data_type is None:
        execute(
            f"ALTER TABLE {qtable} ADD COLUMN IF NOT EXISTS {column_name} VARCHAR({size})",
            schema=BOOTSTRAP_SCHEMA,
        )
        return True
    if _is_varchar_type(data_type):
        return False
    try:
        execute(f"ALTER TABLE {qtable} DROP COLUMN {column_name}", schema=BOOTSTRAP_SCHEMA)
    except ProgrammingError:
        return False
    execute(
        f"ALTER TABLE {qtable} ADD COLUMN {column_name} VARCHAR({size})",
        schema=BOOTSTRAP_SCHEMA,
    )
    return True


def repair_combined_checks_reviewer_columns() -> list[str]:
    """Ensure TWO_X_REVIEWED_* columns are VARCHAR (fixes pandas mis-inferred NUMBER types)."""
    repaired: list[str] = []
    if not table_exists("COMBINED_CHECKS"):
        return repaired
    for column_name in ("TWO_X_REVIEWED_BY", "TWO_X_REVIEWED_FLAG"):
        try:
            if _repair_varchar_column("COMBINED_CHECKS", column_name, 255):
                repaired.append(f"COMBINED_CHECKS.{column_name}")
        except ProgrammingError:
            continue
    return repaired


def repair_records_route_surveyed_code() -> bool:
    """Ensure RECORDS.ROUTE_SURVEYED_CODE is VARCHAR (Tampa uses codes like TAM_1_1_00)."""
    if not table_exists("RECORDS"):
        return False
    data_type = _column_data_type("RECORDS", "ROUTE_SURVEYED_CODE")
    if data_type is None or _is_varchar_type(data_type):
        return False

    qtable = _qualified("RECORDS")
    tmp = "ROUTE_SURVEYED_CODE_VARCHAR_MIG"
    if _column_exists("RECORDS", tmp):
        execute(f"ALTER TABLE {qtable} DROP COLUMN {tmp}", schema=BOOTSTRAP_SCHEMA)
    execute(f"ALTER TABLE {qtable} ADD COLUMN {tmp} VARCHAR(255)", schema=BOOTSTRAP_SCHEMA)
    execute(
        f"UPDATE {qtable} SET {tmp} = TO_VARCHAR(ROUTE_SURVEYED_CODE) "
        f"WHERE ROUTE_SURVEYED_CODE IS NOT NULL",
        schema=BOOTSTRAP_SCHEMA,
    )
    execute(f"ALTER TABLE {qtable} DROP COLUMN ROUTE_SURVEYED_CODE", schema=BOOTSTRAP_SCHEMA)
    execute(
        f"ALTER TABLE {qtable} RENAME COLUMN {tmp} TO ROUTE_SURVEYED_CODE",
        schema=BOOTSTRAP_SCHEMA,
    )
    return True


def ensure_migrations() -> None:
    """Create any tables added after an older bootstrap (idempotent, additive only)."""
    if not schema_is_ready():
        return
    with cursor(schema=BOOTSTRAP_SCHEMA) as cur:
        for statement in _additive_table_ddl():
            cur.execute(statement)
    repair_timestamp_columns()
    _ensure_demographic_check_metadata_columns()
    _ensure_combined_checks_escalated()
    _ensure_records_pipeline_sort_order()
    repair_combined_checks_reviewer_columns()
    try:
        repair_records_route_surveyed_code()
    except ProgrammingError:
        pass


def _ensure_demographic_check_metadata_columns() -> None:
    """Add traceability columns for configurable demographic rules."""
    if not table_exists("DEMOGRAPHIC_CHECKS"):
        return
    columns = {
        "CATEGORY": "VARCHAR(64)",
        "SEVERITY": "VARCHAR(32)",
        "RULE_VERSION": "NUMBER DEFAULT 1",
        "DETAIL_JSON": "VARIANT",
    }
    for column_name, column_type in columns.items():
        try:
            execute(
                f"ALTER TABLE {_qualified('DEMOGRAPHIC_CHECKS')} ADD COLUMN IF NOT EXISTS {column_name} {column_type}",
                schema=BOOTSTRAP_SCHEMA,
            )
        except ProgrammingError:
            pass


def _ensure_combined_checks_escalated() -> None:
    """Add ESCALATED column to COMBINED_CHECKS if missing (additive migration)."""
    if not table_exists("COMBINED_CHECKS"):
        return
    try:
        execute(
            f"ALTER TABLE {_qualified('COMBINED_CHECKS')} ADD COLUMN IF NOT EXISTS ESCALATED BOOLEAN DEFAULT FALSE",
            schema=BOOTSTRAP_SCHEMA,
        )
    except ProgrammingError:
        pass


def _ensure_records_pipeline_sort_order() -> None:
    """Preserve Elvis Review row order from the pipeline sort step."""
    if not table_exists("RECORDS"):
        return
    try:
        execute(
            f"ALTER TABLE {_qualified('RECORDS')} ADD COLUMN IF NOT EXISTS PIPELINE_SORT_ORDER NUMBER",
            schema=BOOTSTRAP_SCHEMA,
        )
    except ProgrammingError:
        pass


def table_exists(table_name: str) -> bool:
    try:
        with cursor(schema=BOOTSTRAP_SCHEMA) as cur:
            cur.execute(f"SELECT 1 FROM {_qualified(table_name)} LIMIT 1")
        return True
    except ProgrammingError:
        return False


def seed_projects_from_app_config() -> int:
    app_config_table = fq_table("PROJECT_CONFIGS", APP_CONFIG_SCHEMA)
    query = f"""
    SELECT
        PROJECT_NAME,
        BASE_SCHEMA,
        ELVIS_DATABASE,
        ELVIS_TABLE,
        MAIN_DATABASE,
        MAIN_TABLE,
        DETAILS_FILE_NAME,
        CR_FILE_NAME,
        KINGELVIS_FILE_NAME,
        ELVIS_PROJECT_NAME
    FROM {app_config_table}
    WHERE IS_ACTIVE = TRUE
    """
    try:
        projects = fetch_df(query, schema="PUBLIC")
    except Exception as exc:
        raise RuntimeError(
            f"Could not read projects from {app_config_table}. "
            f"Check APP_CONFIG_SCHEMA in .env and your Snowflake role grants. Detail: {exc}"
        ) from exc
    if projects.empty:
        return 0

    rows = []
    active_names: list[str] = []
    for _, row in projects.iterrows():
        project_name = str(row["PROJECT_NAME"]).strip()
        if not project_name:
            continue
        active_names.append(project_name)
        pipeline_code = str(row.get("ELVIS_PROJECT_NAME") or project_name).upper().replace(" ", "_")
        rows.append(
            {
                "PROJECT_NAME": project_name,
                "BASE_SCHEMA": row.get("BASE_SCHEMA"),
                "ELVIS_DATABASE": row.get("ELVIS_DATABASE"),
                "ELVIS_TABLE": row.get("ELVIS_TABLE"),
                "MAIN_DATABASE": row.get("MAIN_DATABASE"),
                "MAIN_TABLE": row.get("MAIN_TABLE"),
                "DETAILS_FILE_NAME": row.get("DETAILS_FILE_NAME"),
                "CR_FILE_NAME": row.get("CR_FILE_NAME"),
                "KINGELVIS_FILE_NAME": row.get("KINGELVIS_FILE_NAME"),
                "ELVIS_PROJECT_NAME": row.get("ELVIS_PROJECT_NAME"),
                "PIPELINE_PROJECT_CODE": pipeline_code,
                "IS_ACTIVE": True,
                "SYNCED_AT": datetime.utcnow(),
            }
        )
    if not rows:
        return 0

    merge_upsert(
        pd.DataFrame(rows),
        "PROJECTS",
        key_columns=["PROJECT_NAME"],
    )

    # Hide projects that are no longer active in APP_CONFIG (matches OD Dashboard).
    if active_names:
        placeholders = ", ".join(["%s"] * len(active_names))
        execute(
            f"""
            UPDATE {fq_table('PROJECTS')}
            SET IS_ACTIVE = FALSE,
                SYNCED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            WHERE PROJECT_NAME NOT IN ({placeholders})
              AND IS_ACTIVE = TRUE
            """,
            tuple(active_names),
            schema="PUBLIC",
        )
    return len(rows)


def bootstrap_database() -> dict[str, Any]:
    init_schema()
    ensure_migrations()
    project_count = seed_projects_from_app_config()
    return {
        "database": env("SNOWFLAKE_DATABASE"),
        "schema": REVIEW_CYCLE_SCHEMA,
        "projects_seeded": project_count,
    }


def refresh_projects() -> int:
    """Re-load PROJECTS from APP_CONFIG without recreating all tables."""
    if not schema_is_ready():
        raise RuntimeError("Run Initialize database schema first.")
    return seed_projects_from_app_config()
