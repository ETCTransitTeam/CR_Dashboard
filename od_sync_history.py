"""
OD sync run history, settings, and email alerts.

Used by morning_od_sync.py, the dashboard Sync button, and the Sync History page.
Does not change sync pipeline logic — only records outcomes and optional alerts.
"""

from __future__ import annotations

import os
import smtplib
import socket
import traceback
import uuid
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

APP_CONFIG_SCHEMA = os.getenv("APP_CONFIG_SCHEMA", "APP_CONFIG").strip() or "APP_CONFIG"
TZ = ZoneInfo("America/Chicago")
APP_PUBLIC_BASE = (
    os.getenv("APP_PUBLIC_BASE_URL", "https://odcollection.etc-research.com").rstrip("/")
)

# Setting keys (values are strings in OD_SYNC_SETTINGS)
KEY_MORNING_ENABLED = "morning_sync_enabled"
KEY_EMAIL_ALERTS = "email_alerts_enabled"
KEY_ALERT_EMAILS = "alert_emails"
KEY_DROP_ALERT_PCT = "drop_alert_pct"

DEFAULT_DROP_ALERT_PCT = "2"  # example default; editable in Sync History

_tables_ready = False
_private_key_bytes = None


def _now_chicago() -> datetime:
    return datetime.now(TZ).replace(tzinfo=None)


def _load_private_key_bytes():
    global _private_key_bytes
    if _private_key_bytes is not None:
        return _private_key_bytes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "path/to/key.p8")
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=os.environ["SNOWFLAKE_PASSPHRASE"].encode(),
            backend=default_backend(),
        )
    _private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return _private_key_bytes


def _connect():
    """Lightweight Snowflake connect — avoids importing automated_refresh_flow_new."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=_load_private_key_bytes(),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        authenticator="SNOWFLAKE_JWT",
        role=os.getenv("SNOWFLAKE_ROLE"),
        schema=APP_CONFIG_SCHEMA,
        network_timeout=60,
        login_timeout=60,
    )
    return conn


def ensure_od_sync_history_tables() -> None:
    """Idempotent CREATE for history + settings tables (once per process)."""
    global _tables_ready
    if _tables_ready:
        return
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {APP_CONFIG_SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {APP_CONFIG_SCHEMA}.OD_SYNC_RUNS (
                RUN_ID VARCHAR NOT NULL,
                SYNC_TRIGGER VARCHAR,
                STARTED_AT TIMESTAMP_NTZ,
                FINISHED_AT TIMESTAMP_NTZ,
                STATUS VARCHAR,
                PROJECT_TOTAL NUMBER,
                PROJECT_OK NUMBER,
                PROJECT_FAILED NUMBER,
                HOST VARCHAR,
                ACTOR VARCHAR,
                NOTES VARCHAR,
                PRIMARY KEY (RUN_ID)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {APP_CONFIG_SCHEMA}.OD_SYNC_RUN_PROJECTS (
                RUN_ID VARCHAR NOT NULL,
                PROJECT_NAME VARCHAR NOT NULL,
                SCHEMA_NAME VARCHAR,
                STATUS VARCHAR,
                STARTED_AT TIMESTAMP_NTZ,
                FINISHED_AT TIMESTAMP_NTZ,
                DURATION_SEC FLOAT,
                RESULT_LABEL VARCHAR,
                SOURCE_ROWS NUMBER,
                WEEKDAY_VISIBLE NUMBER,
                WEEKEND_VISIBLE NUMBER,
                DASHBOARD_VISIBLE NUMBER,
                PREV_DASHBOARD_VISIBLE NUMBER,
                DROP_PCT FLOAT,
                ALERT_DROP BOOLEAN,
                ERROR_MESSAGE VARCHAR,
                ERROR_DETAIL VARCHAR(16777216),
                PRIMARY KEY (RUN_ID, PROJECT_NAME)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {APP_CONFIG_SCHEMA}.OD_SYNC_SETTINGS (
                SETTING_KEY VARCHAR NOT NULL,
                SETTING_VALUE VARCHAR,
                UPDATED_AT TIMESTAMP_NTZ,
                UPDATED_BY VARCHAR,
                PRIMARY KEY (SETTING_KEY)
            )
            """
        )
        _bootstrap_settings(cur)
        conn.commit()
        _tables_ready = True
    finally:
        cur.close()
        conn.close()


def _bootstrap_settings(cur) -> None:
    defaults = {
        KEY_MORNING_ENABLED: "true",
        KEY_EMAIL_ALERTS: "true",
        KEY_ALERT_EMAILS: "",
        KEY_DROP_ALERT_PCT: DEFAULT_DROP_ALERT_PCT,
    }
    now = _now_chicago()
    for key, value in defaults.items():
        cur.execute(
            f"""
            MERGE INTO {APP_CONFIG_SCHEMA}.OD_SYNC_SETTINGS t
            USING (
              SELECT %s AS SETTING_KEY, %s AS SETTING_VALUE, %s AS UPDATED_AT
            ) s
            ON t.SETTING_KEY = s.SETTING_KEY
            WHEN NOT MATCHED THEN INSERT (SETTING_KEY, SETTING_VALUE, UPDATED_AT, UPDATED_BY)
              VALUES (s.SETTING_KEY, s.SETTING_VALUE, s.UPDATED_AT, 'bootstrap')
            """,
            (key, value, now),
        )


def get_setting(key: str, default: str = "") -> str:
    ensure_od_sync_history_tables()
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT SETTING_VALUE FROM {APP_CONFIG_SCHEMA}.OD_SYNC_SETTINGS
            WHERE SETTING_KEY = %s
            """,
            (key,),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return default
        return str(row[0])
    finally:
        cur.close()
        conn.close()


def set_setting(key: str, value: str, updated_by: str = "") -> None:
    ensure_od_sync_history_tables()
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            MERGE INTO {APP_CONFIG_SCHEMA}.OD_SYNC_SETTINGS t
            USING (
              SELECT %s AS SETTING_KEY, %s AS SETTING_VALUE, %s AS UPDATED_AT, %s AS UPDATED_BY
            ) s
            ON t.SETTING_KEY = s.SETTING_KEY
            WHEN MATCHED THEN UPDATE SET
              SETTING_VALUE = s.SETTING_VALUE,
              UPDATED_AT = s.UPDATED_AT,
              UPDATED_BY = s.UPDATED_BY
            WHEN NOT MATCHED THEN INSERT (SETTING_KEY, SETTING_VALUE, UPDATED_AT, UPDATED_BY)
              VALUES (s.SETTING_KEY, s.SETTING_VALUE, s.UPDATED_AT, s.UPDATED_BY)
            """,
            (key, value, _now_chicago(), updated_by or "system"),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def get_setting_meta(key: str) -> dict:
    ensure_od_sync_history_tables()
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT SETTING_VALUE, UPDATED_AT, UPDATED_BY
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_SETTINGS
            WHERE SETTING_KEY = %s
            """,
            (key,),
        )
        row = cur.fetchone()
        if not row:
            return {"value": "", "updated_at": None, "updated_by": ""}
        return {
            "value": "" if row[0] is None else str(row[0]),
            "updated_at": row[1],
            "updated_by": row[2] or "",
        }
    finally:
        cur.close()
        conn.close()


def is_morning_sync_enabled() -> bool:
    return get_setting(KEY_MORNING_ENABLED, "true").strip().lower() in ("1", "true", "yes", "on")


def set_morning_sync_enabled(enabled: bool, updated_by: str = "") -> None:
    set_setting(KEY_MORNING_ENABLED, "true" if enabled else "false", updated_by=updated_by)


def is_email_alerts_enabled() -> bool:
    return get_setting(KEY_EMAIL_ALERTS, "true").strip().lower() in ("1", "true", "yes", "on")


def get_drop_alert_pct() -> float:
    raw = get_setting(KEY_DROP_ALERT_PCT, DEFAULT_DROP_ALERT_PCT).strip()
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(DEFAULT_DROP_ALERT_PCT)


def get_alert_recipients() -> list[str]:
    raw = get_setting(KEY_ALERT_EMAILS, "").strip()
    if raw:
        return [e.strip() for e in raw.split(",") if e.strip()]
    try:
        from authentication.auth import SUPER_ADMIN_EMAILS

        return list(SUPER_ADMIN_EMAILS)
    except Exception:
        return []


def new_run_id() -> str:
    return str(uuid.uuid4())


def start_run(
    *,
    trigger: str,
    project_total: int,
    actor: str = "",
    notes: str = "",
    status: str = "running",
) -> str:
    ensure_od_sync_history_tables()
    run_id = new_run_id()
    host = socket.gethostname()
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            INSERT INTO {APP_CONFIG_SCHEMA}.OD_SYNC_RUNS (
                RUN_ID, SYNC_TRIGGER, STARTED_AT, FINISHED_AT, STATUS,
                PROJECT_TOTAL, PROJECT_OK, PROJECT_FAILED, HOST, ACTOR, NOTES
            ) VALUES (%s, %s, %s, NULL, %s, %s, 0, 0, %s, %s, %s)
            """,
            (
                run_id,
                trigger,
                _now_chicago(),
                status,
                project_total,
                host,
                actor or "",
                notes or "",
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return run_id


def finish_run(
    run_id: str,
    *,
    status: str,
    project_ok: int,
    project_failed: int,
    notes: str = "",
) -> None:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE {APP_CONFIG_SCHEMA}.OD_SYNC_RUNS
            SET FINISHED_AT = %s,
                STATUS = %s,
                PROJECT_OK = %s,
                PROJECT_FAILED = %s,
                NOTES = CASE WHEN %s = '' THEN NOTES ELSE %s END
            WHERE RUN_ID = %s
            """,
            (_now_chicago(), status, project_ok, project_failed, notes or "", notes or "", run_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def start_project_row(run_id: str, project_name: str, schema_name: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            MERGE INTO {APP_CONFIG_SCHEMA}.OD_SYNC_RUN_PROJECTS t
            USING (
              SELECT %s AS RUN_ID, %s AS PROJECT_NAME, %s AS SCHEMA_NAME, %s AS STARTED_AT
            ) s
            ON t.RUN_ID = s.RUN_ID AND t.PROJECT_NAME = s.PROJECT_NAME
            WHEN MATCHED THEN UPDATE SET
              SCHEMA_NAME = s.SCHEMA_NAME,
              STARTED_AT = s.STARTED_AT,
              STATUS = 'running',
              FINISHED_AT = NULL,
              ERROR_MESSAGE = NULL,
              ERROR_DETAIL = NULL
            WHEN NOT MATCHED THEN INSERT (
              RUN_ID, PROJECT_NAME, SCHEMA_NAME, STATUS, STARTED_AT
            ) VALUES (s.RUN_ID, s.PROJECT_NAME, s.SCHEMA_NAME, 'running', s.STARTED_AT)
            """,
            (run_id, project_name, schema_name, _now_chicago()),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _previous_success_dashboard_visible(project_name: str) -> Optional[int]:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT DASHBOARD_VISIBLE
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_RUN_PROJECTS
            WHERE PROJECT_NAME = %s
              AND STATUS = 'success'
              AND DASHBOARD_VISIBLE IS NOT NULL
            ORDER BY FINISHED_AT DESC NULLS LAST
            LIMIT 1
            """,
            (project_name,),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()


def fetch_visible_counts(project_name: str, schema_name: str) -> dict[str, int]:
    """Reuse landing-card weekday/weekend sums."""
    try:
        from authentication.auth import fetch_admin_landing_project_stats

        stats = fetch_admin_landing_project_stats(((project_name, schema_name),))
        row = stats.get(project_name) or {}
        weekday = int(row.get("weekday") or 0)
        weekend = int(row.get("weekend") or 0)
        return {
            "weekday": weekday,
            "weekend": weekend,
            "dashboard": weekday + weekend,
        }
    except Exception:
        return {"weekday": 0, "weekend": 0, "dashboard": 0}


def finish_project_row(
    run_id: str,
    project_name: str,
    *,
    status: str,
    started_monotonic: float,
    result_label: str = "",
    error_message: str = "",
    error_detail: str = "",
    schema_name: str = "",
    collect_counts: bool = True,
) -> dict[str, Any]:
    """
    Finalize a project row. Returns alert payload fields used by email digest.
    """
    import time

    finished = _now_chicago()
    duration = max(0.0, time.time() - started_monotonic)
    weekday = weekend = dashboard = None
    prev = None
    drop_pct = None
    alert_drop = False
    threshold = get_drop_alert_pct()

    if status == "success" and collect_counts and schema_name:
        counts = fetch_visible_counts(project_name, schema_name)
        weekday = counts["weekday"]
        weekend = counts["weekend"]
        dashboard = counts["dashboard"]
        prev = _previous_success_dashboard_visible(project_name)
        # Compare against previous success BEFORE writing this row
        if prev is not None and prev > 0 and dashboard is not None:
            drop_pct = (prev - dashboard) / prev * 100.0
            if drop_pct >= threshold:
                alert_drop = True

    detail = (error_detail or "")[:16000]

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE {APP_CONFIG_SCHEMA}.OD_SYNC_RUN_PROJECTS
            SET STATUS = %s,
                FINISHED_AT = %s,
                DURATION_SEC = %s,
                RESULT_LABEL = %s,
                WEEKDAY_VISIBLE = %s,
                WEEKEND_VISIBLE = %s,
                DASHBOARD_VISIBLE = %s,
                PREV_DASHBOARD_VISIBLE = %s,
                DROP_PCT = %s,
                ALERT_DROP = %s,
                ERROR_MESSAGE = %s,
                ERROR_DETAIL = %s,
                SCHEMA_NAME = COALESCE(NULLIF(%s, ''), SCHEMA_NAME)
            WHERE RUN_ID = %s AND PROJECT_NAME = %s
            """,
            (
                status,
                finished,
                duration,
                result_label or "",
                weekday,
                weekend,
                dashboard,
                prev,
                drop_pct,
                alert_drop,
                (error_message or "")[:2000],
                detail,
                schema_name or "",
                run_id,
                project_name,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {
        "project_name": project_name,
        "status": status,
        "error_message": error_message or "",
        "dashboard_visible": dashboard,
        "prev_dashboard_visible": prev,
        "drop_pct": drop_pct,
        "alert_drop": alert_drop,
        "threshold": threshold,
        "result_label": result_label or "",
        "duration_sec": duration,
    }


def _smtp_port() -> int:
    try:
        return int(os.getenv("EMAIL_PORT") or "587")
    except (TypeError, ValueError):
        return 587


def send_od_sync_alert_email(subject: str, html_body: str, to_emails: Optional[list[str]] = None) -> bool:
    recipients = to_emails if to_emails is not None else get_alert_recipients()
    if not recipients:
        return False
    host = os.getenv("EMAIL_HOST")
    address = os.getenv("EMAIL_ADDRESS")
    password = os.getenv("EMAIL_PASSWORD")
    if not host or not address or not password:
        print("[od_sync_history] SMTP not configured; skip email", flush=True)
        return False
    try:
        msg = MIMEMultipart()
        msg["From"] = address
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP(host, _smtp_port()) as server:
            server.starttls()
            server.login(address, password)
            server.sendmail(address, recipients, msg.as_string())
        return True
    except Exception as exc:
        print(f"[od_sync_history] email failed: {exc}", flush=True)
        traceback.print_exc()
        return False


def send_run_alert_digest(
    *,
    run_id: str,
    trigger: str,
    failures: list[dict[str, Any]],
    drops: list[dict[str, Any]],
) -> None:
    if not is_email_alerts_enabled():
        return
    if not failures and not drops:
        return

    history_url = f"{APP_PUBLIC_BASE}/?page=sync_history"
    fail_rows = "".join(
        f"<tr><td>{f.get('project_name','')}</td><td>{f.get('error_message','')}</td></tr>"
        for f in failures
    )
    drop_rows = "".join(
        (
            f"<tr><td>{d.get('project_name','')}</td>"
            f"<td>{d.get('prev_dashboard_visible')}</td>"
            f"<td>{d.get('dashboard_visible')}</td>"
            f"<td>{(d.get('drop_pct') or 0):.2f}%</td>"
            f"<td>{d.get('threshold')}%</td></tr>"
        )
        for d in drops
    )
    parts = [
        f"<p>Trigger: <strong>{trigger}</strong></p>",
        f"<p>Run ID: <code>{run_id}</code></p>",
        f'<p><a href="{history_url}">Open Sync History</a></p>',
    ]
    if failures:
        parts.append("<h3>Failures</h3>")
        parts.append(
            "<table border='1' cellpadding='6'><tr><th>Project</th><th>Error</th></tr>"
            f"{fail_rows}</table>"
        )
    if drops:
        parts.append("<h3>Record drops</h3>")
        parts.append(
            "<table border='1' cellpadding='6'>"
            "<tr><th>Project</th><th>Previous</th><th>Current</th><th>Drop %</th><th>Threshold</th></tr>"
            f"{drop_rows}</table>"
        )
    subject_bits = []
    if failures:
        subject_bits.append(f"{len(failures)} failed")
    if drops:
        subject_bits.append(f"{len(drops)} drop alert(s)")
    subject = f"[OD Sync] {', '.join(subject_bits)} — {trigger}"
    send_od_sync_alert_email(subject, "<html><body>" + "".join(parts) + "</body></html>")


def list_recent_runs(limit: int = 100, days: int = 30) -> list[dict]:
    ensure_od_sync_history_tables()
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT RUN_ID, SYNC_TRIGGER, STARTED_AT, FINISHED_AT, STATUS,
                   PROJECT_TOTAL, PROJECT_OK, PROJECT_FAILED, HOST, ACTOR, NOTES
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_RUNS
            WHERE STARTED_AT >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
            ORDER BY STARTED_AT DESC
            LIMIT %s
            """,
            (days, limit),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in (cur.fetchall() or [])]
        # Alias for UI: treat SYNC_TRIGGER as TRIGGER
        for r in rows:
            if "SYNC_TRIGGER" in r and "TRIGGER" not in r:
                r["TRIGGER"] = r["SYNC_TRIGGER"]
        return rows
    finally:
        cur.close()
        conn.close()


def list_run_projects(run_id: str) -> list[dict]:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT RUN_ID, PROJECT_NAME, SCHEMA_NAME, STATUS, STARTED_AT, FINISHED_AT,
                   DURATION_SEC, RESULT_LABEL, SOURCE_ROWS,
                   WEEKDAY_VISIBLE, WEEKEND_VISIBLE, DASHBOARD_VISIBLE,
                   PREV_DASHBOARD_VISIBLE, DROP_PCT, ALERT_DROP,
                   ERROR_MESSAGE, ERROR_DETAIL
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_RUN_PROJECTS
            WHERE RUN_ID = %s
            ORDER BY PROJECT_NAME
            """,
            (run_id,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in (cur.fetchall() or [])]
    finally:
        cur.close()
        conn.close()


def summary_chips(days: int = 7) -> dict:
    ensure_od_sync_history_tables()
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT STATUS, COUNT(*)
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_RUNS
            WHERE STARTED_AT >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
            GROUP BY STATUS
            """,
            (days,),
        )
        by_status = {str(r[0]): int(r[1]) for r in (cur.fetchall() or [])}
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_RUN_PROJECTS
            WHERE COALESCE(ALERT_DROP, FALSE) = TRUE
              AND FINISHED_AT >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
            """,
            (days,),
        )
        drop_alerts = int((cur.fetchone() or [0])[0] or 0)
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_RUN_PROJECTS
            WHERE STATUS = 'failed'
              AND FINISHED_AT >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
            """,
            (days,),
        )
        project_fails = int((cur.fetchone() or [0])[0] or 0)
        cur.execute(
            f"""
            SELECT RUN_ID, STATUS, STARTED_AT, SYNC_TRIGGER
            FROM {APP_CONFIG_SCHEMA}.OD_SYNC_RUNS
            ORDER BY STARTED_AT DESC NULLS LAST
            LIMIT 1
            """
        )
        last = cur.fetchone()
        return {
            "by_status": by_status,
            "drop_alerts": drop_alerts,
            "project_fails": project_fails,
            "last_run": (
                {
                    "run_id": last[0],
                    "status": last[1],
                    "started_at": last[2],
                    "trigger": last[3],
                }
                if last
                else None
            ),
            "morning_enabled": is_morning_sync_enabled(),
        }
    finally:
        cur.close()
        conn.close()
