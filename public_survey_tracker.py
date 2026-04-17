"""
Public survey tracker.

Data flow:
- MySQL is only queried when the user clicks Sync / Refresh (full latest snapshot).
- That snapshot is stored in Snowflake (SURVEY_TRACKER_ROWS); project settings are in
  PROJECT_TRACKER_CONFIGS in the same dedicated tracker schema (see PUBLIC_SURVEY_TRACKER_SCHEMA).
- The public page reads counts, filters, and the table from Snowflake only — not from MySQL
  on each load.

Setup: superadmin. Public: ?page=tracker&project_code=... (no auth).
"""
from __future__ import annotations

import os
import re
from html import escape as html_escape
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

# Same timezone as `tucson_cr.py` header Last_Sync_Date display (America/Chicago).
TRACKER_SYNC_TZ = ZoneInfo("America/Chicago")

import pandas as pd
import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from database import DatabaseConnector

load_dotenv()

# Dedicated Snowflake schema for tracker config + cached rows (separate from project BASE_SCHEMA data).
TRACKER_SCHEMA = os.getenv("PUBLIC_SURVEY_TRACKER_SCHEMA", "PUBLIC_SURVEY_TRACKER").strip() or "PUBLIC_SURVEY_TRACKER"

# Project code in URL: letters, digits, dot, underscore, hyphen
_PROJECT_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _snowflake_database() -> str:
    db = os.getenv("SNOWFLAKE_DATABASE")
    if not db:
        raise RuntimeError("SNOWFLAKE_DATABASE is not set")
    return db


def connect_snowflake(private_key_bytes: bytes, schema: Optional[str] = None):
    # JSON result format avoids PyArrow row iteration bugs (e.g. InterfaceError 252005) on
    # some TIMESTAMP_TZ / mixed-type rows when using the default Arrow path.
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=private_key_bytes,
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=_snowflake_database(),
        authenticator="SNOWFLAKE_JWT",
        schema=schema or os.getenv("SNOWFLAKE_DEFAULT_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        network_timeout=120,
        session_parameters={"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "json"},
    )


def _mysql_backtick_quote(ident: str) -> str:
    """Quote a single MySQL identifier (handles hyphens, reserved words, etc.)."""
    return "`" + ident.replace("`", "``") + "`"


def _mysql_from_database_table(database: str, table: str) -> str:
    """
    Build `db`.`part` or `db`.`a`.`b` for qualified table names.
    Matches how MySQL resolves database.table identifiers (same idea as utils.fetch_data + default db).
    """
    db = _mysql_backtick_quote(database.strip())
    parts = [p.strip() for p in table.split(".") if p.strip()]
    if not parts:
        raise ValueError("Table name is empty or invalid.")
    tbl = ".".join(_mysql_backtick_quote(p) for p in parts)
    return f"{db}.{tbl}"


def _validate_mysql_segment(seg: str, label: str) -> Optional[str]:
    """Return an error message if invalid; None if OK. Segments are used inside backticks."""
    if not seg:
        return f"{label} cannot be empty."
    if len(seg) > 255:
        return f"{label} is too long (max 255 characters per part)."
    if any(ord(c) < 32 for c in seg):
        return f"{label} cannot contain control characters."
    _bad = {';', '`', "'", '"', '\\'}
    if any(c in seg for c in _bad):
        return f"{label} cannot contain: semicolon, backtick, quotes, or backslash."
    if "--" in seg or "/*" in seg:
        return f"{label} cannot contain SQL comment sequences (-- or /*)."
    if " " in seg:
        return f"{label} cannot contain spaces."
    return None


def validate_mysql_database_name(name: str) -> Optional[str]:
    s = (name or "").strip()
    err = _validate_mysql_segment(s, "Database name")
    if err:
        return err
    if "." in s:
        return "Use a single database name here (no dots). Put schema.table in the table field if needed."
    return None


def validate_mysql_table_name(name: str) -> Optional[str]:
    s = (name or "").strip()
    if not s:
        return "Table name is required."
    parts = [p for p in s.split(".") if p.strip()]
    if not parts:
        return "Table name is invalid."
    for p in parts:
        err = _validate_mysql_segment(p.strip(), "Table name")
        if err:
            return err
    return None


def ensure_tracker_ddl(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TRACKER_SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TRACKER_SCHEMA}.PROJECT_TRACKER_CONFIGS (
                PROJECT_NAME VARCHAR(255) NOT NULL,
                PROJECT_CODE VARCHAR(128) NOT NULL,
                SOURCE_DATABASE_NAME VARCHAR(255) NOT NULL,
                SOURCE_TABLE_NAME VARCHAR(255) NOT NULL,
                CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                IS_ACTIVE BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (PROJECT_CODE)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TRACKER_SCHEMA}.SURVEY_TRACKER_ROWS (
                PROJECT_CODE VARCHAR(128) NOT NULL,
                ROW_ID VARCHAR(1024) NOT NULL,
                ORIGIN_ADDR VARCHAR(16777216),
                ROUTE_SURVEYED VARCHAR(1024),
                DEST_ADDR VARCHAR(16777216),
                INTERV_INIT VARCHAR(256),
                DATE_STARTED VARCHAR(64),
                DATE_SUBMITTED VARCHAR(64),
                LOCAL_TIME VARCHAR(64),
                DEVICE_TIME VARCHAR(64),
                SYNCED_AT VARCHAR(64),
                PRIMARY KEY (PROJECT_CODE, ROW_ID)
            )
            """
        )
        # Backward-compatible migration from earlier TIMESTAMP_* design to stable VARCHAR datetimes.
        for c in ["DATE_STARTED", "DATE_SUBMITTED", "LOCAL_TIME", "DEVICE_TIME", "SYNCED_AT"]:
            try:
                cur.execute(f"ALTER TABLE {TRACKER_SCHEMA}.SURVEY_TRACKER_ROWS ALTER COLUMN {c} SET DATA TYPE VARCHAR(64)")
            except Exception:
                # If already VARCHAR / column missing / no-op, continue.
                pass
    finally:
        cur.close()


def _col(df: pd.DataFrame, logical: str) -> Optional[pd.Series]:
    """Case-insensitive column match."""
    for c in df.columns:
        if str(c).strip().lower() == logical.lower():
            return df[c]
    return None


def fetch_mysql_table(database: str, table: str) -> pd.DataFrame:
    """
    Plain fetch from MySQL: SELECT * (same pattern as utils.fetch_data).
    Only called during sync. Identifiers are backtick-quoted (hyphens, etc.).
    """
    host = os.getenv("SQL_HOST")
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    if not all([host, user, password is not None, database, table]):
        raise RuntimeError("SQL_HOST, SQL_USER, SQL_PASSWORD, and config database/table must be set.")
    d_err = validate_mysql_database_name(database)
    if d_err:
        raise ValueError(d_err)
    t_err = validate_mysql_table_name(table)
    if t_err:
        raise ValueError(t_err)

    db_clean = database.strip()
    tbl_clean = table.strip()
    from_clause = _mysql_from_database_table(db_clean, tbl_clean)
    sql = f"SELECT * FROM {from_clause}"

    connector = DatabaseConnector(host, db_clean, user, password)
    connector.connect()
    if connector.connection is None:
        raise RuntimeError("Could not connect to MySQL (see server logs).")
    try:
        df = pd.read_sql(sql, connector.connection)
    finally:
        connector.disconnect()
    return df


def _to_tz_naive_series(s: pd.Series) -> pd.Series:
    """
    Robust datetime normalization for mixed SQL source values.
    Handles:
    - regular datetime strings
    - datetime objects
    - unix epochs in seconds/milliseconds/microseconds/nanoseconds
    Returns naive UTC datetimes (Python datetime objects / None).
    """
    if s is None or len(s) == 0:
        return pd.Series([], dtype="object")

    raw = s.copy()
    str_vals = raw.astype(str).str.strip()
    null_like = str_vals.str.lower().isin({"", "none", "null", "nat", "nan"})

    result = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")

    num_vals = pd.to_numeric(str_vals, errors="coerce")
    num_mask = (~null_like) & num_vals.notna()
    if num_mask.any():
        n = num_vals[num_mask]
        sec_mask = n.abs().between(1e9, 1e11, inclusive="left")
        ms_mask = n.abs().between(1e12, 1e14, inclusive="left")
        us_mask = n.abs().between(1e15, 1e17, inclusive="left")
        ns_mask = n.abs().between(1e18, 1e20, inclusive="left")

        if sec_mask.any():
            dt = pd.to_datetime(n[sec_mask], unit="s", errors="coerce", utc=True).dt.tz_convert(None)
            result.loc[dt.index] = dt
        if ms_mask.any():
            dt = pd.to_datetime(n[ms_mask], unit="ms", errors="coerce", utc=True).dt.tz_convert(None)
            result.loc[dt.index] = dt
        if us_mask.any():
            dt = pd.to_datetime(n[us_mask], unit="us", errors="coerce", utc=True).dt.tz_convert(None)
            result.loc[dt.index] = dt
        if ns_mask.any():
            dt = pd.to_datetime(n[ns_mask], unit="ns", errors="coerce", utc=True).dt.tz_convert(None)
            result.loc[dt.index] = dt

    remaining = result.isna() & (~null_like)
    if remaining.any():
        parsed = pd.to_datetime(str_vals[remaining], errors="coerce", utc=True).dt.tz_convert(None)
        result.loc[parsed.index] = parsed

    out = result.astype("object")
    return out.where(pd.notna(out), None)


def _to_snowflake_datetime_string_series(s: pd.Series) -> pd.Series:
    """
    Match existing flow behavior: parse datetime then write as '%Y-%m-%d %H:%M:%S' string.
    """
    dt = _to_tz_naive_series(s)
    parsed = pd.to_datetime(dt, errors="coerce")
    out = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")
    return out.where(parsed.notna(), None)


def dataframe_for_snowflake(df: pd.DataFrame, project_code: str) -> pd.DataFrame:
    """Map MySQL columns to Snowflake row table."""
    def col(name: str) -> pd.Series:
        s = _col(df, name)
        if s is None:
            return pd.Series([None] * len(df), index=df.index)
        return s

    ids = col("id").astype(str)
    pcode = project_code.strip().upper()
    row = pd.DataFrame(
        {
            "PROJECT_CODE": pcode,
            "ROW_ID": ids,
            "ORIGIN_ADDR": col("OriginAddress_ADDR").astype(str).where(col("OriginAddress_ADDR").notna(), None),
            "ROUTE_SURVEYED": col("RouteSurveyed").astype(str).where(col("RouteSurveyed").notna(), None),
            "DEST_ADDR": col("DestinAddress_ADDR").astype(str).where(col("DestinAddress_ADDR").notna(), None),
            "INTERV_INIT": col("IntervInit").astype(str).where(col("IntervInit").notna(), None),
            "DATE_STARTED": _to_snowflake_datetime_string_series(col("Date_started")),
            "DATE_SUBMITTED": _to_snowflake_datetime_string_series(col("Date_submitted")),
            "LOCAL_TIME": _to_snowflake_datetime_string_series(col("LocalTime")),
            "DEVICE_TIME": _to_snowflake_datetime_string_series(col("DeviceTime")),
            "SYNCED_AT": pd.Series(
                [datetime.now(TRACKER_SYNC_TZ).strftime("%Y-%m-%d %H:%M:%S")] * len(df),
                index=df.index,
            ),
        }
    )
    return row


def _apply_source_row_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove test source records before caching to Snowflake.
    Required rules:
    1) IntervInit != 999
    2) Have5MinForSurveCode (as string) == '1'
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    interv = _col(out, "IntervInit")
    if interv is not None:
        # Robust match for values like 999, 999.0, 999.00, " 999.000 "
        interv_num = pd.to_numeric(interv.astype(str).str.strip(), errors="coerce")
        out = out[interv_num.ne(999) | interv_num.isna()]

    have5 = _col(out, "Have5MinForSurveCode")
    if have5 is not None:
        # Accept 1, 1.0, 1.00, " 1 "
        have5_num = pd.to_numeric(have5.astype(str).str.strip(), errors="coerce")
        out = out[have5_num.eq(1)]

    return out


def sync_mysql_to_snowflake(private_key_bytes: bytes, project_code: str, source_db: str, source_table: str) -> dict:
    """Replace Snowflake rows for this project with a full fresh read from MySQL."""
    pcode = project_code.strip().upper()
    raw = fetch_mysql_table(source_db, source_table)
    total_fetched = len(raw)
    filtered_source = _apply_source_row_filters(raw)
    total_loaded = len(filtered_source)
    total_test_removed = max(0, total_fetched - total_loaded)
    staged = dataframe_for_snowflake(filtered_source, pcode)
    conn = connect_snowflake(private_key_bytes, schema=TRACKER_SCHEMA)
    try:
        ensure_tracker_ddl(conn)
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {TRACKER_SCHEMA}.SURVEY_TRACKER_ROWS WHERE UPPER(PROJECT_CODE) = UPPER(%s)",
            (pcode,),
        )
        cur.close()
        conn.commit()
        if not staged.empty:
            ok, _, nrows, _ = write_pandas(
                conn,
                staged,
                "SURVEY_TRACKER_ROWS",
                schema=TRACKER_SCHEMA,
                quote_identifiers=False,
            )
            if not ok:
                raise RuntimeError("write_pandas failed to load survey tracker rows")
            conn.commit()
            loaded_rows = int(nrows)
        else:
            loaded_rows = 0
        return {
            "total_fetched": int(total_fetched),
            "total_test_removed": int(total_test_removed),
            "total_loaded": int(loaded_rows),
        }
    finally:
        conn.close()


def load_config_by_code(private_key_bytes: bytes, project_code: str) -> Optional[dict]:
    conn = connect_snowflake(private_key_bytes, schema=TRACKER_SCHEMA)
    try:
        ensure_tracker_ddl(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT PROJECT_NAME, PROJECT_CODE, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME,
                   CREATED_AT, UPDATED_AT, IS_ACTIVE
            FROM {TRACKER_SCHEMA}.PROJECT_TRACKER_CONFIGS
            WHERE UPPER(PROJECT_CODE) = UPPER(%s) AND IS_ACTIVE = TRUE
            """,
            (project_code.strip(),),
        )
        r = cur.fetchone()
        cur.close()
        if not r:
            return None
        cols = [
            "PROJECT_NAME",
            "PROJECT_CODE",
            "SOURCE_DATABASE_NAME",
            "SOURCE_TABLE_NAME",
            "CREATED_AT",
            "UPDATED_AT",
            "IS_ACTIVE",
        ]
        return dict(zip(cols, r))
    finally:
        conn.close()


def list_all_configs(private_key_bytes: bytes) -> list[dict]:
    conn = connect_snowflake(private_key_bytes, schema=TRACKER_SCHEMA)
    try:
        ensure_tracker_ddl(conn)
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(
            f"""
            SELECT PROJECT_NAME, PROJECT_CODE, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME,
                   CREATED_AT, UPDATED_AT, IS_ACTIVE
            FROM {TRACKER_SCHEMA}.PROJECT_TRACKER_CONFIGS
            WHERE IS_ACTIVE = TRUE
            ORDER BY PROJECT_NAME
            """
        )
        rows = cur.fetchall() or []
        cur.close()
        out = []
        for r in rows:
            d = dict(r)
            out.append({str(k).upper(): v for k, v in d.items()})
        return out
    finally:
        conn.close()


def upsert_config(
    private_key_bytes: bytes,
    project_name: str,
    project_code: str,
    source_db: str,
    source_table: str,
) -> None:
    conn = connect_snowflake(private_key_bytes, schema=TRACKER_SCHEMA)
    try:
        ensure_tracker_ddl(conn)
        cur = conn.cursor()
        code_upper = project_code.strip().upper()
        cur.execute(
            f"SELECT COUNT(*) FROM {TRACKER_SCHEMA}.PROJECT_TRACKER_CONFIGS WHERE PROJECT_CODE = %s",
            (code_upper,),
        )
        exists = cur.fetchone()[0] > 0
        if exists:
            cur.execute(
                f"""
                UPDATE {TRACKER_SCHEMA}.PROJECT_TRACKER_CONFIGS
                SET PROJECT_NAME = %s, SOURCE_DATABASE_NAME = %s, SOURCE_TABLE_NAME = %s,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE PROJECT_CODE = %s
                """,
                (project_name.strip(), source_db.strip(), source_table.strip(), code_upper),
            )
        else:
            cur.execute(
                f"""
                INSERT INTO {TRACKER_SCHEMA}.PROJECT_TRACKER_CONFIGS
                (PROJECT_NAME, PROJECT_CODE, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME, CREATED_AT, UPDATED_AT, IS_ACTIVE)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE)
                """,
                (project_name.strip(), code_upper, source_db.strip(), source_table.strip()),
            )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def load_rows_from_snowflake(private_key_bytes: bytes, project_code: str) -> pd.DataFrame:
    """
    Load cached tracker rows from Snowflake.

    Datetime columns are returned as VARCHAR (formatted strings). The Snowflake Python
    connector can raise InterfaceError 252005 when deserializing TIMESTAMP_TZ / TIMESTAMP_NTZ
    rows whose internal nanosecond values overflow C int conversion — strings avoid that.
    Downstream code already uses pd.to_datetime for filters and display.
    """
    conn = connect_snowflake(private_key_bytes, schema=TRACKER_SCHEMA)
    try:
        ensure_tracker_ddl(conn)
        _ts_fmt = "YYYY-MM-DD HH24:MI:SS.FF9"
        q = f"""
        SELECT
            ROW_ID AS ID,
            ORIGIN_ADDR,
            ROUTE_SURVEYED,
            DEST_ADDR,
            INTERV_INIT,
            TO_VARCHAR(DATE_STARTED, '{_ts_fmt}') AS DATE_STARTED,
            TO_VARCHAR(DATE_SUBMITTED, '{_ts_fmt}') AS DATE_SUBMITTED,
            TO_VARCHAR(LOCAL_TIME, '{_ts_fmt}') AS LOCAL_TIME,
            TO_VARCHAR(DEVICE_TIME, '{_ts_fmt}') AS DEVICE_TIME,
            TO_VARCHAR(SYNCED_AT, '{_ts_fmt}') AS SYNCED_AT
        FROM {TRACKER_SCHEMA}.SURVEY_TRACKER_ROWS
        WHERE UPPER(PROJECT_CODE) = UPPER(%s)
        """
        cur = conn.cursor()
        try:
            cur.execute(q, (project_code.strip(),))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
        finally:
            cur.close()
        df = pd.DataFrame(rows, columns=cols) if cols else pd.DataFrame()
        if len(df.columns) > 0:
            df.columns = [str(c).upper() for c in df.columns]
        return df
    finally:
        conn.close()


def _normalize_tracker_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize/guarantee expected tracker columns to avoid runtime KeyError when result
    metadata differs by connector/version/case.
    """
    expected = [
        "ID",
        "ROW_ID",
        "ORIGIN_ADDR",
        "ROUTE_SURVEYED",
        "DEST_ADDR",
        "INTERV_INIT",
        "DATE_STARTED",
        "DATE_SUBMITTED",
        "LOCAL_TIME",
        "DEVICE_TIME",
        "SYNCED_AT",
    ]
    if df is None or df.empty and len(df.columns) == 0:
        return pd.DataFrame(columns=expected)

    out = df.copy()
    out.columns = [str(c).strip().upper() for c in out.columns]

    # Case-insensitive fallback map for connector variants.
    aliases = {
        "ROWID": "ID",
        "ROW_ID": "ID",
        "INTERVINIT": "INTERV_INIT",
        "ROUTESURVEYED": "ROUTE_SURVEYED",
        "ORIGINADDRESS_ADDR": "ORIGIN_ADDR",
        "DESTINADDRESS_ADDR": "DEST_ADDR",
        "DATE_SUBMIT": "DATE_SUBMITTED",
        "DATE_START": "DATE_STARTED",
        "LOCALTIME": "LOCAL_TIME",
        "DEVICETIME": "DEVICE_TIME",
    }
    for src, dst in aliases.items():
        if src in out.columns and dst not in out.columns:
            out[dst] = out[src]

    for col in expected:
        if col not in out.columns:
            out[col] = None

    # Keep both names available for compatibility across old/new rows.
    if "ID" in out.columns and "ROW_ID" in out.columns:
        out["ID"] = out["ID"].where(out["ID"].notna(), out["ROW_ID"])
        out["ROW_ID"] = out["ROW_ID"].where(out["ROW_ID"].notna(), out["ID"])

    return out[expected]


def _route_str(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return s


def _is_route_excluded(route: str) -> bool:
    r = _route_str(route)
    return r == "999" or r.upper() == "999"


def _is_completed(row: pd.Series) -> bool:
    ds = row.get("DATE_SUBMITTED")
    if ds is None or (isinstance(ds, float) and pd.isna(ds)):
        return False
    t = pd.to_datetime(ds, errors="coerce")
    return not pd.isna(t)


def apply_filters(
    df: pd.DataFrame,
    route_sel: str,
    initials_sel: str,
    survey_day: Optional[date],
) -> pd.DataFrame:
    """
    Filter tracker rows. When ``survey_day`` is set, keep rows whose **LocalTime** calendar
    date equals that day (same moment as the ``Local time`` column). When ``survey_day`` is
    None, no date filter is applied (e.g. for an all-time total).
    """
    if df.empty:
        return df.iloc[0:0]
    out = df.copy()
    # Exclude 999
    out = out[~out["ROUTE_SURVEYED"].map(_is_route_excluded)]
    # Completed only
    out = out[out.apply(_is_completed, axis=1)]
    if route_sel and route_sel != "All":
        out = out[out["ROUTE_SURVEYED"].astype(str).str.strip() == route_sel]
    if initials_sel and initials_sel != "All":
        out = out[out["INTERV_INIT"].astype(str).str.strip() == initials_sel]
    if survey_day is not None:
        def local_day_ok(r) -> bool:
            lt = r.get("LOCAL_TIME")
            if lt is None or (isinstance(lt, float) and pd.isna(lt)):
                return False
            t = pd.to_datetime(lt, errors="coerce")
            if pd.isna(t):
                return False
            return t.date() == survey_day

        out = out[out.apply(local_day_ok, axis=1)]
    if out.empty:
        return df.iloc[0:0]
    return out


def _last_sync_display(df: pd.DataFrame) -> Optional[str]:
    """
    Max SYNCED_AT from cached rows (set at MySQL→Snowflake refresh).
    Same formatting approach as ``tucson_cr.py`` ``create_professional_header`` for
    Last_Sync_Date: America/Chicago, ``%Y-%m-%d %H:%M:%S %Z``.
    Naive timestamps are treated as wall time in America/Chicago (matches main app).
    """
    if df is None or df.empty:
        return None
    s = _col(df, "SYNCED_AT")
    if s is None:
        return None
    parsed = pd.to_datetime(s.astype(str).str.strip(), errors="coerce")
    if parsed.isna().all():
        return None
    ts = parsed.max()
    if pd.isna(ts):
        return None
    try:
        if isinstance(ts, pd.Timestamp):
            last_sync_dt = ts.to_pydatetime()
        else:
            last_sync_dt = ts
        if last_sync_dt.tzinfo is None:
            last_sync_dt = last_sync_dt.replace(tzinfo=TRACKER_SYNC_TZ)
        else:
            last_sync_dt = last_sync_dt.astimezone(TRACKER_SYNC_TZ)
        return last_sync_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return None


def _combined_oride(row: pd.Series) -> str:
    o = _route_str(row.get("ORIGIN_ADDR"))
    r = _route_str(row.get("ROUTE_SURVEYED"))
    d = _route_str(row.get("DEST_ADDR"))
    return f"Origin: {o}\nRoute: {r}\nDestination: {d}"


def render_public_survey_tracker_page(private_key_bytes: bytes, project_code: str) -> None:
    st.markdown(
        """
        <style>
        .tracker-page-outer {
            max-width: 1100px;
            margin: -2rem auto 0 auto;
            padding: 0 0 2.5rem 0;
        }
        .tracker-head {
            margin: 0 0 0.85rem 0;
            padding: 0 0.15rem 0.85rem 0;
            border-bottom: 1px solid #e2e8f0;
        }
        .tracker-head h1.tracker-page-title {
            font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
            font-size: clamp(2.15rem, 6vw, 3.15rem);
            font-weight: 800;
            color: #0f172a;
            margin: 0 0 0.45rem 0;
            padding: 0;
            border: none;
            letter-spacing: -0.045em;
            line-height: 1.05;
        }
        .tracker-project-line {
            font-size: 1.12rem;
            font-weight: 600;
            color: #0c4a6e;
            margin: 0 0 0.45rem 0;
            letter-spacing: -0.02em;
        }
        .tracker-blurb {
            font-size: 0.95rem;
            color: #64748b;
            margin: 0 0 0.5rem 0;
            line-height: 1.45;
            max-width: 40rem;
        }
        .tracker-code-chip {
            display: inline-block;
            font-family: ui-monospace, monospace;
            font-size: 0.78rem;
            background: #f1f5f9;
            border: 1px solid #e2e8f0;
            padding: 0.2rem 0.55rem;
            border-radius: 8px;
            color: #475569;
        }
        .tracker-last-sync {
            font-size: 0.82rem;
            color: #64748b;
            margin: 0.5rem 0 0 0;
            line-height: 1.45;
        }
        .tracker-last-sync strong { color: #0f172a; font-weight: 600; }
        .tracker-section-label {
            font-size: 1.05rem;
            font-weight: 700;
            color: #334155;
            margin: 0 0 0.65rem 0;
            letter-spacing: -0.02em;
        }
        .tracker-hint {
            font-size: 0.85rem;
            color: #64748b;
            margin: 0.5rem 0 0 0;
            line-height: 1.4;
        }
        div[data-testid="stMetricValue"] { font-variant-numeric: tabular-nums; }
        .tracker-stats-loading {
            color: #64748b;
            font-size: 0.95rem;
            padding: 0.5rem 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    pc = (project_code or "").strip()
    if not _PROJECT_CODE_RE.match(pc):
        st.error("Invalid or missing project_code in the URL.")
        st.caption("Use: ?page=tracker&project_code=YOURCODE")
        return

    cfg = load_config_by_code(private_key_bytes, pc)
    if not cfg:
        st.error("No survey tracker configuration found for this project code.")
        return

    code_key = str(cfg.get("PROJECT_CODE") or pc).upper()
    pname = cfg.get("PROJECT_NAME") or code_key

    st.markdown('<div class="tracker-page-outer">', unsafe_allow_html=True)
    # Slot is filled at the end so the stats bar sits above filters. Until then, show a loading
    # stub so the area never goes blank (e.g. after Sync → rerun while Snowflake reloads).
    stats_bar_slot = st.empty()
    with stats_bar_slot.container():
        with st.container(border=True):
            st.markdown(
                '<p class="tracker-stats-loading">Loading dashboard…</p>',
                unsafe_allow_html=True,
            )

    # Single source of truth for the UI: Snowflake cache (refreshed only by the button below).
    df = _normalize_tracker_dataframe(load_rows_from_snowflake(private_key_bytes, code_key))
    total_completed_all = len(apply_filters(df, "All", "All", None))
    last_sync_str = _last_sync_display(df)

    routes = sorted(
        {_route_str(x) for x in df["ROUTE_SURVEYED"].dropna().unique() if not _is_route_excluded(x)},
        key=lambda x: (x == "", x),
    )
    initials = sorted(
        {_route_str(x) for x in df["INTERV_INIT"].dropna().unique()},
        key=lambda x: (x == "", x),
    )

    route_options = ["All"] + routes
    initials_options = ["All"] + initials

    if "tracker_route_sel" not in st.session_state:
        st.session_state["tracker_route_sel"] = "All"
    if "tracker_initials_sel" not in st.session_state:
        st.session_state["tracker_initials_sel"] = "All"
    if "tracker_day" not in st.session_state:
        st.session_state["tracker_day"] = date.today()
    if "tracker_limit_one_day" not in st.session_state:
        # Migrate from older radio-based session
        st.session_state["tracker_limit_one_day"] = (
            st.session_state.get("tracker_date_scope_radio") == "One day"
            if "tracker_date_scope_radio" in st.session_state
            else False
        )

    # Filters first so counts + table match the same session state in this run.
    st.markdown(
        '<p class="tracker-section-label" style="margin-top:0.75rem">Find surveys</p>',
        unsafe_allow_html=True,
    )
    with st.container(border=True):
        r1, r2 = st.columns(2, gap="medium")
        with r1:
            st.selectbox("Route", route_options, key="tracker_route_sel")
        with r2:
            st.selectbox("Survey initials", initials_options, key="tracker_initials_sel")
        st.toggle(
            "Limit to one calendar day (Local time)",
            key="tracker_limit_one_day",
            help="Off = every survey day. On = only the day you pick below (matches the tablet’s Local time).",
        )
        _one_day = bool(st.session_state.get("tracker_limit_one_day", False))
        st.date_input(
            "Day",
            key="tracker_day",
            disabled=not _one_day,
            help="Only used when the switch above is on.",
        )
    st.markdown(
        '<p class="tracker-hint">Completed surveys only · route 999 &amp; test rows removed at sync</p>',
        unsafe_allow_html=True,
    )

    route_state = st.session_state.get("tracker_route_sel", "All")
    initials_state = st.session_state.get("tracker_initials_sel", "All")
    if bool(st.session_state.get("tracker_limit_one_day", False)):
        day_state = st.session_state.get("tracker_day", date.today())
    else:
        day_state = None

    if route_state not in route_options:
        route_state = "All"
    if initials_state not in initials_options:
        initials_state = "All"

    filtered = apply_filters(df, route_state, initials_state, day_state)
    count = len(filtered)

    with stats_bar_slot.container():
        with st.container(border=True):
            st.markdown(
                f"""
                <div class="tracker-head">
                  <h1 class="tracker-page-title">Tablet Survey Progress</h1>
                  <p class="tracker-project-line">{html_escape(str(pname))}</p>
                  <p class="tracker-blurb">Quick read on completed tablet surveys. Numbers follow your last sync — use <strong>Sync</strong> below for the newest data.</p>
                  <span class="tracker-code-chip">Project code · {html_escape(str(code_key))}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            h2, h3, h4 = st.columns([1.1, 1.1, 1], vertical_alignment="center")
            with h2:
                st.metric(
                    "In this project",
                    total_completed_all,
                    help="All completed surveys in the synced snapshot (not affected by the filters further down).",
                )
            with h3:
                st.metric(
                    "Showing now",
                    count,
                    help="What you see in the table — Route, initials, and optional one-day filter.",
                )
            with h4:
                if st.button("Sync / Refresh", type="primary", use_container_width=True):
                    with st.spinner("Syncing from source database…"):
                        try:
                            stats = sync_mysql_to_snowflake(
                                private_key_bytes,
                                code_key,
                                str(cfg["SOURCE_DATABASE_NAME"]),
                                str(cfg["SOURCE_TABLE_NAME"]),
                            )
                            st.success(
                                "Sync completed. "
                                f"Total fetched: **{stats.get('total_fetched', 0)}**, "
                                f"Total test removed (IntervInit=999 or Have5MinForSurveCode!=1): "
                                f"**{stats.get('total_test_removed', 0)}**, "
                                f"Total loaded: **{stats.get('total_loaded', 0)}**."
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"Sync failed: {e}")
                st.caption("Get the newest rows from the field database.")
                _sync_html = (
                    f'<p class="tracker-last-sync">Last sync<br/><strong>{html_escape(last_sync_str)}</strong></p>'
                    if last_sync_str
                    else '<p class="tracker-last-sync">Last sync<br/><em>Not yet — press Sync</em></p>'
                )
                st.markdown(_sync_html, unsafe_allow_html=True)
    if filtered.empty:
        st.warning(
            "Nothing matches yet. Turn **off** “Limit to one calendar day” to see every day, set Route and initials to **All**, "
            "or pick a day that has surveys. **Sync** if you just collected new data."
        )
        st.stop()

    lime_id_series = filtered.get("ID")
    if lime_id_series is None:
        lime_id_series = filtered.get("ROW_ID")
    if lime_id_series is None:
        lime_id_series = pd.Series([""] * len(filtered), index=filtered.index)

    display = pd.DataFrame(
        {
            "No": range(1, len(filtered) + 1),
            "Lime ID": lime_id_series.astype(str),
            "Origin / Route / Destination": filtered.apply(_combined_oride, axis=1),
            "Trip Type": "Main Trip",
            "Survey initials": filtered.get("INTERV_INIT", pd.Series([""] * len(filtered), index=filtered.index)).astype(str),
            "Start date": filtered.get("DATE_STARTED", pd.Series([""] * len(filtered), index=filtered.index)).map(lambda x: _fmt_ts(x)),
            "Completed date": filtered.get("DATE_SUBMITTED", pd.Series([""] * len(filtered), index=filtered.index)).map(lambda x: _fmt_ts(x)),
            "Local time": filtered.get("LOCAL_TIME", pd.Series([""] * len(filtered), index=filtered.index)).map(lambda x: _fmt_ts(x)),
            "Device time": filtered.get("DEVICE_TIME", pd.Series([""] * len(filtered), index=filtered.index)).map(lambda x: _fmt_ts(x)),
        }
    )

    st.markdown('<p class="tracker-section-label" style="margin-top:1.5rem">Survey records</p>', unsafe_allow_html=True)
    st.dataframe(display, use_container_width=True, hide_index=True, height=min(520, 40 + 36 * min(len(display), 12)))

    st.markdown("</div><!-- tracker-page-outer -->", unsafe_allow_html=True)


def _fmt_ts(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    t = pd.to_datetime(x, errors="coerce")
    if pd.isna(t):
        return str(x)
    return t.strftime("%Y-%m-%d %H:%M:%S")


def survey_tracker_setup_page(private_key_bytes: bytes) -> None:
    st.title("Survey tracker setup")
    st.markdown(
        "Configure **project name**, **project code** (public URL), and the **MySQL database / table** to pull from "
        "on each sync. Settings are stored in **Snowflake** (tracker schema). "
        "Surveyors’ page reads only from Snowflake after data is synced; SQL is contacted **only** when they press "
        "Sync / Refresh. Uses `SQL_HOST`, `SQL_USER`, `SQL_PASSWORD` like `utils.fetch_data` / `DatabaseConnector`."
    )

    configs = list_all_configs(private_key_bytes)
    options = ["— New project —"] + [f"{c['PROJECT_NAME']} ({c['PROJECT_CODE']})" for c in configs]
    choice = st.selectbox("Existing projects", options, index=0)

    defaults = {
        "project_name": "",
        "project_code": "",
        "source_db": "",
        "source_table": "",
    }
    if choice != "— New project —":
        idx = options.index(choice) - 1
        c = configs[idx]
        defaults = {
            "project_name": str(c.get("PROJECT_NAME") or ""),
            "project_code": str(c.get("PROJECT_CODE") or ""),
            "source_db": str(c.get("SOURCE_DATABASE_NAME") or ""),
            "source_table": str(c.get("SOURCE_TABLE_NAME") or ""),
        }

    with st.form("tracker_setup_form"):
        project_name = st.text_input("Project name *", value=defaults["project_name"])
        project_code = st.text_input(
            "Project code * (URL segment; letters, digits, . _ -)",
            value=defaults["project_code"],
            help="Example: TUCSON2025 — public URL: ?page=tracker&project_code=TUCSON2025",
        )
        source_db = st.text_input("MySQL database name *", value=defaults["source_db"])
        source_table = st.text_input("MySQL table name *", value=defaults["source_table"])
        submitted = st.form_submit_button("Save", type="primary")

    if submitted:
        pn = (project_name or "").strip()
        pc = (project_code or "").strip()
        dbn = (source_db or "").strip()
        tbn = (source_table or "").strip()
        missing = []
        if not pn:
            missing.append("Project name")
        if not pc:
            missing.append("Project code")
        if not dbn:
            missing.append("Database name")
        if not tbn:
            missing.append("Table name")
        if missing:
            st.error("Please complete all fields (no empty values): " + ", ".join(missing))
            return
        if not _PROJECT_CODE_RE.match(pc):
            st.error("Project code must start with a letter or digit; then letters, digits, ., _, or - only.")
            return
        db_err = validate_mysql_database_name(dbn)
        if db_err:
            st.error(db_err)
            return
        tbl_err = validate_mysql_table_name(tbn)
        if tbl_err:
            st.error(tbl_err)
            return
        try:
            upsert_config(private_key_bytes, pn, pc, dbn, tbn)
            st.success(f"Saved configuration for **{pn}** (`{pc.upper()}`).")
        except Exception as e:
            st.error(f"Save failed: {e}")

    st.markdown("---")
    st.markdown("**Public link for surveyors** (no login):")
    st.code("?page=tracker&project_code=YOUR_PROJECT_CODE", language="text")
