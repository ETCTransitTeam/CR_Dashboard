from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.pandas_tools import write_pandas

from core.config import REPO_ROOT, REVIEW_CYCLE_SCHEMA, ROOT_DIR, env

# Columns loaded via write_pandas temp tables must be cast back to TIMESTAMP on MERGE.
_TIMESTAMP_COLUMN_NAMES = frozenset(
    {
        "UPDATED_AT",
        "CREATED_AT",
        "INGESTED_AT",
        "CAPTURED_AT",
        "ASSIGNED_AT",
        "DEFER_UNTIL",
        "SYNCED_AT",
        "LAST_PULL_TS",
        "LAST_OD_SYNC_SEEN",
        "LAST_KINGELVIS_EXPORT_TS",
    }
)

# Always stringify before write_pandas so Snowflake MERGE does not fail on mixed route codes.
_VARCHAR_COLUMN_NAMES = frozenset(
    {
        "ROUTE_SURVEYED_CODE",
        "FINAL_USAGE",
        "FINAL_REVIEWER",
        "INTERV_INIT",
        "TWO_X_REVIEWED_BY",
        "TWO_X_REVIEWED_FLAG",
    }
)

DEFAULT_KEY_PATH = "path/to/key.p8"


def _resolve_key_path() -> Path | None:
    key_path = env("SNOWFLAKE_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)
    resolved = Path(key_path)
    if not resolved.is_absolute():
        for base in (REPO_ROOT, ROOT_DIR):
            candidate = base / resolved
            if candidate.exists():
                return candidate
        return REPO_ROOT / resolved if (REPO_ROOT / resolved).exists() else None
    return resolved if resolved.exists() else None


def _load_private_key_bytes() -> bytes:
    resolved = _resolve_key_path()
    if resolved is None:
        expected = env("SNOWFLAKE_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)
        raise FileNotFoundError(
            f"Snowflake private key not found at {ROOT_DIR / expected}. "
            "Place your encrypted key.p8 there or set SNOWFLAKE_PRIVATE_KEY_PATH."
        )

    passphrase_value = env("SNOWFLAKE_PASSPHRASE")
    passphrase = passphrase_value.encode() if passphrase_value else None
    key_bytes = resolved.read_bytes()

    try:
        private_key = serialization.load_pem_private_key(
            key_bytes,
            password=passphrase,
            backend=default_backend(),
        )
    except TypeError as exc:
        raise ValueError(
            "Private key is encrypted but SNOWFLAKE_PASSPHRASE is missing or empty."
        ) from exc
    except ValueError as exc:
        raise ValueError(
            "Could not decrypt private key. Check SNOWFLAKE_PASSPHRASE and key file."
        ) from exc

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


_private_key_cache: bytes | None = None


def _private_key_bytes() -> bytes:
    global _private_key_cache
    if _private_key_cache is None:
        _private_key_cache = _load_private_key_bytes()
    return _private_key_cache


def keypair_auth_configured() -> bool:
    return _resolve_key_path() is not None and bool(env("SNOWFLAKE_PASSPHRASE"))


def snowflake_auth_mode() -> str:
    if keypair_auth_configured():
        return "keypair-jwt (encrypted)"
    if _resolve_key_path() is not None:
        return "keypair-jwt"
    if env("SNOWFLAKE_PASSWORD"):
        return "password"
    return "unset"


def connect(schema: str | None = None):
    common = {
        "user": env("SNOWFLAKE_USER"),
        "account": env("SNOWFLAKE_ACCOUNT"),
        "warehouse": env("SNOWFLAKE_WAREHOUSE"),
        "database": env("SNOWFLAKE_DATABASE"),
        "schema": schema or REVIEW_CYCLE_SCHEMA,
        "role": env("SNOWFLAKE_ROLE"),
        "network_timeout": 120,
    }

    if keypair_auth_configured() or (_resolve_key_path() is not None and not env("SNOWFLAKE_PASSWORD")):
        return snowflake.connector.connect(
            private_key=_private_key_bytes(),
            authenticator="SNOWFLAKE_JWT",
            **common,
        )

    password = env("SNOWFLAKE_PASSWORD")
    if password:
        return snowflake.connector.connect(password=password, **common)

    raise RuntimeError(
        "Snowflake credentials missing. For encrypted key auth set "
        "SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PASSPHRASE, and place key.p8 in path/to/. "
        "Alternatively set SNOWFLAKE_PASSWORD."
    )


def test_connection() -> str:
    conn = connect(schema="PUBLIC")
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE()")
        row = cur.fetchone()
        cur.close()
        return f"Connected as {row[0]} (role={row[1]}, database={row[2]})"
    finally:
        conn.close()


@contextmanager
def cursor(schema: str | None = None) -> Iterator:
    conn = connect(schema=schema)
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _in_streamlit_runtime() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


_read_connect = None


def _is_auth_token_expired(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "390114" in msg
        or "authentication token has expired" in msg
        or "must authenticate again" in msg
    )


def _invalidate_read_connections() -> None:
    """Drop cached Snowflake read connections so the next query opens a fresh JWT session."""
    global _read_connect
    if _read_connect is not None:
        try:
            _read_connect.clear()
        except Exception:
            pass
    _read_connect = None


def _get_read_connection(schema: str):
    global _read_connect
    import streamlit as st

    if _read_connect is None:

        @st.cache_resource(show_spinner=False, ttl=2700)
        def _connect(schema_name: str):
            return connect(schema=schema_name)

        _read_connect = _connect
    return _read_connect(schema)


def _fetch_df_with_connection(conn, query: str, params) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(query, params or ())
        if cur.description:
            try:
                return cur.fetch_pandas_all()
            except Exception:
                pass
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=columns)
    finally:
        cur.close()


def fetch_df(query: str, params=None, schema: str | None = None) -> pd.DataFrame:
    schema_key = schema or REVIEW_CYCLE_SCHEMA
    if _in_streamlit_runtime():
        last_exc: BaseException | None = None
        for attempt in range(2):
            try:
                conn = _get_read_connection(schema_key)
                return _fetch_df_with_connection(conn, query, params)
            except Exception as exc:
                last_exc = exc
                if attempt == 0 and _is_auth_token_expired(exc):
                    _invalidate_read_connections()
                    continue
                raise
        if last_exc is not None:
            raise last_exc
    with cursor(schema=schema) as cur:
        cur.execute(query, params or ())
        if cur.description:
            try:
                return cur.fetch_pandas_all()
            except Exception:
                pass
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def fetch_df_optional(query: str, params=None, schema: str | None = None) -> pd.DataFrame:
    """Like fetch_df but returns an empty frame when the target table does not exist yet."""
    try:
        return fetch_df(query, params, schema)
    except ProgrammingError as exc:
        msg = str(exc).lower()
        if "does not exist" in msg or "not authorized" in msg:
            return pd.DataFrame()
        raise


def execute(query: str, params=None, schema: str | None = None) -> None:
    with cursor(schema=schema) as cur:
        cur.execute(query, params or ())


def executemany(query: str, params_seq, schema: str | None = None, chunk_size: int = 15000) -> None:
    """Run executemany in chunks (Snowflake caps expressions per statement at 200,000)."""
    params_list = list(params_seq)
    if not params_list:
        return
    with cursor(schema=schema) as cur:
        for start in range(0, len(params_list), chunk_size):
            cur.executemany(query, params_list[start : start + chunk_size])


def append_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str | None = None,
    chunk_size: int = 15000,
) -> int:
    """Append a dataframe in chunks without recreating the target table."""
    if df.empty:
        return 0
    schema = schema or REVIEW_CYCLE_SCHEMA
    conn = connect(schema=schema)
    written = 0
    try:
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            write_pandas(
                conn,
                chunk,
                table_name.upper(),
                schema=schema,
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
            )
            written += len(chunk)
        conn.commit()
    finally:
        conn.close()
    return written


def write_table(df: pd.DataFrame, table_name: str, schema: str | None = None) -> None:
    """Full-table overwrite. Retained for snapshot tables only.

    Prefer merge_upsert() for RECORDS/COMBINED_CHECKS/etc. so that the table
    definition (VARIANT columns, primary keys) and prior user edits survive.
    """
    if df.empty:
        return
    conn = connect(schema=schema)
    try:
        write_pandas(
            conn,
            df,
            table_name.upper(),
            schema=schema or REVIEW_CYCLE_SCHEMA,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
    finally:
        conn.close()


def append_rows(df: pd.DataFrame, table_name: str, schema: str | None = None) -> int:
    """Append rows to an existing table without dropping/recreating it."""
    if df.empty:
        return 0
    conn = connect(schema=schema)
    try:
        write_pandas(
            conn,
            df,
            table_name.upper(),
            schema=schema or REVIEW_CYCLE_SCHEMA,
            auto_create_table=True,
            overwrite=False,
            quote_identifiers=False,
        )
    finally:
        conn.close()
    return len(df)


def _prepare_timestamp_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, set[str]]:
    """Stringify datetimes so write_pandas does not store epoch integers."""
    df = df.copy()
    timestamp_cols: set[str] = set()
    for col in df.columns:
        is_ts_name = col in _TIMESTAMP_COLUMN_NAMES
        is_ts_dtype = pd.api.types.is_datetime64_any_dtype(df[col])
        if not is_ts_name and not is_ts_dtype:
            continue
        timestamp_cols.add(col)
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return df, timestamp_cols


def _prepare_varchar_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce known text columns to strings (e.g. TAM_1_1_00 route codes)."""
    df = df.copy()
    for col in df.columns:
        if col not in _VARCHAR_COLUMN_NAMES:
            continue
        df[col] = df[col].apply(
            lambda v: None
            if v is None or (isinstance(v, float) and pd.isna(v))
            else str(v).strip() or None
        )
    return df


def merge_upsert(
    df: pd.DataFrame,
    table_name: str,
    key_columns: Sequence[str],
    update_columns: Sequence[str] | None = None,
    insert_only: bool = False,
    variant_columns: Sequence[str] | None = None,
    schema: str | None = None,
) -> int:
    """Incremental MERGE upsert that preserves the target table definition.

    - Rows whose keys are new are INSERTed.
    - Matched rows are UPDATEd only on ``update_columns`` (all non-key columns when
      ``update_columns`` is None), unless ``insert_only`` is set, in which case matched
      rows are left untouched. This is how weekly pipeline re-runs avoid clobbering
      cleaning/review edits already stored for existing records.
    - ``variant_columns`` are wrapped with TRY_PARSE_JSON so JSON strings land in
      VARIANT columns as parsed objects.
    """
    if df.empty:
        return 0
    schema = schema or REVIEW_CYCLE_SCHEMA
    df = df.copy()
    df.columns = [str(c).upper() for c in df.columns]
    df = _prepare_varchar_columns(df)
    df, timestamp_cols = _prepare_timestamp_columns(df)
    columns = list(df.columns)
    key_cols = [k.upper() for k in key_columns]
    variant = {c.upper() for c in (variant_columns or [])}
    tmp_name = f"TMP_MERGE_{uuid.uuid4().hex[:16]}".upper()

    def src_ref(col: str) -> str:
        if col in variant:
            return f"TRY_PARSE_JSON(src.{col})"
        if col in timestamp_cols:
            return f"TO_TIMESTAMP_NTZ(src.{col})"
        return f"src.{col}"

    conn = connect(schema=schema)
    try:
        write_pandas(
            conn,
            df,
            tmp_name,
            schema=schema,
            auto_create_table=True,
            overwrite=True,
            table_type="transient",
            quote_identifiers=False,
        )
        on_clause = " AND ".join(f"tgt.{k} = src.{k}" for k in key_cols)
        non_key = [c for c in columns if c not in key_cols]
        upd_cols = [c.upper() for c in update_columns] if update_columns is not None else non_key
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(src_ref(c) for c in columns)
        merge = (
            f"MERGE INTO {schema}.{table_name.upper()} tgt "
            f"USING {schema}.{tmp_name} src ON {on_clause} "
        )
        if not insert_only and upd_cols:
            set_clause = ", ".join(f"tgt.{c} = {src_ref(c)}" for c in upd_cols)
            merge += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        merge += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        cur = conn.cursor()
        try:
            cur.execute(merge)
            affected = cur.rowcount or 0
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{tmp_name}")
            cur.close()
        conn.commit()
        return affected
    finally:
        conn.close()
