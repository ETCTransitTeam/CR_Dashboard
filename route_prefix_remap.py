"""Per-project route/stop/transfer code prefix conversions (e.g. BUS_2_ -> BUS_1_).

Used by OD sync and Review Cycle. Rows come from APP_CONFIG.PROJECT_ROUTE_PREFIX_MAP.
Not found in the map = leave the value unchanged.
"""
from __future__ import annotations

import os
import re
from typing import Iterable, Sequence

import pandas as pd

# Canonical post-header-map column names (ls6 / KCATA mapping).
ROUTE_PREFIX_TARGET_COLUMNS: tuple[str, ...] = (
    "ROUTE_SURVEYEDCode",
    "STOP_ON_CLNTID",
    "STOP_OFF_CLNTID",
    "TRIP_FIRST_ROUTECode",
    "TRIP_SECOND_ROUTECode",
    "TRIP_THIRD_ROUTECode",
    "TRIP_FOURTH_ROUTECode",
    "TRIP_NEXT_ROUTECode",
    "TRIP_AFTER_ROUTECode",
    "TRIP_3RD_ROUTECode",
    "TRIP_LAST4TH_RTECode",
)

_TRANSFER_STOP_ID_RE = re.compile(
    r"^(PREV|NEXT)_TRAN_\d+_(ON|OFF)_BUS_CLNTID$",
    re.IGNORECASE,
)


def _clean_col(name: str) -> str:
    return (
        str(name)
        .replace("_", "")
        .replace("[", "")
        .replace("]", "")
        .replace(" ", "")
        .replace("#", "")
        .lower()
    )


def normalize_prefix_pairs(pairs: Iterable[tuple[str, str]] | None) -> list[tuple[str, str]]:
    """Dedupe by FROM prefix; longer prefixes first so BUS_2_X_ beats BUS_2_ if both exist."""
    by_from: dict[str, str] = {}
    for raw_from, raw_to in pairs or []:
        frm = str(raw_from or "").strip()
        to = str(raw_to or "").strip()
        if not frm or not to or frm == to:
            continue
        by_from[frm] = to
    return sorted(by_from.items(), key=lambda item: (-len(item[0]), item[0]))


def columns_for_route_prefix_remap(df: pd.DataFrame) -> list:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    targets_clean = {_clean_col(c): c for c in ROUTE_PREFIX_TARGET_COLUMNS}
    found = []
    seen = set()
    for col in df.columns:
        key = str(col)
        if key in seen:
            continue
        if key in ROUTE_PREFIX_TARGET_COLUMNS or _TRANSFER_STOP_ID_RE.match(key):
            found.append(col)
            seen.add(key)
            continue
        cleaned = _clean_col(key)
        if cleaned in targets_clean:
            found.append(col)
            seen.add(key)
    return found


def apply_route_prefix_conversions(
    df: pd.DataFrame,
    pairs: Sequence[tuple[str, str]] | None,
    *,
    context: str = "",
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Rewrite cell values that start with a mapped survey prefix.

    Returns (df, stats) where stats maps "column:FROM->TO" -> rows changed.
    """
    pairs_n = normalize_prefix_pairs(pairs)
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or not pairs_n:
        return df, {}

    stats: dict[str, int] = {}
    for col in columns_for_route_prefix_remap(df):
        series = df[col]
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, 0]
        values = series.astype(str)
        # Leave true nulls alone (astype(str) made them "nan")
        null_mask = series.isna()
        out = values.copy()
        for frm, to in pairs_n:
            mask = (~null_mask) & out.str.startswith(frm, na=False)
            n = int(mask.sum())
            if n == 0:
                continue
            out = out.where(~mask, out.str.replace(f"^{re.escape(frm)}", to, n=1, regex=True))
            stats[f"{col}:{frm}->{to}"] = stats.get(f"{col}:{frm}->{to}", 0) + n
        if not null_mask.all():
            df[col] = out.where(~null_mask, series)

    if stats:
        where = f" ({context})" if context else ""
        total = sum(stats.values())
        print(f"Route prefix convert{where}: {total} cell update(s) across {len(stats)} column/rule hits")
        for key, n in list(stats.items())[:12]:
            print(f"  {key}: {n}")
        if len(stats) > 12:
            print(f"  … {len(stats) - 12} more")
    return df, stats


def load_route_prefix_map_from_db(
    project_name: str,
    *,
    connect_fn=None,
    app_config_schema: str | None = None,
) -> list[tuple[str, str]]:
    """Load (FROM_PREFIX, TO_PREFIX) rows for a project. Empty list if none / table missing."""
    schema = (app_config_schema or os.getenv("APP_CONFIG_SCHEMA", "APP_CONFIG") or "APP_CONFIG").strip()
    if not project_name or connect_fn is None:
        return []
    conn = None
    cur = None
    try:
        conn = connect_fn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT FROM_PREFIX, TO_PREFIX
            FROM {schema}.PROJECT_ROUTE_PREFIX_MAP
            WHERE UPPER(PROJECT_NAME) = UPPER(%s)
            ORDER BY SORT_ORDER, FROM_PREFIX
            """,
            (project_name,),
        )
        rows = cur.fetchall() or []
        return normalize_prefix_pairs((r[0], r[1]) for r in rows)
    except Exception as exc:
        print(f"Route prefix map load skipped for {project_name}: {exc}")
        return []
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def ensure_route_prefix_map_table(cur, app_config_schema: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {app_config_schema}.PROJECT_ROUTE_PREFIX_MAP (
            PROJECT_NAME VARCHAR NOT NULL,
            FROM_PREFIX  VARCHAR NOT NULL,
            TO_PREFIX    VARCHAR NOT NULL,
            SORT_ORDER   INTEGER DEFAULT 0,
            PRIMARY KEY (PROJECT_NAME, FROM_PREFIX)
        )
        """
    )
