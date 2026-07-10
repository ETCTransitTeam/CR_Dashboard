# -*- coding: utf-8 -*-
"""
improved_auto_approval.py
=========================
Improved auto-approval logic matching Power Query M code from the HRTVA pipeline.
Provides the same high-level functionality as auto_and_suggestions_more_transfers.py,
with approval rules aligned to the M code.

Functionality parity with auto_and_suggestions_more_transfers.py:
  - Data preparation: header mapping (ls6->ls2), check_home_airport_hotel, MERGED coords, PBIX renames.
  - Full-record output (default): Final_Usage (Use/Remove), FINAL_REVIEWER, 1st Cleaner,
    REASON FOR REMOVAL, POSSIBLE ERRORS, PATTERN_NUMBER, PREV/NEXT_TRANSFER_COUNT, ZIGZAG_FLAG,
    APPROVAL_BASIS, Supervisor View Only; same semantics as original.
  - Remove logic: Field Approved (ElvisStatus), Test (INTERV_INIT=999), No 5 MIN, Origin=Destination=Home.
  - Use logic: improved M-code pipeline (Phase1/Phase2 -> (01)(02)(03)) instead of 17-pattern check.
  - Transfer suggestions: When run_transfer_suggestions=True and stops_path is provided, runs
    the same logic as auto_and_suggestions_more_transfers (_run_transfer_chain for PREV and NEXT),
    builds suggested_transfers_df, and merges SUGGESTED_PREV_* / SUGGESTED_NEXT_* columns into
    the full output.

Sources:
  - exported_m_code.txt: Primary source of truth for approval business rules
  - exported_table_structures.txt: Column names and data types
  - auto_and_suggestions_more_transfers.py: Transfer suggestion logic (preserved exactly)
  - Flow diagram: Pipeline sequence and decision points

Pipeline sequence (from M code; matches Queries structure in PBIX):
  Walk-Walk Auto Approve Checks [7]: Walk-Walk [No Transfers], [1 PREV], [2 PREV], [1 NEXT], [2 NEXT], [1 PREV - 1 NEXT] -> Phase1_Walk-Walk.
  NOTwalk-Walk Auto Approve Checks [11]: NOTwalk-Walk / Walk-NOTwalk [No Transfers], [1/2 PREV], [1/2 NEXT] -> Phase2_NotWalkOptions.
  Combine-Approved [1]: Phase-All = Phase1_Walk-Walk + Phase2_NotWalkOptions.
  OVERALL FLAGS AND CHECKS [3]:
    (01) OD [DISTANCE TRANSFERS CHECK]: filter where DISTANCE TRANSFER CHECK = 0
    (02) OD [TRANSFERS CHECK]: filter where OD [TRANSFERS CHECK] FLAGS <> 1
    (03) OD [TRIP DISTANCE CHECK]: filter where OD [DISTANCE CHECK] FLAGS <> 1 (matches query_03_trip_distance_check)
  Other: _auto-approved (id, GROUP), OD [Supervisor Only Checks] -> Supervisor View Only.

CLI: each run writes ``improved_auto_approval_pipeline_logic.txt`` (thresholds, Phase 1/2, (01)(02)(03), xfer details)
next to ``-o`` output or in the working directory unless ``--no-logic-txt`` or ``--logic-txt PATH``.

M code alignment (exported_m_code.txt):
  - Thresholds match M: 0.10 mi transfer flag; 2.00/0.20/0.25 O-B and A-D; 0.05/0.25/75 O-D; 1.75/0.35 B2A/OD; 1.85 PREV/NEXT walk; 3959 earth radius; SHORT/MEDIUM/LONG.
  - (02) PREV/NEXT transfer count vs TRIP_* route count, duplicate transfers, FLAGS <> 1.
  - (03) O-B_Dist_Check2/A-D_Dist_Check2 use SHORT=false AND MEDIUM=false. OD [DISTANCE CHECK] FLAGS excludes PREV/NEXT transfer walk flags (same as M filter).
  - (03) ORIGIN_TO_BOARD / ALIGHTING_TO_DESTINATION use transfer-aware endpoints when prev/next>0 (PREV{n}_OFF→STOP_ON; NEXT{n}_OFF→merged D), not merged origin nor surveyed STOP_OFF alone for those legs.
  - (01) Full parity: Transfer1..8_Distance, TRANSFER FLAG DISTANCE, Transfer*_onroute_Distance, TRANSFER_onroute_FLAG, #ofTransferGPS, #ofTranfers, # OF TRANSFER POINT CHECK, DISTANCE TRANSFER CHECK.

Thresholds (from M code):
  - Transfer leg distance flag: > 0.10 miles (exported_m_code.txt TRANSFER FLAG DISTANCE)
  - O-B walk (SHORT, no prev): > 2.00 miles => flag
  - O-B non-SHORT/MEDIUM: < 0.20 miles => flag
  - O-B with prev transfer: < 0.25 miles => flag
  - A-D same pattern: 2.00, 0.20, 0.25
  - O-D: < 0.05, < 0.25, > 75 => flag
  - B2A/OD: > 1.75 or (SHORT+SHORT, no loop, < 0.35) => flag
  - PREV/NEXT transfer walk: > 1.85 miles (Walk/Wheelchair/Skateboard) => flag
  - Earth radius for haversine: 3959 miles (M code Number.Acos(...)*3959)

Input preparation (same as auto_and_suggestions_more_transfers.py):
  - Input: Elvis export CSV (e.g. from SELECT * FROM elvis_transit_ls6_733524_export_odbc) or path to that CSV.
  - mapping_file: Excel with Headers-ls6 -> FormattedHeader-ls2 (e.g. request_20250708_ls6tols2-headers.xlsx).
  - details_file: Details Excel for home/airport/hotel (e.g. details_lacmta-feeder_733524_od_excel.xlsx).
    Stop grids and __(01) xfer_list use worksheet names exactly: 'STOPS' and 'XFER_STOPS' (either or both).
  When mapping_file and/or details_file are provided, data is prepared to PBIX/exported_table_structures column
  names (MERGED_ORIGIN_ADDRESS [LAT], STOP_ON [LAT], PREV_TRANSFERS[Code], ORIGIN_Transport_Mode, etc.) before
  the approval pipeline runs.

Created: 2025-03-17
"""

from __future__ import annotations

import copy
import logging
import math
import os
import re
import sys
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Section 2: Import Statements
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Optional: transfer suggestion (use suggest_transfer_routes directly; avoids importing auto_and_suggestions_more_transfers)
try:
    from suggest_transfer_routes import (
        find_most_suitable_route,
        load_stops_df,
        haversine_miles as suggest_haversine_miles,
        is_walk as suggest_is_walk,
        resolve_elvis_columns as suggest_resolve_elvis_columns,
    )
    TRANSFER_SUGGESTION_AVAILABLE = True
except ImportError as e:
    TRANSFER_SUGGESTION_AVAILABLE = False
    suggest_haversine_miles = None
    suggest_is_walk = None
    suggest_resolve_elvis_columns = None
    logger.warning("Transfer suggestion unavailable: %s", e)

# Optional: data preparation (same as auto_and_suggestions_more_transfers.py)
try:
    from utils import check_home_airport_hotel
    from helper import details_dataframe, check_all_characters_present
    from constants import home_airport_hotel_column_names
    _PREP_DEPS_AVAILABLE = True
except ImportError:
    _PREP_DEPS_AVAILABLE = False

try:
    from power_query_od_pipeline import _is_null_m, _nz
    from power_query_m_checks_supervisor import (
        GPS_COUNT_COLS,
        _add_xfer_list_merges,
        build_xfer_list as _pq_build_xfer_list,
    )
except ImportError:  # pragma: no cover
    _is_null_m = None  # type: ignore[misc, assignment]
    _nz = None  # type: ignore[misc, assignment]
    GPS_COUNT_COLS = []  # type: ignore[misc, assignment]
    _add_xfer_list_merges = None  # type: ignore[misc, assignment]
    _pq_build_xfer_list = None  # type: ignore[misc, assignment]

# ---------------------------------------------------------------------------
# Section 3: Configuration and Thresholds (FROM M CODE)
# ---------------------------------------------------------------------------

# FROM M CODE: __(01) OD [DISTANCE TRANSFERS CHECK] - Transfer*_Distance > 0.10 then flag
TRANSFER_DISTANCE_FLAG_MILES = 0.10

# FROM M CODE: __(03) OD [TRIP DISTANCE CHECK] - O-B, A-D checks
ORIGIN_TO_BOARD_MAX_WALK_MILES = 2.00   # O-B_Dist_Check1: SHORT and no prev
ORIGIN_TO_BOARD_MIN_NON_WALK_MILES = 0.20  # O-B_Dist_Check2: not SHORT and not MEDIUM
ORIGIN_TO_BOARD_MIN_WITH_PREV_MILES = 0.25  # O-B_Dist_Check3: prev transfer
ALIGHTING_TO_DEST_MAX_WALK_MILES = 2.00   # A-D_Dist_Check1
ALIGHTING_TO_DEST_MIN_NON_WALK_MILES = 0.20  # A-D_Dist_Check2
ALIGHTING_TO_DEST_MIN_WITH_NEXT_MILES = 0.25  # A-D_Dist_Check3

# FROM M CODE: O-D checks
ORIGIN_TO_DEST_MIN_VERY_CLOSE = 0.05   # O-D_Dist_Check1
ORIGIN_TO_DEST_MIN_CLOSE = 0.25       # O-D_Dist_Check2
ORIGIN_TO_DEST_MAX_FAR = 75.0         # O-D_Dist_Check3

# FROM M CODE: B-A ratio checks
B2A_OD_RATIO_MAX = 1.75   # B-A_Dist_Check1
B2A_OD_RATIO_MIN_SHORT_SHORT = 0.35   # B-A_Dist_Check2 (SHORT+SHORT, no loop, 0 prev, 0 next)

# FROM M CODE: PREV/NEXT transfer walk distance
PREV_NEXT_TRANSFER_WALK_FLAG_MILES = 1.85

# FROM M CODE: Earth radius in miles (Number.Acos(...)*3959)
EARTH_RADIUS_MILES = 3959.0

# Transport mode categories (from TRANSPORT-CODE: WALK/WHEELCHAIR=SHORT, BIKE/SCOOTER=MEDIUM, DRIVE=LONG)
TRANSPORT_SHORT = "SHORT"
TRANSPORT_MEDIUM = "MEDIUM"
TRANSPORT_LONG = "LONG"

# PREV_TRANSFERS[Code] / NEXT_TRANSFERS[Code] as string in M code
PREV_NEXT_CODE_ZERO = "0"

# Required columns for pipeline (from exported_table_structures / M code)
REQUIRED_ID = "id"
COL_ORIGIN_PLACE_TYPE = "ORIGIN_PLACE_TYPE"
COL_DESTIN_PLACE_TYPE = "DESTIN_PLACE_TYPE"
COL_MERGED_ORIGIN_LAT = "MERGED_ORIGIN_ADDRESS [LAT]"
COL_MERGED_ORIGIN_LONG = "MERGED_ORIGIN_ADDRESS [LONG]"
COL_MERGED_DESTIN_LAT = "MERGED_DESTIN_ADDRESS [LAT]"
COL_MERGED_DESTIN_LONG = "MERGED_DESTIN_ADDRESS [LONG]"
COL_ORIGIN_TRANSPORT_MODE = "ORIGIN_Transport_Mode"
COL_DESTIN_TRANSPORT_MODE = "DESTIN_Transport_Mode"
COL_ORIGIN_TRANSPORT = "ORIGIN_TRANSPORT"
COL_DESTIN_TRANSPORT = "DESTIN_TRANSPORT"
COL_GROUP = "GROUP"
COL_PREV_TRANSFERS_CODE = "PREV_TRANSFERS[Code]"
COL_NEXT_TRANSFERS_CODE = "NEXT_TRANSFERS[Code]"
COL_ROUTE_SURVEYED_CODE = "ROUTE_SURVEYED[Code]"
COL_TRIP_FIRST_ROUTE = "TRIP_FIRST_ROUTE[Code]"
COL_TRIP_SECOND_ROUTE = "TRIP_SECOND_ROUTE[Code]"
COL_TRIP_THIRD_ROUTE = "TRIP_THIRD_ROUTE[Code]"
COL_TRIP_FOURTH_ROUTE = "TRIP_FOURTH_ROUTE[Code]"
COL_TRIP_NEXT_ROUTE = "TRIP_NEXT_ROUTE[Code]"
COL_TRIP_AFTER_ROUTE = "TRIP_AFTER_ROUTE[Code]"
COL_TRIP_3RD_ROUTE = "TRIP_3RD_ROUTE[Code]"
COL_TRIP_LAST4TH_RTE = "TRIP_LAST4TH_RTE[Code]"
COL_SUPERVISOR_VIEW_ONLY = "Supervisor View Only"

# Columns from Elvis/ls2 used for Remove/Use (same as auto_and_suggestions_more_transfers.py)
COL_ELVIS_STATUS = "ElvisStatus"
COL_INTERV_INIT = "INTERV_INIT"
COL_HAVE_5_MIN = "HAVE_5_MIN_FOR_SURVECode"

# Field Approved: same as auto_and_suggestions_more_transfers.py (pre-compiled regex)
_RE_APPROVED = re.compile(r"approved", re.I)

# Alias column names (OG dataset may use suffixes like _Code_ or _LAT_)
ALIAS_ORIGIN_LAT = "ORIGIN_ADDRESS [LAT]"
ALIAS_ORIGIN_LONG = "ORIGIN_ADDRESS [LONG]"
ALIAS_DESTIN_LAT = "DESTIN_ADDRESS [LAT]"
ALIAS_DESTIN_LONG = "DESTIN_ADDRESS [LONG]"
ALIAS_STOP_ON_LAT = "STOP_ON [LAT]"
ALIAS_STOP_ON_LONG = "STOP_ON [LONG]"
ALIAS_STOP_OFF_LAT = "STOP_OFF [LAT]"
ALIAS_STOP_OFF_LONG = "STOP_OFF [LONG]"

# PREV/NEXT transfer bus coords (M code naming)
PREV_TRAN_LAT_LONG_PATTERN = "PREV_TRAN_{}_ON_BUS [LAT]"  # 1..4
NEXT_TRAN_LAT_LONG_PATTERN = "NEXT_TRAN_{}_ON_BUS [LAT]"

# VAL_DIST columns for trip distance check (optional; if missing we use computed distances)
COL_VAL_DIST_OTO_PRE0 = "VAL_DIST_OtoPre0"
COL_VAL_DIST_NOFF_TO_D = "VAL_DIST_NOFF_TO_D"
COL_FINAL_DIRECTION = "FINAL_DIRECTION"


# ---------------------------------------------------------------------------
# Section 4: Preserved Transfer Suggestion (FROM ORIGINAL SCRIPT)
# Transfer suggestion is invoked via imports from auto_and_suggestions_more_transfers
# and suggest_transfer_routes. No modifications; behavior preserved exactly.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Section 5: Utility Functions
# ---------------------------------------------------------------------------

def fn_haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Haversine distance in miles. FROM M CODE: Number.Acos(Number.Sin(lat1_rad)*Number.Sin(lat2_rad)+...)*3959
    """
    try:
        if any(pd.isna(x) for x in (lat1, lon1, lat2, lon2)):
            return float("nan")
        lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return EARTH_RADIUS_MILES * c
    except (TypeError, ValueError):
        return float("nan")


def fn_acos_distance_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Distance in miles using the same spherical law of cosines formula as the PBIX M code:
    Number.Acos(Number.Sin(lat1_rad) * Number.Sin(lat2_rad) +
                Number.Cos(lat1_rad) * Number.Cos(lat2_rad) * Number.Cos(lon2_rad-lon1_rad)) * 3959
    """
    try:
        if any(pd.isna(x) for x in (lat1, lon1, lat2, lon2)):
            return float("nan")
        lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
        cos_val = math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)
        # Numerical safety: acos argument must be in [-1, 1]
        cos_val = max(-1.0, min(1.0, cos_val))
        return EARTH_RADIUS_MILES * math.acos(cos_val)
    except (TypeError, ValueError):
        return float("nan")


def _safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    """Coerce value to float; return default for invalid."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    try:
        v = float(val)
        return v if not pd.isna(v) else default
    except (TypeError, ValueError):
        return default


def _detect_tabular_delimiter(path: str) -> str:
    """
    Best-effort delimiter detection using the first header line.

    Handles cases like `id\tCompleted\tLast_page\t...` where the input is actually TSV.
    """
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            header = f.readline()
    except OSError:
        return ","

    if not header:
        return ","

    # Candidate delimiters to handle common exports (comma, tab, semicolon, pipe).
    candidates = [",", "\t", ";", "|"]
    counts = {sep: header.count(sep) for sep in candidates}
    best_sep = max(counts, key=counts.get)
    return best_sep if counts[best_sep] > 0 else ","


def _read_csv_auto_delimiter(path: str) -> pd.DataFrame:
    """Read CSV/TSV with delimiter autodetection + retry."""
    detected = _detect_tabular_delimiter(path)
    retry_order = [detected] + [s for s in [",", "\t", ";", "|"] if s != detected]
    logger.info("Auto-delimiter for %s: %r (retry order: %s)", os.path.basename(path), detected, retry_order)

    last_exc: Optional[Exception] = None
    for sep in retry_order:
        try:
            df = pd.read_csv(path, sep=sep, low_memory=False)
            # If we guessed wrong and got a single column, try again.
            if df is not None and len(df.columns) <= 1 and sep != "|":
                # Some malformed files may legitimately be 1 column, but for your case
                # (tab-separated headers) this catches the common delimiter mismatch.
                continue
            return df
        except Exception as e:
            last_exc = e
            continue

    # Re-raise the last failure with context.
    if last_exc is not None:
        raise last_exc
    return pd.read_csv(path, low_memory=False)


def _norm_col_for_fallback(x: Any) -> str:
    """
    Normalize a column name for resilient fallback-rename matching.

    Goal: match headers that vary by casing, whitespace, and underscore patterns
    (common in exported files like your TSV).
    """
    s = "" if x is None else str(x)
    s = s.replace("\ufeff", "").replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.strip("_")
    # Collapse repeated underscores.
    s = re.sub(r"_+", "_", s)
    # Normalize "..._Code_" variants to "...Code..."
    s = re.sub(r"_+Code_?", "Code", s, flags=re.IGNORECASE)
    return s.lower()


def _apply_ls6_fallback_renames(
    df: pd.DataFrame,
    fallback_renames: Dict[str, str],
) -> Tuple[pd.DataFrame, int, Dict[str, str]]:
    """
    Apply LS6_FALLBACK_RENAMES using normalized matching.

    Unlike the old exact-match approach, this will rename columns even if the
    input header differs slightly (case/underscores/trailing underscore patterns).
    """
    out = df.copy()
    # Build normalized lookup: normalized_source_header -> target_header
    norm_to_target: Dict[str, str] = {}
    for src, target in fallback_renames.items():
        norm_to_target[_norm_col_for_fallback(src)] = target

    rename: Dict[str, str] = {}
    for col in list(out.columns):
        norm = _norm_col_for_fallback(col)
        target = norm_to_target.get(norm)
        if target and target not in out.columns and col in out.columns:
            rename[col] = target

    if rename:
        out = out.rename(columns=rename)
    return out, len(rename), rename


def _normalize_input_headers(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, Dict[str, str]]:
    """
    Normalize raw export headers (your TSV/CSV) to a more consistent naming style.

    Rules:
    - *_Code_*  -> *Code
    - *_Other_* -> *Other
    - trim leading/trailing underscores
    - collapse repeated underscores
    """
    out = df.copy()
    new_cols: List[str] = []
    rename: Dict[str, str] = {}

    for c in list(out.columns):
        s = "" if c is None else str(c)
        s = s.replace("\ufeff", "").replace("\u00a0", " ").strip()
        s2 = s
        s2 = re.sub(r"_Code_", "Code", s2, flags=re.IGNORECASE)
        s2 = re.sub(r"_Other_", "Other", s2, flags=re.IGNORECASE)
        s2 = s2.strip("_")
        s2 = re.sub(r"_+", "_", s2)
        if s2 != s:
            # Avoid collisions: if the normalized name already exists, skip this rename.
            if s2 in out.columns:
                continue
            rename[s] = s2

    if rename:
        out = out.rename(columns=rename)
    changed = len(rename)
    return out, changed, rename


def _norm_route_id_token(v: Any) -> str:
    """Strict PQ parity: split route id on '_' and keep first token."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if not s:
        return ""
    return s.split("_", 1)[0].strip()


def _text_contains_short(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.upper().str.contains("SHORT", regex=False, na=False)


def _compute_clean_prereq_columns(clean_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add prerequisite columns needed for Phase1/Phase2 strict alignment:
    - ORIGIN_TO_DESTIN (miles)
    - ORIGIN2Transfer1_Distance (miles): origin->STOP_ON when PREV=0 else origin->PREV_TRAN_1_ON_BUS
    - DESTIN2Transfer1_Distance (miles): destin->STOP_OFF when NEXT=0 else destin->NEXT_TRAN_1_OFF_BUS
    - O2DtoFIRSTBLASTA, O2DtoFIRSTBLASTA_value (ratio check using STOP_ON->STOP_OFF as FirstBoard2LastAlight proxy)
    """
    df = _ensure_clean_dataset_columns(clean_df)

    # transport mode categories
    if "ORIGIN_Transport_Mode" not in df.columns:
        df["ORIGIN_Transport_Mode"] = df.get(COL_ORIGIN_TRANSPORT, "").apply(_transport_mode_category)
    if "DESTIN_Transport_Mode" not in df.columns:
        df["DESTIN_Transport_Mode"] = df.get(COL_DESTIN_TRANSPORT, "").apply(_transport_mode_category)

    # Distances
    # M-code uses acos(sin*...+cos*...)*3959. Use the same here for parity near thresholds.
    def _hv(lat1, lon1, lat2, lon2) -> float:
        return fn_acos_distance_miles(lat1, lon1, lat2, lon2)

    df["ORIGIN_TO_DESTIN"] = df.apply(
        lambda r: _hv(
            _safe_float(r.get(COL_MERGED_ORIGIN_LAT)),
            _safe_float(r.get(COL_MERGED_ORIGIN_LONG)),
            _safe_float(r.get(COL_MERGED_DESTIN_LAT)),
            _safe_float(r.get(COL_MERGED_DESTIN_LONG)),
        ),
        axis=1,
    )

    prev_code = df.get(COL_PREV_TRANSFERS_CODE, pd.Series("0", index=df.index)).fillna("0").astype(str).str.strip()
    next_code = df.get(COL_NEXT_TRANSFERS_CODE, pd.Series("0", index=df.index)).fillna("0").astype(str).str.strip()

    # Resolve STOP/PREV/NEXT coordinate columns robustly (PBIX + ls2 suffix + ls6 fallbacks)
    C = _resolve_transfer_coord_columns(df)
    stop_on_lat = C.get("STOP_ON_LAT") or "STOP_ON [LAT]"
    stop_on_lon = C.get("STOP_ON_LON") or "STOP_ON [LONG]"
    stop_off_lat = C.get("STOP_OFF_LAT") or "STOP_OFF [LAT]"
    stop_off_lon = C.get("STOP_OFF_LON") or "STOP_OFF [LONG]"
    prev1_on_lat = C.get("PREV1_ON_LAT") or "PREV_TRAN_1_ON_BUS [LAT]"
    prev1_on_lon = C.get("PREV1_ON_LON") or "PREV_TRAN_1_ON_BUS [LONG]"
    next1_off_lat = C.get("NEXT1_OFF_LAT") or "NEXT_TRAN_1_OFF_BUS [LAT]"
    next1_off_lon = C.get("NEXT1_OFF_LON") or "NEXT_TRAN_1_OFF_BUS [LONG]"

    origin2 = []
    destin2 = []
    for _, r in df.iterrows():
        o_lat, o_lon = _safe_float(r.get(COL_MERGED_ORIGIN_LAT)), _safe_float(r.get(COL_MERGED_ORIGIN_LONG))
        d_lat, d_lon = _safe_float(r.get(COL_MERGED_DESTIN_LAT)), _safe_float(r.get(COL_MERGED_DESTIN_LONG))
        # choose first board: STOP_ON or PREV_TRAN_1_ON_BUS
        if str(r.get(COL_PREV_TRANSFERS_CODE, "0")).strip() == "0":
            b_lat, b_lon = _safe_float(r.get(stop_on_lat)), _safe_float(r.get(stop_on_lon))
        else:
            b_lat, b_lon = _safe_float(r.get(prev1_on_lat)), _safe_float(r.get(prev1_on_lon))
        # choose last alight: STOP_OFF or NEXT_TRAN_1_OFF_BUS
        if str(r.get(COL_NEXT_TRANSFERS_CODE, "0")).strip() == "0":
            a_lat, a_lon = _safe_float(r.get(stop_off_lat)), _safe_float(r.get(stop_off_lon))
        else:
            a_lat, a_lon = _safe_float(r.get(next1_off_lat)), _safe_float(r.get(next1_off_lon))

        origin2.append(_hv(o_lat, o_lon, b_lat, b_lon))
        destin2.append(_hv(a_lat, a_lon, d_lat, d_lon))

    df["ORIGIN2Transfer1_Distance"] = pd.to_numeric(pd.Series(origin2, index=df.index), errors="coerce")
    df["DESTIN2Transfer1_Distance"] = pd.to_numeric(pd.Series(destin2, index=df.index), errors="coerce")

    # FirstBoard2LastAlight proxy
    df["FirstBoard2LastAlight_Distance"] = df.apply(
        lambda r: _hv(_safe_float(r.get("STOP_ON [LAT]")), _safe_float(r.get("STOP_ON [LONG]")), _safe_float(r.get("STOP_OFF [LAT]")), _safe_float(r.get("STOP_OFF [LONG]"))),
        axis=1,
    )
    ratio = df["FirstBoard2LastAlight_Distance"] / df["ORIGIN_TO_DESTIN"].replace(0, np.nan)
    allowed = 1 + (2 / df["ORIGIN_TO_DESTIN"].replace(0, np.nan))
    df["O2DtoFIRSTBLASTA_value"] = ratio
    df["O2DtoFIRSTBLASTA"] = (ratio > allowed).fillna(False).astype(int)

    # Ensure PREV/NEXT code columns are strings like PQ
    df[COL_PREV_TRANSFERS_CODE] = prev_code
    df[COL_NEXT_TRANSFERS_CODE] = next_code
    return df


def _fn_count_stops_on_route_within_distance(
    stops_df: Optional[pd.DataFrame],
    lat: float,
    lon: float,
    threshold_miles: float,
    route_code: str,
) -> int:
    """PowerQuery fnCountStopsOnRouteWithinDistance parity (HalfThreshold = threshold/2)."""
    if stops_df is None or stops_df.empty:
        return 0
    try:
        thr = float(threshold_miles)
        if not (thr > 0):
            return 0
    except (TypeError, ValueError):
        return 0
    half = thr / 2.0

    lat_col = "stop_lat6" if "stop_lat6" in stops_df.columns else ("stop_lat" if "stop_lat" in stops_df.columns else None)
    lon_col = "stop_lon6" if "stop_lon6" in stops_df.columns else ("stop_lon" if "stop_lon" in stops_df.columns else None)
    route_col = "ETC_ROUTE_ID" if "ETC_ROUTE_ID" in stops_df.columns else ("XFER_ROUTE_ID" if "XFER_ROUTE_ID" in stops_df.columns else ("route_id" if "route_id" in stops_df.columns else None))
    if lat_col is None or lon_col is None or route_col is None:
        return 0

    rc = _norm_route_id_token(route_code)
    if not rc:
        return 0
    df = stops_df[stops_df[route_col].map(_norm_route_id_token) == rc]
    if df.empty:
        return 0

    df = df.copy()
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    lat_min, lat_max = lat - half, lat + half
    lon_min, lon_max = lon - half, lon + half
    box = df[(df[lat_col] >= lat_min) & (df[lat_col] <= lat_max) & (df[lon_col] >= lon_min) & (df[lon_col] <= lon_max)]
    if box.empty:
        return 0
    # M-code parity: fnCountStopsOnRouteWithinDistance uses fnHaversineMiles (not acos).
    d = box.apply(
        lambda r: fn_haversine_miles(
            lat,
            lon,
            _safe_float(r.get(lat_col)),
            _safe_float(r.get(lon_col)),
        )
        or 999999.0,
        axis=1,
    )
    return int((d <= half).sum())


def _compute_number_of_nearby_stops(
    clean_df: pd.DataFrame,
    og_df: pd.DataFrame,
    stops_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Implements OD [number_of_nearby_stops] from the Phase txts:
    - FirstROUTE: TRIP_FIRST_ROUTE[Code] else ROUTE_SURVEYED[Code] (trim before '_')
    - LastROUTE: TRIP_LAST4TH_RTE[Code] else TRIP_3RD else TRIP_AFTER else TRIP_NEXT else ROUTE_SURVEYED[Code]
    - ORIGIN_fnCountStopsOnRouteWithinDistance(lat, lon, ORIGIN2Transfer1_Distance, FirstROUTE)
    - DESTIN_fnCountStopsOnRouteWithinDistance(lat, lon, DESTIN2Transfer1_Distance, LastROUTE)
    """
    base = clean_df[[REQUIRED_ID, COL_MERGED_ORIGIN_LAT, COL_MERGED_ORIGIN_LONG, COL_MERGED_DESTIN_LAT, COL_MERGED_DESTIN_LONG, "ORIGIN2Transfer1_Distance", "DESTIN2Transfer1_Distance"]].copy()
    merged = base.merge(
        og_df[[c for c in [
            REQUIRED_ID,
            COL_ROUTE_SURVEYED_CODE,
            COL_TRIP_FIRST_ROUTE, COL_TRIP_SECOND_ROUTE, COL_TRIP_THIRD_ROUTE, COL_TRIP_FOURTH_ROUTE,
            COL_TRIP_NEXT_ROUTE, COL_TRIP_AFTER_ROUTE, COL_TRIP_3RD_ROUTE, COL_TRIP_LAST4TH_RTE,
        ] if c in og_df.columns]],
        on=REQUIRED_ID,
        how="left",
    )

    # M-code parity: ROUTE_SURVEYED[Code] is truncated before '_' earlier in the export logic,
    # but TRIP_*_ROUTE[Code] values are used as-is inside OD [number_of_nearby_stops].
    rs = merged.get(COL_ROUTE_SURVEYED_CODE, pd.Series("", index=merged.index)).map(_norm_route_id_token)
    first_route = merged.get(COL_TRIP_FIRST_ROUTE, pd.Series("", index=merged.index))
    first_route = first_route.where(first_route.notna() & (first_route.astype(str).str.strip() != ""), rs)
    first_route = first_route.astype(str).str.strip()

    last_route = merged.get(COL_TRIP_LAST4TH_RTE, pd.Series("", index=merged.index))
    for fallback_col in [COL_TRIP_3RD_ROUTE, COL_TRIP_AFTER_ROUTE, COL_TRIP_NEXT_ROUTE]:
        fb = merged.get(fallback_col, pd.Series("", index=merged.index))
        last_route = last_route.where(last_route.notna() & (last_route.astype(str).str.strip() != ""), fb)
    last_route = last_route.where(last_route.notna() & (last_route.astype(str).str.strip() != ""), rs)
    last_route = last_route.astype(str).str.strip()

    # Fast path: pre-normalize stops and cache per route
    if stops_df is None or stops_df.empty:
        return pd.DataFrame(
            {
                REQUIRED_ID: merged[REQUIRED_ID],
                "ORIGIN_fnCountStopsOnRouteWithinDistance": np.nan,
                "DESTIN_fnCountStopsOnRouteWithinDistance": np.nan,
            }
        ).drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)

    lat_col = "stop_lat6" if "stop_lat6" in stops_df.columns else ("stop_lat" if "stop_lat" in stops_df.columns else None)
    lon_col = "stop_lon6" if "stop_lon6" in stops_df.columns else ("stop_lon" if "stop_lon" in stops_df.columns else None)
    route_col = "ETC_ROUTE_ID" if "ETC_ROUTE_ID" in stops_df.columns else ("XFER_ROUTE_ID" if "XFER_ROUTE_ID" in stops_df.columns else ("route_id" if "route_id" in stops_df.columns else None))
    if lat_col is None or lon_col is None or route_col is None:
        return pd.DataFrame(
            {
                REQUIRED_ID: merged[REQUIRED_ID],
                "ORIGIN_fnCountStopsOnRouteWithinDistance": np.nan,
                "DESTIN_fnCountStopsOnRouteWithinDistance": np.nan,
            }
        ).drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)

    stops_work = stops_df[[lat_col, lon_col, route_col]].copy()
    stops_work[lat_col] = pd.to_numeric(stops_work[lat_col], errors="coerce")
    stops_work[lon_col] = pd.to_numeric(stops_work[lon_col], errors="coerce")
    # M-code / PQ parity: route match is exact equality on ETC_ROUTE_ID (no truncation).
    # RouteCode passed in may already have been truncated upstream (e.g. ROUTE_SURVEYED),
    # so tokenizing stops here would incorrectly create matches.
    stops_work["_route_match"] = stops_work[route_col].astype(str).str.strip()
    stops_work = stops_work.dropna(subset=[lat_col, lon_col])

    route_cache: Dict[str, pd.DataFrame] = {}

    def _count_for(route_norm: str, lat: float, lon: float, threshold: float) -> int:
        if not route_norm or threshold is None or not (threshold > 0):
            return 0
        half = threshold / 2.0
        if route_norm not in route_cache:
            route_cache[route_norm] = stops_work[stops_work["_route_match"] == route_norm]
        sdf = route_cache[route_norm]
        if sdf.empty:
            return 0
        # bounding box
        lat_min, lat_max = lat - half, lat + half
        lon_min, lon_max = lon - half, lon + half
        box = sdf[(sdf[lat_col] >= lat_min) & (sdf[lat_col] <= lat_max) & (sdf[lon_col] >= lon_min) & (sdf[lon_col] <= lon_max)]
        if box.empty:
            return 0
        # haversine distance
        d = box.apply(lambda rr: fn_haversine_miles(lat, lon, float(rr[lat_col]), float(rr[lon_col])) or 999999.0, axis=1)
        return int((d <= half).sum())

    origin_counts = []
    destin_counts = []
    for idx, r in merged.iterrows():
        olat, olon = _safe_float(r.get(COL_MERGED_ORIGIN_LAT)), _safe_float(r.get(COL_MERGED_ORIGIN_LONG))
        dlat, dlon = _safe_float(r.get(COL_MERGED_DESTIN_LAT)), _safe_float(r.get(COL_MERGED_DESTIN_LONG))
        o_thr = _safe_float(r.get("ORIGIN2Transfer1_Distance"), 0.0) or 0.0
        d_thr = _safe_float(r.get("DESTIN2Transfer1_Distance"), 0.0) or 0.0
        fr = first_route.loc[idx] if idx in first_route.index else ""
        lr = last_route.loc[idx] if idx in last_route.index else ""
        origin_counts.append(_count_for(fr, float(olat) if olat is not None else 0.0, float(olon) if olon is not None else 0.0, float(o_thr)))
        destin_counts.append(_count_for(lr, float(dlat) if dlat is not None else 0.0, float(dlon) if dlon is not None else 0.0, float(d_thr)))

    # Keep REQUIRED_ID dtype aligned with clean_df so merge(..., on=REQUIRED_ID) does not mix int64 vs str.
    out = pd.DataFrame({
        REQUIRED_ID: merged[REQUIRED_ID],
        "ORIGIN_fnCountStopsOnRouteWithinDistance": origin_counts,
        "DESTIN_fnCountStopsOnRouteWithinDistance": destin_counts,
    })
    return out.drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)

def _coord_valid(lat: Optional[float], lon: Optional[float]) -> bool:
    """Check coordinates are within valid ranges."""
    if lat is None or lon is None or (isinstance(lat, float) and pd.isna(lat)) or (isinstance(lon, float) and pd.isna(lon)):
        return False
    try:
        la, lo = float(lat), float(lon)
        return abs(la) <= 90 and abs(lo) <= 180
    except (TypeError, ValueError):
        return False


def _transport_mode_category(transport_str: Any) -> str:
    """
    Map ORIGIN_TRANSPORT/DESTIN_TRANSPORT to SHORT/MEDIUM/LONG.
    FROM M CODE: TRANSPORT-CODE (WALK/WHEELCHAIR=SHORT, BIKE/SCOOTER=MEDIUM, DRIVE=LONG).
    """
    if transport_str is None or (isinstance(transport_str, float) and pd.isna(transport_str)):
        return ""
    s = str(transport_str).strip().upper()
    if not s:
        return ""
    if "WALK" in s or "WHEELCHAIR" in s or "SKATEBOARD" in s or "SHORT" in s:
        return TRANSPORT_SHORT
    if "BIKE" in s or "SCOOTER" in s or "MEDIUM" in s:
        return TRANSPORT_MEDIUM
    if "DRIVE" in s or "CAR" in s or "LONG" in s:
        return TRANSPORT_LONG
    return s


def _prev_next_code_to_int(code_val: Any) -> int:
    """PREV_TRANSFERS[Code] / NEXT_TRANSFERS[Code]: "0","1", 1.0, etc. -> int (0, 1, or 2)."""
    if code_val is None or (isinstance(code_val, float) and pd.isna(code_val)):
        return 0
    s = str(code_val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return 0
    try:
        # Handle values like "(1) One", "(0) None", "1.0", etc.
        m = re.search(r"-?\d+(\.\d+)?", s)
        if m is None:
            return 0
        n = int(float(m.group(0)))
        return max(0, min(2, n))  # clamp to 0..2 for transfer count
    except (ValueError, TypeError):
        return 0


def _get_transfer_code_series(df: pd.DataFrame, which: str) -> pd.Series:
    """
    Get PREV or NEXT transfer code as a Series of strings "0", "1", "2".
    Resolves column name (PBIX, ls2, ls6) and normalizes values so phase logic matches.
    which: "prev" or "next"
    """
    if which.lower() == "prev":
        candidates = [
            "PREV_TRANSFERS",
            COL_PREV_TRANSFERS_CODE,
            "VAL_COUNT_PREVTRANS",
            "PREV_TRANSFERS_Code_",
            "PrevTransfersCode",
            "PREV_TRANSFER_COUNT",
        ]
        needle = "prev"
    else:
        candidates = [
            "NEXT_TRANSFERS",
            COL_NEXT_TRANSFERS_CODE,
            "VAL_COUNT_NEXTTRANS",
            "NEXT_TRANSFERS_Code_",
            "NextTransfersCode",
            "NEXT_TRANSFER_COUNT",
        ]
        needle = "next"
    col = None
    for c in candidates:
        if c in df.columns:
            col = c
            break
    if col is None:
        # Fallback: any column whose name contains prev/next and (transfer or code)
        low = {str(c).lower(): c for c in df.columns}
        for k, c in low.items():
            if needle in k and ("transfer" in k or "code" in k or "count" in k):
                col = c
                break
    if col is None:
        return pd.Series("0", index=df.index, dtype=str)
    return df[col].apply(lambda x: str(_prev_next_code_to_int(x)))


def _get_transfer_code_series_and_column(df: pd.DataFrame, which: str) -> Tuple[pd.Series, str]:
    """Same as _get_transfer_code_series but also returns the column name used (for debugging)."""
    if which.lower() == "prev":
        candidates = [
            "PREV_TRANSFERS",
            COL_PREV_TRANSFERS_CODE,
            "VAL_COUNT_PREVTRANS",
            "PREV_TRANSFERS_Code_",
            "PrevTransfersCode",
            "PREV_TRANSFER_COUNT",
        ]
        needle = "prev"
    else:
        candidates = [
            "NEXT_TRANSFERS",
            COL_NEXT_TRANSFERS_CODE,
            "VAL_COUNT_NEXTTRANS",
            "NEXT_TRANSFERS_Code_",
            "NextTransfersCode",
            "NEXT_TRANSFER_COUNT",
        ]
        needle = "next"
    for c in candidates:
        if c in df.columns:
            return df[c].apply(lambda x: str(_prev_next_code_to_int(x))), c
    low = {str(c).lower(): c for c in df.columns}
    for k, c in low.items():
        if needle in k and ("transfer" in k or "code" in k or "count" in k):
            return df[c].apply(lambda x: str(_prev_next_code_to_int(x))), c
    return pd.Series("0", index=df.index, dtype=str), ""


def _debug_transfer_approval(
    clean_df: pd.DataFrame,
    phase1_df: pd.DataFrame,
    phase2_df: pd.DataFrame,
    phase_all_df: pd.DataFrame,
    step01_df: pd.DataFrame,
    step02_df: pd.DataFrame,
    step03_df: pd.DataFrame,
) -> None:
    """Log why only 0-transfer records might be approved: transfer code distribution and pipeline funnel."""
    prev_ser, prev_col = _get_transfer_code_series_and_column(clean_df, "prev")
    nxt_ser, nxt_col = _get_transfer_code_series_and_column(clean_df, "next")
    logger.info("[DEBUG transfers] PREV column used: %r | value_counts: %s", prev_col or "(none)", prev_ser.value_counts().to_dict())
    logger.info("[DEBUG transfers] NEXT column used: %r | value_counts: %s", nxt_col or "(none)", nxt_ser.value_counts().to_dict())
    # Scan for other potential transfer-count/code columns that may contain non-zero values
    candidates = []
    for c in clean_df.columns:
        k = str(c).lower()
        if ("prev" in k or "next" in k) and ("transfer" in k or "transfers" in k or "code" in k or "count" in k):
            candidates.append(c)
    # Keep stable ordering for logs
    candidates = sorted(set(candidates), key=lambda x: str(x))
    if candidates:
        nonzero = []
        for c in candidates:
            try:
                s_norm = clean_df[c].apply(_prev_next_code_to_int).astype(int)
                if (s_norm != 0).any():
                    vc = s_norm.value_counts().to_dict()
                    nonzero.append((str(c), vc))
            except Exception:
                continue
        if nonzero:
            logger.info("[DEBUG transfers] Non-zero transfer-like columns found (normalized int value_counts): %s", nonzero)
        else:
            logger.info("[DEBUG transfers] No non-zero values found in any transfer-like column names (%d columns scanned)", len(candidates))
    else:
        logger.info("[DEBUG transfers] No transfer-like columns found by name scan")
    # (prev, next) combo counts in full clean_df
    combo = prev_ser.astype(str) + "_" + nxt_ser.astype(str)
    logger.info("[DEBUG transfers] clean_df (prev, next) combo counts: %s", combo.value_counts().to_dict())
    # How many have at least one transfer
    with_transfer = (prev_ser != "0") | (nxt_ser != "0")
    logger.info("[DEBUG transfers] clean_df rows with prev!=0 or next!=0: %d of %d", with_transfer.sum(), len(clean_df))
    if with_transfer.any() and REQUIRED_ID in clean_df.columns:
        sample_ids = clean_df.loc[with_transfer, REQUIRED_ID].head(5).tolist()
        logger.info("[DEBUG transfers] sample ids with 1+ transfer: %s", sample_ids)
    # Phase-All: which (prev, next) made it in
    if not phase_all_df.empty and REQUIRED_ID in phase_all_df.columns and REQUIRED_ID in clean_df.columns:
        pa_ids = set(phase_all_df[REQUIRED_ID].astype(str))
        in_pa = clean_df[REQUIRED_ID].astype(str).isin(pa_ids)
        prev_in_pa = prev_ser[in_pa]
        nxt_in_pa = nxt_ser[in_pa]
        combo_pa = prev_in_pa.astype(str) + "_" + nxt_in_pa.astype(str)
        logger.info("[DEBUG transfers] Phase-All (prev, next) combo counts: %s", combo_pa.value_counts().to_dict())
    # Step03 approved: which (prev, next)
    if not step03_df.empty and REQUIRED_ID in step03_df.columns:
        app_ids = set(step03_df[REQUIRED_ID].astype(str))
        in_app = clean_df[REQUIRED_ID].astype(str).isin(app_ids)
        prev_app = prev_ser[in_app]
        nxt_app = nxt_ser[in_app]
        combo_app = prev_app.astype(str) + "_" + nxt_app.astype(str)
        logger.info("[DEBUG transfers] Approved/step03 (prev, next) combo counts: %s", combo_app.value_counts().to_dict())
    # Funnel by step: Phase-All -> (01) -> (02) -> (03)
    try:
        if REQUIRED_ID in clean_df.columns:
            def _combo_for_ids(ids_set: set) -> Dict[str, int]:
                in_set = clean_df[REQUIRED_ID].astype(str).isin(ids_set)
                return (prev_ser[in_set].astype(str) + "_" + nxt_ser[in_set].astype(str)).value_counts().to_dict()
            pa_ids = set(phase_all_df[REQUIRED_ID].astype(str)) if (not phase_all_df.empty and REQUIRED_ID in phase_all_df.columns) else set()
            s01_ids = set(step01_df[REQUIRED_ID].astype(str)) if (step01_df is not None and not step01_df.empty and REQUIRED_ID in step01_df.columns) else set()
            s02_ids = set(step02_df[REQUIRED_ID].astype(str)) if (step02_df is not None and not step02_df.empty and REQUIRED_ID in step02_df.columns) else set()
            s03_ids = set(step03_df[REQUIRED_ID].astype(str)) if (step03_df is not None and not step03_df.empty and REQUIRED_ID in step03_df.columns) else set()
            logger.info("[DEBUG transfers] Funnel combos Phase-All: %s", _combo_for_ids(pa_ids))
            logger.info("[DEBUG transfers] Funnel combos after (01): %s", _combo_for_ids(s01_ids))
            logger.info("[DEBUG transfers] Funnel combos after (02): %s", _combo_for_ids(s02_ids))
            logger.info("[DEBUG transfers] Funnel combos after (03): %s", _combo_for_ids(s03_ids))
    except Exception as e:
        logger.warning("[DEBUG transfers] Step funnel combos failed: %s", e)
    # Phase1/Phase2 group counts
    if COL_GROUP in phase1_df.columns:
        logger.info("[DEBUG transfers] Phase1 group counts: %s", phase1_df[COL_GROUP].value_counts().to_dict())
    if COL_GROUP in phase2_df.columns:
        logger.info("[DEBUG transfers] Phase2 group counts: %s", phase2_df[COL_GROUP].value_counts().to_dict())
    # Nearby stops: if missing (NaN), phase conditions (origin_near <= 5) fail
    on_col = "ORIGIN_fnCountStopsOnRouteWithinDistance"
    dn_col = "DESTIN_fnCountStopsOnRouteWithinDistance"
    if on_col in clean_df.columns and dn_col in clean_df.columns:
        on_nan = clean_df[on_col].isna()
        dn_nan = clean_df[dn_col].isna()
        with_transfer_near_missing = with_transfer & (on_nan | dn_nan)
        logger.info(
            "[DEBUG transfers] Rows with 1+ transfer that have missing nearby stops (NaN): %d (these fail origin_near/destin_near <= 5)",
            with_transfer_near_missing.sum(),
        )

    # Deep-dive: why transfer rows are not entering Phase1/Phase2
    try:
        df = clean_df  # already has prereq columns + nearby stops merged (when available)
        origin_short = _text_contains_short(df.get("ORIGIN_Transport_Mode", pd.Series("", index=df.index)))
        destin_short = _text_contains_short(df.get("DESTIN_Transport_Mode", pd.Series("", index=df.index)))
        o2d_first = df.get("O2DtoFIRSTBLASTA", pd.Series(999, index=df.index))
        coords_ok = (
            df.get(COL_MERGED_ORIGIN_LAT, pd.Series(np.nan, index=df.index)).notna()
            & df.get(COL_MERGED_ORIGIN_LONG, pd.Series(np.nan, index=df.index)).notna()
            & df.get(COL_MERGED_DESTIN_LAT, pd.Series(np.nan, index=df.index)).notna()
            & df.get(COL_MERGED_DESTIN_LONG, pd.Series(np.nan, index=df.index)).notna()
        )
        base_ww = origin_short & destin_short & (o2d_first == 0) & coords_ok
        base_nw = (o2d_first == 0) & coords_ok
        origin_near = pd.to_numeric(df.get(on_col, pd.Series(np.inf, index=df.index)), errors="coerce")
        destin_near = pd.to_numeric(df.get(dn_col, pd.Series(np.inf, index=df.index)), errors="coerce")

        # For transfer rows only, show which base conditions fail
        t = with_transfer
        if t.any():
            logger.info(
                "[DEBUG transfers] Transfer rows base failures: ww_base_ok=%d/%d, nw_base_ok=%d/%d, origin_short_true=%d/%d, destin_short_true=%d/%d, O2DtoFIRSTBLASTA==0=%d/%d, coords_ok=%d/%d",
                (base_ww & t).sum(), t.sum(),
                (base_nw & t).sum(), t.sum(),
                (origin_short & t).sum(), t.sum(),
                (destin_short & t).sum(), t.sum(),
                ((o2d_first == 0) & t).sum(), t.sum(),
                (coords_ok & t).sum(), t.sum(),
            )
            logger.info(
                "[DEBUG transfers] Transfer rows nearby stops pass rates: origin_near<=5=%d/%d, destin_near<=5=%d/%d, origin_near<=10=%d/%d, destin_near<=10=%d/%d",
                ((origin_near <= 5) & t).sum(), t.sum(),
                ((destin_near <= 5) & t).sum(), t.sum(),
                ((origin_near <= 10) & t).sum(), t.sum(),
                ((destin_near <= 10) & t).sum(), t.sum(),
            )

        # Phase1 (Walk-Walk) transfer subgroup counts (should be >0 if any 1/2-transfer rows qualify)
        def _safe_num(col: str) -> pd.Series:
            return pd.to_numeric(df.get(col, pd.Series(np.nan, index=df.index)), errors="coerce")
        o2t1 = _safe_num("ORIGIN2Transfer1_Distance")
        d2t1 = _safe_num("DESTIN2Transfer1_Distance")
        o2d = _safe_num("ORIGIN_TO_DESTIN")
        tr1 = _safe_num("Transfer1_Distance")
        tr2 = _safe_num("Transfer2_Distance")
        tr5 = _safe_num("Transfer5_Distance")
        tr6 = _safe_num("Transfer6_Distance")

        ww_10 = base_ww & (prev_ser == "1") & (nxt_ser == "0") & (o2t1 <= 1) & (d2t1 <= 1) & (o2d >= 0.50) & (tr1 <= 0.10) & (origin_near <= 5) & (destin_near <= 5)
        ww_20 = base_ww & (prev_ser == "2") & (nxt_ser == "0") & (o2t1 <= 1) & (d2t1 <= 1) & (o2d >= 1) & (tr1 <= 0.10) & (tr2 <= 0.10) & (origin_near <= 5) & (destin_near <= 5)
        ww_01 = base_ww & (prev_ser == "0") & (nxt_ser == "1") & (o2t1 <= 1) & (d2t1 <= 1) & (o2d >= 0.50) & (tr5 <= 0.10) & (origin_near <= 5) & (destin_near <= 5)
        ww_02 = base_ww & (prev_ser == "0") & (nxt_ser == "2") & (o2t1 <= 1) & (d2t1 <= 1) & (o2d >= 1) & (tr5 <= 0.10) & (tr6 <= 0.10) & (origin_near <= 5) & (destin_near <= 5)
        ww_11 = base_ww & (prev_ser == "1") & (nxt_ser == "1") & (o2t1 <= 1) & (d2t1 <= 1) & (o2d >= 1) & (tr1 <= 0.10) & (tr5 <= 0.10) & (origin_near <= 5) & (destin_near <= 5)
        logger.info(
            "[DEBUG transfers] Phase1 transfer subgroup hits: ww_1prev=%d, ww_2prev=%d, ww_1next=%d, ww_2next=%d, ww_1prev1next=%d",
            int(ww_10.sum()), int(ww_20.sum()), int(ww_01.sum()), int(ww_02.sum()), int(ww_11.sum()),
        )
        # Funnel breakdown for the most common cases
        def _funnel(label: str, masks: List[Tuple[str, pd.Series]]) -> None:
            cur = pd.Series(True, index=df.index)
            parts = []
            for name, m in masks:
                cur = cur & m
                parts.append(f"{name}={int(cur.sum())}")
            logger.info("[DEBUG transfers] Funnel %s: %s", label, ", ".join(parts))

        _funnel(
            "Walk-Walk 1 PREV (prev=1,next=0)",
            [
                ("base_ww", base_ww),
                ("prev==1", (prev_ser == "1")),
                ("next==0", (nxt_ser == "0")),
                ("O2T1<=1", (o2t1 <= 1)),
                ("D2T1<=1", (d2t1 <= 1)),
                ("O2D>=0.50", (o2d >= 0.50)),
                ("T1<=0.10", (tr1 <= 0.10)),
                ("origin_near<=5", (origin_near <= 5)),
                ("destin_near<=5", (destin_near <= 5)),
            ],
        )
        _funnel(
            "Walk-Walk 1 NEXT (prev=0,next=1)",
            [
                ("base_ww", base_ww),
                ("prev==0", (prev_ser == "0")),
                ("next==1", (nxt_ser == "1")),
                ("O2T1<=1", (o2t1 <= 1)),
                ("D2T1<=1", (d2t1 <= 1)),
                ("O2D>=0.50", (o2d >= 0.50)),
                ("T5<=0.10", (tr5 <= 0.10)),
                ("origin_near<=5", (origin_near <= 5)),
                ("destin_near<=5", (destin_near <= 5)),
            ],
        )

        # Phase2 (NOTwalk-Walk / Walk-NOTwalk) transfer subgroup hits summary
        def _origin_access_ok() -> pd.Series:
            return (((df.get("ORIGIN_Transport_Mode", "") == "LONG") & (o2t1 <= 10)) | ((df.get("ORIGIN_Transport_Mode", "") == "MEDIUM") & (o2t1 <= 5)))
        def _destin_access_ok() -> pd.Series:
            return (((df.get("DESTIN_Transport_Mode", "") == "LONG") & (d2t1 <= 10)) | ((df.get("DESTIN_Transport_Mode", "") == "MEDIUM") & (d2t1 <= 5)))

        nw_10 = base_nw & (~origin_short) & destin_short & (prev_ser == "1") & (nxt_ser == "0") & _origin_access_ok() & (d2t1 <= 1) & (o2d >= 0.50) & (tr1 <= 0.10) & (origin_near <= 10) & (destin_near <= 5)
        wn_10 = base_nw & origin_short & (~destin_short) & (prev_ser == "1") & (nxt_ser == "0") & _destin_access_ok() & (o2t1 <= 1) & (o2d >= 0.50) & (tr1 <= 0.10) & (origin_near <= 5) & (destin_near <= 10)
        logger.info(
            "[DEBUG transfers] Phase2 transfer subgroup hits (sample): NOTWalk-Walk[1PREV]=%d, Walk-NOTWalk[1PREV]=%d",
            int(nw_10.sum()), int(wn_10.sum()),
        )
    except Exception as e:
        logger.warning("[DEBUG transfers] Deep-dive failed: %s", e)


def _resolve_id_column(df: pd.DataFrame) -> Optional[str]:
    """
    Return the actual column name to use as record ID.
    Prefer REQUIRED_ID ('id'), then case-insensitive / common aliases, then first column.
    """
    cols = list(df.columns)
    if REQUIRED_ID in cols:
        return REQUIRED_ID
    lower_to_col = {str(c).lower(): c for c in cols}
    for candidate in ("id", "id_", "elvis_id", "record_id", "recordid"):
        if candidate in lower_to_col:
            return lower_to_col[candidate]
    if cols:
        return cols[0]
    return None


def _ensure_clean_dataset_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure dataframe has columns needed for OD [CLEAN DATASET] style names.
    Maps common alternatives (e.g. ORIGIN_ADDRESS_LAT_) to MERGED_ORIGIN_ADDRESS [LAT] etc.
    Ensures an 'id' column exists (copied from resolved ID column if missing).
    """
    out = df.copy()
    # Ensure id column exists for pipeline (phase1, phase2, etc.)
    id_col = _resolve_id_column(out)
    if id_col is not None and REQUIRED_ID not in out.columns:
        out[REQUIRED_ID] = out[id_col].astype(str).values
    # Map merged coords if present under other names
    for orig, alt in [
        (COL_MERGED_ORIGIN_LAT, "ORIGIN_ADDRESS_LAT_"),
        (COL_MERGED_ORIGIN_LONG, "ORIGIN_ADDRESS_LONG_"),
        (COL_MERGED_DESTIN_LAT, "DESTIN_ADDRESS_LAT_"),
        (COL_MERGED_DESTIN_LONG, "DESTIN_ADDRESS_LONG_"),
    ]:
        if orig not in out.columns and alt in out.columns:
            out[orig] = out[alt]
    # Ensure PREV/NEXT transfer codes under standard names (string "0","1","2") for phase logic
    for std_col, alts in [
        (COL_PREV_TRANSFERS_CODE, ["PREV_TRANSFERS", "VAL_COUNT_PREVTRANS", "PREV_TRANSFERS_Code_", "PrevTransfersCode", "PREV_TRANSFER_COUNT"]),
        (COL_NEXT_TRANSFERS_CODE, ["NEXT_TRANSFERS", "VAL_COUNT_NEXTTRANS", "NEXT_TRANSFERS_Code_", "NextTransfersCode", "NEXT_TRANSFER_COUNT"]),
    ]:
        if std_col not in out.columns:
            for a in alts:
                if a in out.columns:
                    out[std_col] = out[a].apply(lambda x: str(_prev_next_code_to_int(x)))
                    break
            else:
                out[std_col] = "0"
        else:
            # If std_col is present but all zeros, and a better alt has non-zero values, prefer the alt (your exports do this).
            std_norm = out[std_col].apply(_prev_next_code_to_int)
            if (std_norm != 0).any():
                out[std_col] = std_norm.astype(str)
            else:
                chosen = None
                for a in alts:
                    if a in out.columns:
                        alt_norm = out[a].apply(_prev_next_code_to_int)
                        if (alt_norm != 0).any():
                            chosen = alt_norm
                            break
                out[std_col] = (chosen if chosen is not None else std_norm).astype(str)
    return out


# ---------------------------------------------------------------------------
# Section 5b: Data preparation (Elvis ls6 -> PBIX-style columns)
# Same inputs as auto_and_suggestions_more_transfers.py: CSV/DB export + mapping + details.
# ---------------------------------------------------------------------------

# ls2 (FormattedHeader-ls2) to PBIX/exported_table_structures column name mapping
# When mapping file is not applied (e.g. openpyxl missing), ls6 column names can be used as fallback.
# Minimal renames so pipeline finds place type and transport columns.
LS6_FALLBACK_RENAMES = {
    "OriginPlaceTypeCode": COL_ORIGIN_PLACE_TYPE,
    "DestinPlaceTypeCode": COL_DESTIN_PLACE_TYPE,
    "OriginTransportCode": COL_ORIGIN_TRANSPORT,
    "DestinTransportCode": COL_DESTIN_TRANSPORT,
    "PrevTransfersCode": COL_PREV_TRANSFERS_CODE,
    "NextTransfersCode": COL_NEXT_TRANSFERS_CODE,
    "TripFirstRouteCode": COL_TRIP_FIRST_ROUTE,
    "TripSecondRouteCode": COL_TRIP_SECOND_ROUTE,
    "TripThirdRouteCode": COL_TRIP_THIRD_ROUTE,
    "TripFourthRouteCode": COL_TRIP_FOURTH_ROUTE,
    "TripAfterRouteCode": COL_TRIP_AFTER_ROUTE,
    "Trip3RdRouteCode": COL_TRIP_3RD_ROUTE,
    "TripLast4ThRteCode": COL_TRIP_LAST4TH_RTE,
    "TripNextRouteCode": COL_TRIP_NEXT_ROUTE,
    "RouteSurveyedCode": COL_ROUTE_SURVEYED_CODE,
    # Elvis field-approved signal (your exports typically have `ELVIS_STATUS`)
    "ELVIS_STATUS": COL_ELVIS_STATUS,
    # No-5-min signal (your exports often use `HAVE_5_MIN_FOR_SURVE_Code_`)
    "HAVE_5_MIN_FOR_SURVE_Code_": COL_HAVE_5_MIN,
    "HAVE_5_MIN_FOR_SURVECode_": COL_HAVE_5_MIN,
    "HAVE_5_MIN_FOR_SURVECode": COL_HAVE_5_MIN,
}
# Add STOP_ON/STOP_OFF and PREV/NEXT transfer coords (ls6 uses StopOn_LAT, PrevTran1OnBus_LAT, etc.)
for i in range(1, 5):
    LS6_FALLBACK_RENAMES[f"PrevTran{i}OnBus_LAT"] = f"PREV_TRAN_{i}_ON_BUS [LAT]"
    LS6_FALLBACK_RENAMES[f"PrevTran{i}OnBus_LONG"] = f"PREV_TRAN_{i}_ON_BUS [LONG]"
    LS6_FALLBACK_RENAMES[f"PrevTran{i}OffBus_LAT"] = f"PREV_TRAN_{i}_OFF_BUS [LAT]"
    LS6_FALLBACK_RENAMES[f"PrevTran{i}OffBus_LONG"] = f"PREV_TRAN_{i}_OFF_BUS [LONG]"
    LS6_FALLBACK_RENAMES[f"NextTran{i}OnBus_LAT"] = f"NEXT_TRAN_{i}_ON_BUS [LAT]"
    LS6_FALLBACK_RENAMES[f"NextTran{i}OnBus_LONG"] = f"NEXT_TRAN_{i}_ON_BUS [LONG]"
    LS6_FALLBACK_RENAMES[f"NextTran{i}OffBus_LAT"] = f"NEXT_TRAN_{i}_OFF_BUS [LAT]"
    LS6_FALLBACK_RENAMES[f"NextTran{i}OffBus_LONG"] = f"NEXT_TRAN_{i}_OFF_BUS [LONG]"
LS6_FALLBACK_RENAMES["StopOn_LAT"] = "STOP_ON [LAT]"
LS6_FALLBACK_RENAMES["StopOn_LONG"] = "STOP_ON [LONG]"
LS6_FALLBACK_RENAMES["StopOff_LAT"] = "STOP_OFF [LAT]"
LS6_FALLBACK_RENAMES["StopOff_LONG"] = "STOP_OFF [LONG]"

# MERGED_ORIGIN/DESTIN are built in _build_merged_origin_dest_address from ORIGIN_ADDRESS_* / HOME_ADDRESS_*
LS2_TO_PBIX_COLUMNS = {
    "STOP_ON_LAT": "STOP_ON [LAT]",
    "STOP_ON_LONG": "STOP_ON [LONG]",
    "STOP_OFF_LAT": "STOP_OFF [LAT]",
    "STOP_OFF_LONG": "STOP_OFF [LONG]",
    "ROUTE_SURVEYED_Code_": COL_ROUTE_SURVEYED_CODE,
    "ROUTE_SURVEYEDCode": COL_ROUTE_SURVEYED_CODE,
    "PREV_TRANSFERS_Code_": COL_PREV_TRANSFERS_CODE,
    "NEXT_TRANSFERS_Code_": COL_NEXT_TRANSFERS_CODE,
    "TRIP_FIRST_ROUTE_Code_": COL_TRIP_FIRST_ROUTE,
    "TRIP_SECOND_ROUTE_Code_": COL_TRIP_SECOND_ROUTE,
    "TRIP_THIRD_ROUTE_Code_": COL_TRIP_THIRD_ROUTE,
    "TRIP_FOURTH_ROUTE_Code_": COL_TRIP_FOURTH_ROUTE,
    "TRIP_NEXT_ROUTE_Code_": COL_TRIP_NEXT_ROUTE,
    "TRIP_AFTER_ROUTE_Code_": COL_TRIP_AFTER_ROUTE,
    "TRIP_3RD_ROUTE_Code_": COL_TRIP_3RD_ROUTE,
    "TRIP_LAST4TH_RTE_Code_": COL_TRIP_LAST4TH_RTE,
}
# PREV/NEXT transfer bus coords: ls2 (e.g. PREV_TRAN_1_ON_BUS_LAT_) -> PBIX (PREV_TRAN_1_ON_BUS [LAT])
for i in range(1, 5):
    LS2_TO_PBIX_COLUMNS[f"PREV_TRAN_{i}_ON_BUS_LAT_"] = f"PREV_TRAN_{i}_ON_BUS [LAT]"
    LS2_TO_PBIX_COLUMNS[f"PREV_TRAN_{i}_ON_BUS_LONG_"] = f"PREV_TRAN_{i}_ON_BUS [LONG]"
    LS2_TO_PBIX_COLUMNS[f"PREV_TRAN_{i}_OFF_BUS_LAT_"] = f"PREV_TRAN_{i}_OFF_BUS [LAT]"
    LS2_TO_PBIX_COLUMNS[f"PREV_TRAN_{i}_OFF_BUS_LONG_"] = f"PREV_TRAN_{i}_OFF_BUS [LONG]"
    LS2_TO_PBIX_COLUMNS[f"NEXT_TRAN_{i}_ON_BUS_LAT_"] = f"NEXT_TRAN_{i}_ON_BUS [LAT]"
    LS2_TO_PBIX_COLUMNS[f"NEXT_TRAN_{i}_ON_BUS_LONG_"] = f"NEXT_TRAN_{i}_ON_BUS [LONG]"
    LS2_TO_PBIX_COLUMNS[f"NEXT_TRAN_{i}_OFF_BUS_LAT_"] = f"NEXT_TRAN_{i}_OFF_BUS [LAT]"
    LS2_TO_PBIX_COLUMNS[f"NEXT_TRAN_{i}_OFF_BUS_LONG_"] = f"NEXT_TRAN_{i}_OFF_BUS [LONG]"


def _first_column(df: pd.DataFrame, *candidates: str, default_index: Optional[pd.Index] = None) -> pd.Series:
    """Return the first column that exists; otherwise a series of NaN with df.index (or default_index)."""
    for c in candidates:
        if c in df.columns:
            return df[c]
    idx = default_index if default_index is not None else df.index
    return pd.Series(np.nan, index=idx, dtype=float)


def _build_merged_origin_dest_address(df: pd.DataFrame) -> pd.DataFrame:
    """
    FROM M CODE: OD [HHOD] - MERGED_ORIGIN_ADDRESS / MERGED_DESTIN_ADDRESS.
    If ORIGIN_PLACE_TYPE contains HOME or -Hotel- use HOME_ADDRESS_LAT/LONG; else ORIGIN_ADDRESS_LAT/LONG.
    Same for DESTIN. Supports both PBIX/ls2 and ls6 column names when mapping was not applied.
    """
    out = df.copy()
    idx = out.index
    # Place type: PBIX/ls2 or ls6 (OriginPlaceTypeCode, DestinPlaceTypeCode)
    orig_pt = _first_column(out, COL_ORIGIN_PLACE_TYPE, "OriginPlaceTypeCode", "OriginPlaceType", default_index=idx)
    orig_pt = orig_pt.fillna("").astype(str).str.upper().replace("NAN", "")
    dest_pt = _first_column(out, COL_DESTIN_PLACE_TYPE, "DestinPlaceTypeCode", "DestinPlaceType", default_index=idx)
    dest_pt = dest_pt.fillna("").astype(str).str.upper().replace("NAN", "")
    home_lat = _first_column(out, "HOME_ADDRESS_LAT", "HOME_ADDRESS_LAT_", "HomeAddress_LAT", default_index=idx)
    home_lon = _first_column(out, "HOME_ADDRESS_LONG", "HOME_ADDRESS_LONG_", "HomeAddress_LONG", default_index=idx)
    o_lat = _first_column(out, "ORIGIN_ADDRESS_LAT", "ORIGIN_ADDRESS_LAT_", "OriginAddress_LAT", "ORIGIN_ADDRESS [LAT]", default_index=idx)
    o_lon = _first_column(out, "ORIGIN_ADDRESS_LONG", "ORIGIN_ADDRESS_LONG_", "OriginAddress_LONG", "ORIGIN_ADDRESS [LONG]", default_index=idx)
    d_lat = _first_column(out, "DESTIN_ADDRESS_LAT", "DESTIN_ADDRESS_LAT_", "DestinAddress_LAT", "DESTIN_ADDRESS [LAT]", default_index=idx)
    d_lon = _first_column(out, "DESTIN_ADDRESS_LONG", "DESTIN_ADDRESS_LONG_", "DestinAddress_LONG", "DESTIN_ADDRESS [LONG]", default_index=idx)
    use_home_origin = orig_pt.str.contains("HOME", na=False) | orig_pt.str.contains("HOTEL", na=False)
    use_home_destin = dest_pt.str.contains("HOME", na=False) | dest_pt.str.contains("HOTEL", na=False)
    out[COL_MERGED_ORIGIN_LAT] = np.where(use_home_origin, home_lat, o_lat)
    out[COL_MERGED_ORIGIN_LONG] = np.where(use_home_origin, home_lon, o_lon)
    out[COL_MERGED_DESTIN_LAT] = np.where(use_home_destin, home_lat, d_lat)
    out[COL_MERGED_DESTIN_LONG] = np.where(use_home_destin, home_lon, d_lon)
    return out


def _add_transport_mode_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add ORIGIN_Transport_Mode and DESTIN_Transport_Mode (SHORT/MEDIUM/LONG) from ORIGIN_TRANSPORT, DESTIN_TRANSPORT."""
    out = df.copy()
    for src_candidates, dest in [
        ([COL_ORIGIN_TRANSPORT, "OriginTransportCode", "OriginTransport"], COL_ORIGIN_TRANSPORT_MODE),
        (["DESTIN_TRANSPORT", "DestinTransportCode", "DestinTransport"], COL_DESTIN_TRANSPORT_MODE),
    ]:
        col = _first_column(out, *src_candidates, default_index=out.index)
        if col.isna().all() or (col.astype(str).str.strip() == "").all():
            out[dest] = ""
        else:
            out[dest] = col.fillna("").astype(str).apply(_transport_mode_category)
    return out


def _apply_ls2_to_pbix_rename(df: pd.DataFrame) -> pd.DataFrame:
    """Rename ls2-style columns to PBIX/exported_table_structures names where they exist."""
    def _norm_col(x: Any) -> str:
        # Normalize unusual underscore patterns + casing for robust header matching.
        # Examples:
        # - "ROUTE_SURVEYED_Code_" -> "routesurveyedcode"
        # - "TRIP_FIRST_ROUTECode" -> "trip_first_routecode"
        s = "" if x is None else str(x).strip()
        if not s:
            return ""
        s = s.replace("\u00a0", " ")  # nbsp -> space
        s = re.sub(r"\s+", " ", s)
        s = s.strip()
        s = re.sub(r"_+Code_?", "Code", s, flags=re.IGNORECASE)
        s = s.strip("_")
        s = re.sub(r"_+", "_", s)
        return s.lower()

    out = df.copy()
    rename: Dict[str, str] = {}

    # Build normalized lookup for ls2_name -> pbix_name
    norm_lookup: Dict[str, str] = {}
    for ls2_name, pbix_name in LS2_TO_PBIX_COLUMNS.items():
        norm_lookup[_norm_col(ls2_name)] = pbix_name

    # Rename any df columns whose normalized name matches a key in LS2_TO_PBIX_COLUMNS
    for col in list(out.columns):
        norm = _norm_col(col)
        pbix_name = norm_lookup.get(norm)
        if pbix_name and pbix_name not in out.columns and col in out.columns:
            rename[col] = pbix_name

    if rename:
        out = out.rename(columns=rename)
    return out


def prepare_elvis_data_for_pipeline(
    input_data: Union[str, pd.DataFrame],
    mapping_file: Optional[str] = None,
    mapping_sheet: str = "Example",
    details_file: Optional[str] = None,
    details_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Prepare Elvis export data (ls6 or ls2 columns) for the improved auto-approval pipeline.
    Mirrors auto_and_suggestions_more_transfers.py: load data -> header mapping -> home/airport/hotel -> MERGED coords -> PBIX column names.

    Parameters
    ----------
    input_data : str or DataFrame
        Path to CSV (e.g. elvis_transit_ls6_733524_export_odbc.csv) or DataFrame from SELECT * FROM elvis_transit_ls6_733524_export_odbc.
    mapping_file : str, optional
        Excel with Headers-ls6 and FormattedHeader-ls2 (e.g. request_20250708_ls6tols2-headers.xlsx).
    mapping_sheet : str
        Sheet name in mapping_file (default "Example").
    details_file : str, optional
        Details Excel (e.g. details_lacmta-feeder_733524_od_excel.xlsx) for check_home_airport_hotel.
        May include ``STOPS`` / ``XFER_STOPS`` sheets; ``run_improved_auto_approval`` uses them for __(01) xfer_list.
    details_df : DataFrame, optional
        Pre-built details (e.g. from load_details_stops_xfer). When provided, used for check_home_airport_hotel instead of loading from details_file.

    Returns
    -------
    DataFrame with columns matching exported_table_structures / PBIX: id, ORIGIN_PLACE_TYPE, DESTIN_PLACE_TYPE,
    MERGED_ORIGIN_ADDRESS [LAT/LONG], MERGED_DESTIN_ADDRESS [LAT/LONG], ORIGIN_Transport_Mode, DESTIN_Transport_Mode,
    STOP_ON [LAT/LONG], STOP_OFF [LAT/LONG], PREV_TRANSFERS[Code], NEXT_TRANSFERS[Code], ROUTE_SURVEYED[Code], TRIP_*_ROUTE[Code], etc.
    """
    if isinstance(input_data, str):
        lower = input_data.lower()
        if lower.endswith((".xlsx", ".xls")):
            df = pd.read_excel(input_data)
        elif lower.endswith((".csv", ".tsv", ".txt")):
            df = _read_csv_auto_delimiter(input_data)
        else:
            # Fallback: attempt CSV-like parsing (some exports are .dat/.log but tab-delimited).
            df = _read_csv_auto_delimiter(input_data)
        logger.info("Loaded input from %s: %d rows", input_data, len(df))
    else:
        df = input_data.copy()

    if df.empty:
        return df

    # Normalize column names: remove BOM and trim edges.
    # This prevents mapping failures when exports include UTF-8 BOM before the first header.
    df.columns = df.columns.map(lambda c: str(c).replace("\ufeff", "").strip())

    # Additional normalization: apply pattern-based header renames to the entire export.
    # This makes your TSV-style headers consistent (`*_Code_*` / `*_Other_*`).
    df, changed_headers_count, changed_header_pairs = _normalize_input_headers(df)
    if changed_headers_count:
        # Log only first 12 pairs to keep logs readable.
        pairs_preview = ", ".join([f"{k}->{v}" for k, v in list(changed_header_pairs.items())[:12]])
        logger.info("Normalized input headers: %d columns renamed (%s%s)", changed_headers_count, pairs_preview, "" if changed_headers_count <= 12 else ", ...")

    # Step 1: Header mapping (ls6 -> ls2) - same as original script
    if mapping_file and os.path.isfile(mapping_file):
        try:
            header_df = pd.read_excel(mapping_file, sheet_name=mapping_sheet)
            if "Headers-ls6" in header_df.columns and "FormattedHeader-ls2" in header_df.columns:
                header_mapping = dict(zip(header_df["Headers-ls6"], header_df["FormattedHeader-ls2"]))

                def _norm_col(x: Any) -> str:
                    s = "" if x is None else str(x).strip()
                    if not s:
                        return ""
                    s = s.replace("\ufeff", "").replace("\u00a0", " ")
                    s = re.sub(r"\s+", " ", s)
                    s = s.strip()
                    s = re.sub(r"_+Code_?", "Code", s, flags=re.IGNORECASE)
                    s = s.strip("_")
                    s = re.sub(r"_+", "_", s)
                    return s.lower()

                # normalized input columns -> actual column name
                df_norm_to_col = { _norm_col(c): c for c in df.columns }

                rename = {}
                for ls6_col, ls2_col in header_mapping.items():
                    norm = _norm_col(ls6_col)
                    if norm in df_norm_to_col:
                        src = df_norm_to_col[norm]
                        rename[src] = ls2_col

                if rename:
                    df = df.rename(columns=rename)
                if rename:
                    pairs = ", ".join([f"{k}->{v}" for k, v in list(rename.items())])
                    logger.info(
                        "Applied header mapping from %s: %d columns renamed (%s)",
                        mapping_file,
                        len(rename),
                        pairs,
                    )
                else:
                    logger.info("Applied header mapping from %s: %d columns renamed", mapping_file, len(rename))
            else:
                logger.warning("Mapping file missing Headers-ls6 or FormattedHeader-ls2; skipping rename")
        except Exception as e:
            logger.warning("Could not apply mapping file %s: %s", mapping_file, e)
    else:
        if mapping_file:
            logger.warning("Mapping file not found: %s", mapping_file)

    # Step 1b: Apply ls6 fallback renames (normalized header matching).
    # This makes the pipeline resilient to export header variants (e.g., TSV vs CSV,
    # inconsistent underscore placement, casing, etc.).
    df, renamed_count, fallback_renamed_pairs = _apply_ls6_fallback_renames(df, LS6_FALLBACK_RENAMES)
    if renamed_count:
        pairs = ", ".join([f"{k}->{v}" for k, v in list(fallback_renamed_pairs.items())])
        logger.info("Applied ls6 fallback renames: %d columns (%s)", renamed_count, pairs)

    # Step 2: Home/airport/hotel resolution (same as original check_home_airport_hotel)
    if _PREP_DEPS_AVAILABLE:
        use_details = details_df
        if use_details is None and details_file and os.path.isfile(details_file):
            try:
                use_details = details_dataframe(details_file)
            except Exception as e:
                logger.warning("Could not load details from file %s: %s", details_file, e)
        if use_details is not None and not use_details.empty:
            try:
                use_details = use_details.copy()
                use_details.columns = use_details.columns.astype(str).str.strip()
                df = check_home_airport_hotel(df, use_details)
                logger.info("Applied check_home_airport_hotel using details")
            except Exception as e:
                logger.warning("check_home_airport_hotel failed: %s", e)

    # Step 3: Build MERGED_ORIGIN_ADDRESS / MERGED_DESTIN_ADDRESS (OD [HHOD] logic from M code)
    df = _build_merged_origin_dest_address(df)

    # Step 4: Add ORIGIN_Transport_Mode, DESTIN_Transport_Mode (TRANSPORT-CODE style)
    df = _add_transport_mode_columns(df)

    # Step 5: Rename ls2 columns to PBIX names (STOP_ON_LAT -> STOP_ON [LAT], PREV_TRANSFERS_Code_ -> PREV_TRANSFERS[Code], etc.)
    df = _apply_ls2_to_pbix_rename(df)

    # Ensure PREV_TRANSFERS[Code] / NEXT_TRANSFERS[Code] exist and are canonical "0","1","2" (M code).
    # Normalize so 1.0 / "1.0" -> "1" else Phase1/Phase2 (prev == "1") would never match.
    for code_col in [COL_PREV_TRANSFERS_CODE, COL_NEXT_TRANSFERS_CODE]:
        if code_col not in df.columns:
            df[code_col] = "0"
        else:
            df[code_col] = df[code_col].apply(lambda x: str(_prev_next_code_to_int(x)))
    # Prefer PREV_TRANSFERS / NEXT_TRANSFERS when [Code] columns are all-zero (common in your export)
    if COL_PREV_TRANSFERS_CODE in df.columns and "PREV_TRANSFERS" in df.columns:
        std = df[COL_PREV_TRANSFERS_CODE].apply(_prev_next_code_to_int)
        alt = df["PREV_TRANSFERS"].apply(_prev_next_code_to_int)
        if not (std != 0).any() and (alt != 0).any():
            df[COL_PREV_TRANSFERS_CODE] = alt.astype(str)
            logger.info("Using PREV_TRANSFERS as PREV_TRANSFERS[Code] (original [Code] was all zero)")
    if COL_NEXT_TRANSFERS_CODE in df.columns and "NEXT_TRANSFERS" in df.columns:
        std = df[COL_NEXT_TRANSFERS_CODE].apply(_prev_next_code_to_int)
        alt = df["NEXT_TRANSFERS"].apply(_prev_next_code_to_int)
        if not (std != 0).any() and (alt != 0).any():
            df[COL_NEXT_TRANSFERS_CODE] = alt.astype(str)
            logger.info("Using NEXT_TRANSFERS as NEXT_TRANSFERS[Code] (original [Code] was all zero)")

    if COL_GROUP not in df.columns:
        df[COL_GROUP] = ""

    logger.info("Data preparation complete: %d rows, key columns present", len(df))
    return df


# ---------------------------------------------------------------------------
# Section 6: Pipeline Functions (FROM M CODE)
# ---------------------------------------------------------------------------

def phase1_walk_walk_evaluation(clean_df: pd.DataFrame) -> pd.DataFrame:
    """Strict alignment to Walk-Walk Auto Approve Checks.txt (all 6 subqueries)."""
    if clean_df.empty:
        return pd.DataFrame(columns=[REQUIRED_ID, COL_GROUP])

    df = _compute_clean_prereq_columns(clean_df)

    # Nearby stops requires OG dataset + stops dataframe; build using clean+og merge in run step (cached in df when present)
    # If these columns are missing, treat as failing (strict).
    origin_near = pd.to_numeric(df.get("ORIGIN_fnCountStopsOnRouteWithinDistance", pd.Series(np.inf, index=df.index)), errors="coerce")
    destin_near = pd.to_numeric(df.get("DESTIN_fnCountStopsOnRouteWithinDistance", pd.Series(np.inf, index=df.index)), errors="coerce")

    origin_short = _text_contains_short(df["ORIGIN_Transport_Mode"])
    destin_short = _text_contains_short(df["DESTIN_Transport_Mode"])
    # Transfer codes as strings "0", "1", "2" (resolve column name and normalize 1.0 -> "1")
    prev = _get_transfer_code_series(df, "prev")
    nxt = _get_transfer_code_series(df, "next")

    base = origin_short & destin_short & (df["O2DtoFIRSTBLASTA"] == 0)
    base = base & df[COL_MERGED_ORIGIN_LAT].notna() & df[COL_MERGED_ORIGIN_LONG].notna() & df[COL_MERGED_DESTIN_LAT].notna() & df[COL_MERGED_DESTIN_LONG].notna()

    rows = []

    def _emit(mask: pd.Series, group_name: str) -> None:
        if not mask.any():
            return
        tmp = df.loc[mask, [REQUIRED_ID]].copy()
        tmp[COL_GROUP] = group_name
        rows.append(tmp)

    # Walk-Walk [No Transfers]
    m = base & (prev == "0") & (nxt == "0")
    m = m & (df["ORIGIN2Transfer1_Distance"] <= 1.5) & (df["DESTIN2Transfer1_Distance"] <= 1.5) & (df["ORIGIN_TO_DESTIN"] >= 0.25)
    m = m & (origin_near <= 5) & (destin_near <= 5)
    _emit(m, "Walk-Walk [No Transfers]")

    # Walk-Walk [1 PREV TRANSFER]
    m = base & (prev == "1") & (nxt == "0")
    m = m & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["DESTIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer1_Distance"] <= 0.10)
    m = m & (origin_near <= 5) & (destin_near <= 5)
    _emit(m, "Walk-Walk [1 PREV TRANSFER]")

    # Walk-Walk [2 PREV TRANSFER]
    m = base & (prev == "2") & (nxt == "0")
    m = m & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["DESTIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 1)
    m = m & (df["Transfer1_Distance"] <= 0.10) & (df["Transfer2_Distance"] <= 0.10)
    m = m & (origin_near <= 5) & (destin_near <= 5)
    _emit(m, "Walk-Walk [2 PREV TRANSFER]")

    # Walk-Walk [1 NEXT TRANSFER]
    m = base & (prev == "0") & (nxt == "1")
    m = m & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["DESTIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer5_Distance"] <= 0.10)
    m = m & (origin_near <= 5) & (destin_near <= 5)
    _emit(m, "Walk-Walk [1 NEXT TRANSFER]")

    # Walk-Walk [2 NEXT TRANSFER] — power_query_od_pipeline.phase1_walk_walk lines 865-877 (M: NEXT_TRANSFERS_Code_ == "1", not "2")
    m = base & (prev == "0") & (nxt == "1")
    m = m & (pd.to_numeric(df["ORIGIN2Transfer1_Distance"], errors="coerce") <= 1)
    m = m & (pd.to_numeric(df["DESTIN2Transfer1_Distance"], errors="coerce") <= 1)
    m = m & (pd.to_numeric(df["ORIGIN_TO_DESTIN"], errors="coerce") >= 1)
    m = m & (pd.to_numeric(df["Transfer5_Distance"], errors="coerce") <= 0.10)
    m = m & (pd.to_numeric(df["Transfer6_Distance"], errors="coerce") <= 0.10)
    m = m & (origin_near <= 5) & (destin_near <= 5)
    _emit(m, "Walk-Walk [2 NEXT TRANSFER]")

    # Walk-Walk [1 PREV - 1 NEXT TRANSFER]
    m = base & (prev == "1") & (nxt == "1")
    m = m & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["DESTIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 1)
    m = m & (df["Transfer1_Distance"] <= 0.10) & (df["Transfer5_Distance"] <= 0.10)
    m = m & (origin_near <= 5) & (destin_near <= 5)
    _emit(m, "Walk-Walk [1 PREV - 1 NEXT TRANSFER]")

    if not rows:
        return pd.DataFrame(columns=[REQUIRED_ID, COL_GROUP])
    out = pd.concat(rows, ignore_index=True).drop_duplicates(subset=[REQUIRED_ID]).sort_values(REQUIRED_ID).reset_index(drop=True)
    logger.info("Phase1_Walk-Walk: %d ids", len(out))
    return out


def phase2_not_walk_options(clean_df: pd.DataFrame) -> pd.DataFrame:
    """Strict alignment to NOTwalk-Walk Auto Approve Checks.txt (all 10 subqueries)."""
    if clean_df.empty:
        return pd.DataFrame(columns=[REQUIRED_ID, COL_GROUP])

    df = _compute_clean_prereq_columns(clean_df)
    origin_near = pd.to_numeric(df.get("ORIGIN_fnCountStopsOnRouteWithinDistance", pd.Series(np.inf, index=df.index)), errors="coerce")
    destin_near = pd.to_numeric(df.get("DESTIN_fnCountStopsOnRouteWithinDistance", pd.Series(np.inf, index=df.index)), errors="coerce")

    origin_short = _text_contains_short(df["ORIGIN_Transport_Mode"])
    destin_short = _text_contains_short(df["DESTIN_Transport_Mode"])
    # Transfer codes as strings "0", "1", "2" (resolve column name and normalize 1.0 -> "1")
    prev = _get_transfer_code_series(df, "prev")
    nxt = _get_transfer_code_series(df, "next")

    base = (df["O2DtoFIRSTBLASTA"] == 0)
    base = base & df[COL_MERGED_ORIGIN_LAT].notna() & df[COL_MERGED_ORIGIN_LONG].notna() & df[COL_MERGED_DESTIN_LAT].notna() & df[COL_MERGED_DESTIN_LONG].notna()

    rows = []
    def _emit(mask: pd.Series, group_name: str) -> None:
        if not mask.any():
            return
        tmp = df.loc[mask, [REQUIRED_ID]].copy()
        tmp[COL_GROUP] = group_name
        rows.append(tmp)

    def _origin_access_ok() -> pd.Series:
        return (
            ((df["ORIGIN_Transport_Mode"] == "LONG") & (df["ORIGIN2Transfer1_Distance"] <= 10))
            | ((df["ORIGIN_Transport_Mode"] == "MEDIUM") & (df["ORIGIN2Transfer1_Distance"] <= 5))
        )

    def _destin_access_ok() -> pd.Series:
        return (
            ((df["DESTIN_Transport_Mode"] == "LONG") & (df["DESTIN2Transfer1_Distance"] <= 10))
            | ((df["DESTIN_Transport_Mode"] == "MEDIUM") & (df["DESTIN2Transfer1_Distance"] <= 5))
        )

    # NOTwalk-Walk [No Transfers]
    m = base & (~origin_short) & destin_short & (prev == "0") & (nxt == "0")
    m = m & _origin_access_ok() & (df["DESTIN2Transfer1_Distance"] <= 1.5) & (df["ORIGIN_TO_DESTIN"] >= 0.25)
    m = m & (origin_near <= 10) & (destin_near <= 5)
    _emit(m, "NOTWalk-Walk [No Transfers]")

    # Walk-NOTWalk [No Transfers]
    m = base & origin_short & (~destin_short) & (prev == "0") & (nxt == "0")
    m = m & (df["ORIGIN2Transfer1_Distance"] <= 1.5) & _destin_access_ok() & (df["ORIGIN_TO_DESTIN"] >= 0.25)
    m = m & (origin_near <= 5) & (destin_near <= 10)
    _emit(m, "Walk-NOTWalk [No Transfers]")

    # NOTWalk-Walk [1 PREV TRANSFER]
    m = base & (~origin_short) & destin_short & (prev == "1") & (nxt == "0")
    m = m & _origin_access_ok() & (df["DESTIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer1_Distance"] <= 0.10) & (origin_near <= 10) & (destin_near <= 5)
    _emit(m, "NOTWalk-Walk [1 PREV TRANSFER]")

    # NOTWalk-Walk [2 PREV TRANSFER]
    m = base & (~origin_short) & destin_short & (prev == "2") & (nxt == "0")
    m = m & _origin_access_ok() & (df["DESTIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer1_Distance"] <= 0.10) & (df["Transfer2_Distance"] <= 0.10) & (origin_near <= 10) & (destin_near <= 5)
    _emit(m, "NOTWalk-Walk [2 PREV TRANSFER]")

    # NOTWalk-Walk [1 NEXT TRANSFER]
    m = base & (~origin_short) & destin_short & (prev == "0") & (nxt == "1")
    m = m & _origin_access_ok() & (df["DESTIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer5_Distance"] <= 0.10) & (origin_near <= 10) & (destin_near <= 5)
    _emit(m, "NOTWalk-Walk [1 NEXT TRANSFER]")

    # NOTWalk-Walk [2 NEXT TRANSFER] — power_query_od_pipeline.phase2_notwalk lines 989-1000 (M: NEXT_TRANSFERS_Code_ == "2")
    m = base & (~origin_short) & destin_short & (prev == "0") & (nxt == "2")
    m = m & _origin_access_ok()
    m = m & (pd.to_numeric(df["DESTIN2Transfer1_Distance"], errors="coerce") <= 1)
    m = m & (pd.to_numeric(df["ORIGIN_TO_DESTIN"], errors="coerce") >= 0.50)
    m = m & (pd.to_numeric(df["Transfer5_Distance"], errors="coerce") <= 0.10)
    m = m & (pd.to_numeric(df["Transfer6_Distance"], errors="coerce") <= 0.10)
    m = m & (origin_near <= 10) & (destin_near <= 5)
    _emit(m, "NOTWalk-Walk [2 NEXT TRANSFER]")

    # Walk-NOTWalk [1 PREV TRANSFER]
    m = base & origin_short & (~destin_short) & (prev == "1") & (nxt == "0")
    m = m & _destin_access_ok() & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer1_Distance"] <= 0.10) & (origin_near <= 5) & (destin_near <= 10)
    _emit(m, "Walk-NOTWalk [1 PREV TRANSFER]")

    # Walk-NOTWalk [2 PREV TRANSFER]
    m = base & origin_short & (~destin_short) & (prev == "2") & (nxt == "0")
    m = m & _destin_access_ok() & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer1_Distance"] <= 0.10) & (df["Transfer2_Distance"] <= 0.10) & (origin_near <= 5) & (destin_near <= 10)
    _emit(m, "Walk-NOTWalk [2 PREV TRANSFER]")

    # Walk-NOTWalk [1 NEXT TRANSFER]
    m = base & origin_short & (~destin_short) & (prev == "0") & (nxt == "1")
    m = m & _destin_access_ok() & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer5_Distance"] <= 0.10) & (origin_near <= 5) & (destin_near <= 10)
    _emit(m, "Walk-NOTWalk [1 NEXT TRANSFER]")

    # Walk-NOTWalk [2 NEXT TRANSFER] — M / power_query_od_pipeline: NEXT = "1" with Transfer5 & Transfer6 (same pattern as Walk-Walk [2 NEXT])
    m = base & origin_short & (~destin_short) & (prev == "0") & (nxt == "1")
    m = m & _destin_access_ok() & (df["ORIGIN2Transfer1_Distance"] <= 1) & (df["ORIGIN_TO_DESTIN"] >= 0.50)
    m = m & (df["Transfer5_Distance"] <= 0.10) & (df["Transfer6_Distance"] <= 0.10) & (origin_near <= 5) & (destin_near <= 10)
    _emit(m, "Walk-NOTWalk [2 NEXT TRANSFER]")

    if not rows:
        return pd.DataFrame(columns=[REQUIRED_ID, COL_GROUP])
    out = pd.concat(rows, ignore_index=True).drop_duplicates(subset=[REQUIRED_ID]).sort_values(REQUIRED_ID).reset_index(drop=True)
    logger.info("Phase2_NotWalkOptions: %d ids", len(out))
    return out


def _phase_all(phase1: pd.DataFrame, phase2: pd.DataFrame) -> pd.DataFrame:
    """FROM M CODE: Phase-All = Table.Combine({Phase1_Walk-Walk, Phase2_NotWalkOptions})."""
    combined = pd.concat([phase1, phase2], ignore_index=True)
    combined = combined.drop_duplicates(subset=[REQUIRED_ID]).sort_values(REQUIRED_ID).reset_index(drop=True)
    logger.info("Phase-All: %d ids", len(combined))
    return combined


def _resolve_transfer_coord_columns(merged: pd.DataFrame) -> Dict[str, Optional[str]]:
    """Resolve PBIX/ls2 column names for PREV/NEXT/STOP coords. Returns dict of logical name -> column name."""

    def _col(name: str, alt: Optional[str] = None, alt2: Optional[str] = None) -> Optional[str]:
        if name in merged.columns:
            return name
        if alt and alt in merged.columns:
            return alt
        if alt2 and alt2 in merged.columns:
            return alt2
        return None

    out = {}
    out["STOP_ON_LAT"] = _col("STOP_ON [LAT]", "STOP_ON_LAT_", "STOP_ON_LAT")
    out["STOP_ON_LON"] = _col("STOP_ON [LONG]", "STOP_ON_LONG_", "STOP_ON_LONG")
    out["STOP_OFF_LAT"] = _col("STOP_OFF [LAT]", "STOP_OFF_LAT_", "STOP_OFF_LAT")
    out["STOP_OFF_LON"] = _col("STOP_OFF [LONG]", "STOP_OFF_LONG_", "STOP_OFF_LONG")
    for i in range(1, 5):
        out[f"PREV{i}_ON_LAT"] = _col(f"PREV_TRAN_{i}_ON_BUS [LAT]", f"PREV_TRAN_{i}_ON_BUS_LAT_", f"PREV_TRAN_{i}_ON_BUS_LAT")
        out[f"PREV{i}_ON_LON"] = _col(f"PREV_TRAN_{i}_ON_BUS [LONG]", f"PREV_TRAN_{i}_ON_BUS_LONG_", f"PREV_TRAN_{i}_ON_BUS_LONG")
        out[f"PREV{i}_OFF_LAT"] = _col(f"PREV_TRAN_{i}_OFF_BUS [LAT]", f"PREV_TRAN_{i}_OFF_BUS_LAT_", f"PREV_TRAN_{i}_OFF_BUS_LAT")
        out[f"PREV{i}_OFF_LON"] = _col(f"PREV_TRAN_{i}_OFF_BUS [LONG]", f"PREV_TRAN_{i}_OFF_BUS_LONG_", f"PREV_TRAN_{i}_OFF_BUS_LONG")
        out[f"NEXT{i}_ON_LAT"] = _col(f"NEXT_TRAN_{i}_ON_BUS [LAT]", f"NEXT_TRAN_{i}_ON_BUS_LAT_", f"NEXT_TRAN_{i}_ON_BUS_LAT")
        out[f"NEXT{i}_ON_LON"] = _col(f"NEXT_TRAN_{i}_ON_BUS [LONG]", f"NEXT_TRAN_{i}_ON_BUS_LONG_", f"NEXT_TRAN_{i}_ON_BUS_LONG")
        out[f"NEXT{i}_OFF_LAT"] = _col(f"NEXT_TRAN_{i}_OFF_BUS [LAT]", f"NEXT_TRAN_{i}_OFF_BUS_LAT_", f"NEXT_TRAN_{i}_OFF_BUS_LAT")
        out[f"NEXT{i}_OFF_LON"] = _col(f"NEXT_TRAN_{i}_OFF_BUS [LONG]", f"NEXT_TRAN_{i}_OFF_BUS_LONG_", f"NEXT_TRAN_{i}_OFF_BUS_LONG")
    return out


def _trip_board_walk_start_coords(
    r: pd.Series,
    C: Dict[str, Optional[str]],
    prev_n: int,
    o_lat: str,
    o_lon: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Start of walk to current boarding stop: merged origin if no prior transfers; else last PREV alight (PREV{n}_OFF)."""
    if prev_n <= 0:
        return _safe_float(r.get(o_lat)), _safe_float(r.get(o_lon))
    k = min(prev_n, 4)
    plat, plon = C.get(f"PREV{k}_OFF_LAT"), C.get(f"PREV{k}_OFF_LON")
    if not plat or not plon:
        return None, None
    return _safe_float(r.get(plat)), _safe_float(r.get(plon))


def _trip_final_walk_start_coords(
    r: pd.Series,
    C: Dict[str, Optional[str]],
    next_n: int,
    a_lat: str,
    a_lon: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Start of walk to merged destination: surveyed alight if no onward transfers; else last post-segment alight (NEXT{n}_OFF)."""
    if next_n <= 0:
        return _safe_float(r.get(a_lat)), _safe_float(r.get(a_lon))
    k = min(next_n, 4)
    nlat, nlon = C.get(f"NEXT{k}_OFF_LAT"), C.get(f"NEXT{k}_OFF_LON")
    if not nlat or not nlon:
        return None, None
    return _safe_float(r.get(nlat)), _safe_float(r.get(nlon))


def _ensure_transfer_leg_distances(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure Transfer1/2/5/6_Distance columns exist on the given dataframe.
    Uses the same logic as distance_transfer_check but operates in-place on df.
    """
    required_cols = ["Transfer1_Distance", "Transfer2_Distance", "Transfer5_Distance", "Transfer6_Distance"]
    if all(c in df.columns for c in required_cols):
        return df

    merged = df.copy()
    C = _resolve_transfer_coord_columns(merged)

    def _dist_with_on_fallback(
        off_lat_col: Optional[str],
        off_lon_col: Optional[str],
        on_lat_col: Optional[str],
        on_lon_col: Optional[str],
        on_lat_fallback_col: Optional[str],
        on_lon_fallback_col: Optional[str],
    ) -> pd.Series:
        """
        Transfer-leg distance with row-level fallback:
        if ON coords are null for a row, use fallback ON coords.
        """
        if not off_lat_col or not off_lon_col:
            return pd.Series(0.0, index=merged.index)
        if (on_lat_col and on_lon_col) is None and (on_lat_fallback_col and on_lon_fallback_col) is None:
            return pd.Series(0.0, index=merged.index)

        def _row(r: pd.Series) -> float:
            lat1 = _safe_float(r.get(off_lat_col))
            lon1 = _safe_float(r.get(off_lon_col))

            lat2 = _safe_float(r.get(on_lat_col)) if on_lat_col else None
            lon2 = _safe_float(r.get(on_lon_col)) if on_lon_col else None

            # Row-level fallback if ON coords are missing.
            if (lat2 is None or lon2 is None) and on_lat_fallback_col and on_lon_fallback_col:
                lat2 = _safe_float(r.get(on_lat_fallback_col))
                lon2 = _safe_float(r.get(on_lon_fallback_col))

            if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
                return float("nan")
            return fn_acos_distance_miles(lat1, lon1, lat2, lon2)

        return merged.apply(_row, axis=1)

    # PREV legs: Transfer1, Transfer2 distances
    for i in range(1, 3):
        off_lat, off_lon = C.get(f"PREV{i}_OFF_LAT"), C.get(f"PREV{i}_OFF_LON")
        # M-code fallback is row-level: if PREV{i+1}_ON coords are null for a row,
        # use STOP_ON coords (board) for that row's distance.
        primary_on_lat, primary_on_lon = C.get(f"PREV{i+1}_ON_LAT"), C.get(f"PREV{i+1}_ON_LON")
        fallback_on_lat, fallback_on_lon = C.get("STOP_ON_LAT"), C.get("STOP_ON_LON")
        d = _dist_with_on_fallback(
            off_lat,
            off_lon,
            primary_on_lat,
            primary_on_lon,
            fallback_on_lat,
            fallback_on_lon,
        )
        merged[f"Transfer{i}_Distance"] = d.fillna(0)

    # NEXT legs: Transfer5 (NEXT1_ON->ALIGHT), Transfer6 (NEXT1_OFF->NEXT2_ON)
    d5 = _dist_with_on_fallback(
        C.get("NEXT1_ON_LAT"),
        C.get("NEXT1_ON_LON"),
        C.get("STOP_OFF_LAT"),
        C.get("STOP_OFF_LON"),
        None,
        None,
    )
    merged["Transfer5_Distance"] = d5.fillna(0)

    d6 = _dist_with_on_fallback(
        C.get("NEXT1_OFF_LAT"),
        C.get("NEXT1_OFF_LON"),
        C.get("NEXT2_ON_LAT"),
        C.get("NEXT2_ON_LON"),
        None,
        None,
    )
    merged["Transfer6_Distance"] = d6.fillna(0)

    # write back into original df
    for c in ["Transfer1_Distance", "Transfer2_Distance", "Transfer5_Distance", "Transfer6_Distance"]:
        df[c] = merged[c]
    return df

def distance_transfer_check(
    phase_all_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    og_df: pd.DataFrame,
    xfer_list: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    __(01) OD [DISTANCE TRANSFERS CHECK] - full M code parity (all 7 conditions that feed into step).
    FROM M CODE:
    1-8. Transfer1..8_Distance (between legs; haversine * 3959); TRANSFER FLAG DISTANCE = 1 if any > 0.10.
    1-8. Transfer1..8_onroute_Distance (within-leg off->on); TRANSFER_onroute_FLAG = 1 if any valid on-route = 0.
    #ofTransferGPS = List.NonNullCount(32 coords); #ofTranfers = PREV+NEXT (− AGNECY count after xfer_list merges);
    # OF TRANSFER POINT CHECK = 1 if (#ofTransferGPS/4) <> #ofTranfers.
    DISTANCE TRANSFER CHECK = TRANSFER_onroute_FLAG + TRANSFER FLAG DISTANCE + # OF TRANSFER POINT CHECK. Filter = 0.
    """
    if phase_all_df.empty:
        return phase_all_df.copy()

    merged = phase_all_df.merge(og_df, on=REQUIRED_ID, how="left", suffixes=("", "_og"))
    if (
        xfer_list is not None
        and not xfer_list.empty
        and _add_xfer_list_merges is not None
    ):
        merged = _add_xfer_list_merges(merged, xfer_list)
    C = _resolve_transfer_coord_columns(merged)

    def _dist(lat1: Optional[str], lon1: Optional[str], lat2: Optional[str], lon2: Optional[str]) -> pd.Series:
        if not all([lat1, lon1, lat2, lon2]):
            return pd.Series(0.0, index=merged.index)
        return merged.apply(
            lambda r: fn_acos_distance_miles(
                _safe_float(r.get(lat1)), _safe_float(r.get(lon1)),
                _safe_float(r.get(lat2)), _safe_float(r.get(lon2)),
            ) or 0.0,
            axis=1,
        )

    def _onroute_seg(lat1: Optional[str], lon1: Optional[str], lat2: Optional[str], lon2: Optional[str]) -> pd.Series:
        """M _onroute_distances: null distance when coord columns missing (do not treat as 0 mi)."""
        if not all([lat1, lon1, lat2, lon2]):
            return pd.Series(np.nan, index=merged.index)
        return merged.apply(
            lambda r: fn_acos_distance_miles(
                _safe_float(r.get(lat1)), _safe_float(r.get(lon1)),
                _safe_float(r.get(lat2)), _safe_float(r.get(lon2)),
            ),
            axis=1,
        )

    # Transfer1..4: PREV leg segments (off -> next on or board)
    transfer_distances = []
    for i in range(1, 5):
        off_lat, off_lon = C.get(f"PREV{i}_OFF_LAT"), C.get(f"PREV{i}_OFF_LON")
        if i < 4:
            on_lat = C.get(f"PREV{i+1}_ON_LAT") or C.get("STOP_ON_LAT")
            on_lon = C.get(f"PREV{i+1}_ON_LON") or C.get("STOP_ON_LON")
        else:
            on_lat, on_lon = C.get("STOP_ON_LAT"), C.get("STOP_ON_LON")
        d = _dist(off_lat, off_lon, on_lat, on_lon)
        merged[f"Transfer{i}_Distance"] = d.fillna(0)
        transfer_distances.append(merged[f"Transfer{i}_Distance"])
    # Transfer5: NEXT1_ON -> ALIGHT
    d5 = _dist(
        C.get("NEXT1_ON_LAT"), C.get("NEXT1_ON_LON"),
        C.get("STOP_OFF_LAT"), C.get("STOP_OFF_LON"),
    )
    merged["Transfer5_Distance"] = d5.fillna(0)
    transfer_distances.append(merged["Transfer5_Distance"])
    # Transfer6,7,8: NEXT1_OFF->NEXT2_ON, NEXT2_OFF->NEXT3_ON, NEXT3_OFF->NEXT4_ON
    for j in [2, 3, 4]:
        off_lat = C.get(f"NEXT{j-1}_OFF_LAT")
        off_lon = C.get(f"NEXT{j-1}_OFF_LON")
        on_lat = C.get(f"NEXT{j}_ON_LAT")
        on_lon = C.get(f"NEXT{j}_ON_LON")
        d = _dist(off_lat, off_lon, on_lat, on_lon)
        merged[f"Transfer{4+j}_Distance"] = d.fillna(0)
        transfer_distances.append(merged[f"Transfer{4+j}_Distance"])

    # TRANSFER FLAG DISTANCE: 1 if any Transfer*_Distance > 0.10 (M: .10)
    transfer_flag_distance = pd.Series(0, index=merged.index, dtype=int)
    for d in transfer_distances:
        transfer_flag_distance = transfer_flag_distance | (d > TRANSFER_DISTANCE_FLAG_MILES)
    merged["TRANSFER FLAG DISTANCE"] = transfer_flag_distance.astype(int)

    # Transfer1..8 on-route: M _transfer_onroute_flag_eq_zero — 1 if any non-null leg distance equals 0.
    for i in range(1, 5):
        lat1, lon1 = C.get(f"PREV{i}_OFF_LAT"), C.get(f"PREV{i}_OFF_LON")
        lat2, lon2 = C.get(f"PREV{i}_ON_LAT"), C.get(f"PREV{i}_ON_LON")
        merged[f"Transfer{i}_onroute_Distance"] = _onroute_seg(lat1, lon1, lat2, lon2)
    for i in range(1, 5):
        lat1, lon1 = C.get(f"NEXT{i}_OFF_LAT"), C.get(f"NEXT{i}_OFF_LON")
        lat2, lon2 = C.get(f"NEXT{i}_ON_LAT"), C.get(f"NEXT{i}_ON_LON")
        merged[f"Transfer{4+i}_onroute_Distance"] = _onroute_seg(lat1, lon1, lat2, lon2)

    _onroute_names = [f"Transfer{k}_onroute_Distance" for k in range(1, 9)]

    def _onroute_flag_row(row: pd.Series) -> int:
        valid: List[float] = []
        for name in _onroute_names:
            x = row.get(name)
            if x is None or (isinstance(x, float) and math.isnan(x)) or pd.isna(x):
                continue
            valid.append(float(x))
        for v in valid:
            if v == 0:
                return 1
        return 0

    merged["TRANSFER_onroute_FLAG"] = merged.apply(_onroute_flag_row, axis=1)

    # #ofTransferGPS: M GPS_COUNT_COLS + List.NonNullCount, then /4 before compare to #ofTranfers
    if GPS_COUNT_COLS and _is_null_m is not None:
        no_raw = merged.apply(
            lambda r: sum(1 for c in GPS_COUNT_COLS if c in r.index and not _is_null_m(r.get(c))),
            axis=1,
        )
    else:
        coord_cols: List[str] = []
        for i in range(1, 5):
            for suf in ["_ON_LAT", "_ON_LON", "_OFF_LAT", "_OFF_LON"]:
                for prefix in ["PREV", "NEXT"]:
                    k = f"{prefix}{i}{suf}"
                    if C.get(k) and C[k] in merged.columns:
                        coord_cols.append(C[k])
        no_raw = merged[coord_cols].notna().sum(axis=1) if coord_cols else pd.Series(0, index=merged.index)
    merged["#ofTransferGPS"] = no_raw.astype(float) / 4.0

    prev_code = merged.get(COL_PREV_TRANSFERS_CODE, pd.Series("0", index=merged.index))
    next_code = merged.get(COL_NEXT_TRANSFERS_CODE, pd.Series("0", index=merged.index))
    prev_int = prev_code.fillna(0).apply(_prev_next_code_to_int)
    next_int = next_code.fillna(0).apply(_prev_next_code_to_int)
    base_cnt = prev_int + next_int
    if xfer_list is not None and not xfer_list.empty and _is_null_m is not None:
        ag_cols = [f"AGNECY_TRANSFERS-{i}" for i in range(1, 9)]
        ag_n = merged.apply(
            lambda r: sum(1 for c in ag_cols if c in r.index and not _is_null_m(r.get(c))),
            axis=1,
        )
        merged["#ofTranfers"] = base_cnt - ag_n
    else:
        merged["#ofTranfers"] = base_cnt

    merged["# OF TRANSFER POINT CHECK"] = (merged["#ofTransferGPS"] != merged["#ofTranfers"]).astype(int)

    merged["DISTANCE TRANSFER CHECK"] = (
        merged["TRANSFER_onroute_FLAG"] + merged["TRANSFER FLAG DISTANCE"] + merged["# OF TRANSFER POINT CHECK"]
    )
    merged["DISTANCE TRANSFER CHECK"] = merged["DISTANCE TRANSFER CHECK"].fillna(2).astype(int)
    filtered = merged[merged["DISTANCE TRANSFER CHECK"] == 0]
    result = phase_all_df[phase_all_df[REQUIRED_ID].isin(filtered[REQUIRED_ID])].drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)
    logger.info("__(01) OD [DISTANCE TRANSFERS CHECK]: %d ids (from %d)", len(result), len(phase_all_df))
    return result


def transfers_check(step01_df: pd.DataFrame, og_df: pd.DataFrame) -> pd.DataFrame:
    """
    __(02) OD [TRANSFERS CHECK].
    FROM M CODE: PREV_TRANSFER CHECK = (count of TRIP_FIRST..FOURTH non-null) = PREV_TRANSFERS[Code];
    NEXT_TRANSFER CHECK = (count of TRIP_NEXT, AFTER, 3RD, LAST4TH non-null) = NEXT_TRANSFERS[Code];
    Duplicate Transfers = 1 if distinct count < non-null count; OD [TRANSFERS CHECK] FLAGS = sum; filter FLAGS <> 1.
    """
    if step01_df.empty:
        return step01_df.copy()

    merged = step01_df.merge(
        og_df[[REQUIRED_ID] + [c for c in [
            COL_ROUTE_SURVEYED_CODE, COL_PREV_TRANSFERS_CODE, COL_NEXT_TRANSFERS_CODE,
            COL_TRIP_FIRST_ROUTE, COL_TRIP_SECOND_ROUTE, COL_TRIP_THIRD_ROUTE, COL_TRIP_FOURTH_ROUTE,
            COL_TRIP_NEXT_ROUTE, COL_TRIP_AFTER_ROUTE, COL_TRIP_3RD_ROUTE, COL_TRIP_LAST4TH_RTE,
        ] if c in og_df.columns]],
        on=REQUIRED_ID,
        how="left",
    )
    prev_cols = [c for c in [COL_TRIP_FIRST_ROUTE, COL_TRIP_SECOND_ROUTE, COL_TRIP_THIRD_ROUTE, COL_TRIP_FOURTH_ROUTE] if c in merged.columns]
    next_cols = [c for c in [COL_TRIP_NEXT_ROUTE, COL_TRIP_AFTER_ROUTE, COL_TRIP_3RD_ROUTE, COL_TRIP_LAST4TH_RTE] if c in merged.columns]
    route_keys = [
        COL_ROUTE_SURVEYED_CODE,
        COL_TRIP_FIRST_ROUTE,
        COL_TRIP_SECOND_ROUTE,
        COL_TRIP_THIRD_ROUTE,
        COL_TRIP_FOURTH_ROUTE,
        COL_TRIP_NEXT_ROUTE,
        COL_TRIP_AFTER_ROUTE,
        COL_TRIP_3RD_ROUTE,
        COL_TRIP_LAST4TH_RTE,
    ]
    if _nz is not None:
        def prev_chk(r: pd.Series) -> int:
            nn = sum(1 for k in prev_cols if _nz(r.get(k)))
            try:
                want = int(float(str(r[COL_PREV_TRANSFERS_CODE]).strip()))
            except (TypeError, ValueError, KeyError):
                want = -1
            return 0 if nn == want else 1

        def next_chk(r: pd.Series) -> int:
            nn = sum(1 for k in next_cols if _nz(r.get(k)))
            try:
                want = int(float(str(r[COL_NEXT_TRANSFERS_CODE]).strip()))
            except (TypeError, ValueError, KeyError):
                want = -1
            return 0 if nn == want else 1

        def dup_transfers(r: pd.Series) -> int:
            vals = [r.get(k) for k in route_keys]
            non_null = [v for v in vals if _nz(v)]
            nn = len(non_null)
            if nn == 0:
                return 0
            return 0 if len(set(non_null)) == nn else 1

        merged["PREV_TRANSFER CHECK"] = merged.apply(prev_chk, axis=1)
        merged["NEXT_TRANSFER CHECK"] = merged.apply(next_chk, axis=1)
        merged["TRANSFER COUNT FLAG"] = merged["PREV_TRANSFER CHECK"] + merged["NEXT_TRANSFER CHECK"]
        merged["Duplicate Transfers"] = merged.apply(dup_transfers, axis=1)
    else:
        prev_count_filled = merged[prev_cols].notna().sum(axis=1) if prev_cols else pd.Series(0, index=merged.index)
        next_count_filled = merged[next_cols].notna().sum(axis=1) if next_cols else pd.Series(0, index=merged.index)
        prev_code = merged.get(COL_PREV_TRANSFERS_CODE, pd.Series(0, index=merged.index)).fillna(0).apply(_prev_next_code_to_int)
        next_code = merged.get(COL_NEXT_TRANSFERS_CODE, pd.Series(0, index=merged.index)).fillna(0).apply(_prev_next_code_to_int)
        merged["TRANSFER COUNT FLAG"] = (prev_count_filled != prev_code).astype(int) + (next_count_filled != next_code).astype(int)
        list_cols = [c for c in [COL_ROUTE_SURVEYED_CODE] + prev_cols + next_cols if c in merged.columns]
        if list_cols:
            non_null_count = merged[list_cols].notna().sum(axis=1)
            distinct_count = merged[list_cols].apply(
                lambda r: len(set(x for x in r if pd.notna(x) and str(x).strip())),
                axis=1,
            )
            merged["Duplicate Transfers"] = (distinct_count < non_null_count).astype(int)
        else:
            merged["Duplicate Transfers"] = 0
    merged["OD [TRANSFERS CHECK] FLAGS"] = merged["TRANSFER COUNT FLAG"] + merged["Duplicate Transfers"]
    filtered = merged[merged["OD [TRANSFERS CHECK] FLAGS"] != 1]
    result = step01_df[step01_df[REQUIRED_ID].isin(filtered[REQUIRED_ID])].drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)
    logger.info("__(02) OD [TRANSFERS CHECK]: %d ids (from %d)", len(result), len(step01_df))
    return result


def trip_distance_check(
    step02_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    og_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    __(03) OD [TRIP DISTANCE CHECK].
    FROM M CODE: Merge CLEAN, compute ORIGIN_TO_BOARD, BOARDING_TO_ALIGHTING, ORIGIN_TO_DESTINATION,
    ALIGHTING_TO_DESTINATION (haversine * 3959), then O-B, A-D, O-D, B-A checks; OD [DISTANCE CHECK] FLAGS; filter <> 1.

    Transfer-aware legs (not merged origin / not STOP_OFF when transfers imply a different walk endpoint):
    - prev > 0: ORIGIN_TO_BOARD = PREV{prev}_OFF_BUS -> STOP_ON (last prior alight to current board).
    - next > 0: ALIGHTING_TO_DESTINATION = NEXT{next}_OFF_BUS -> merged destination (final alight before walk to D).
    """
    if step02_df.empty:
        logger.info("__(03) OD [TRIP DISTANCE CHECK]: 0 ids (from 0)")
        return step02_df.copy()

    clean = _ensure_clean_dataset_columns(clean_df)
    merged = step02_df.merge(clean, on=REQUIRED_ID, how="left", suffixes=("", "_clean"))

    extra_og_cols = [COL_FINAL_DIRECTION, "VAL_DIST_OtoPre0", COL_VAL_DIST_NOFF_TO_D, "VAL_DIST_OTOPRE_0"]
    if og_df is not None:
        og_cols = [REQUIRED_ID] + [c for c in extra_og_cols if c in og_df.columns]
        if len(og_cols) > 1:
            ogx = og_df[og_cols].copy()
            merged = merged.merge(ogx, on=REQUIRED_ID, how="left", suffixes=("", "_og"))
            for c in extra_og_cols:
                cog = f"{c}_og"
                if cog in merged.columns:
                    if c in merged.columns:
                        merged[c] = merged[c].fillna(merged[cog])
                    else:
                        merged[c] = merged[cog]
                    merged = merged.drop(columns=[cog], errors="ignore")

    def _coord_col(*candidates):
        for c in candidates:
            if c and c in merged.columns:
                return c
        return None

    C = _resolve_transfer_coord_columns(merged)
    o_lat = _coord_col(COL_MERGED_ORIGIN_LAT, "ORIGIN_ADDRESS_LAT_", "ORIGIN_ADDRESS [LAT]_clean")
    o_lon = _coord_col(COL_MERGED_ORIGIN_LONG, "ORIGIN_ADDRESS_LONG_", "ORIGIN_ADDRESS [LONG]_clean")
    b_lat = _coord_col(C.get("STOP_ON_LAT"), ALIAS_STOP_ON_LAT, "STOP_ON_LAT_", "STOP_ON [LAT]_clean", "StopOn_LAT")
    b_lon = _coord_col(C.get("STOP_ON_LON"), ALIAS_STOP_ON_LONG, "STOP_ON_LONG_", "STOP_ON [LONG]_clean", "StopOn_LONG")
    a_lat = _coord_col(C.get("STOP_OFF_LAT"), ALIAS_STOP_OFF_LAT, "STOP_OFF_LAT_", "STOP_OFF [LAT]_clean", "StopOff_LAT")
    a_lon = _coord_col(C.get("STOP_OFF_LON"), ALIAS_STOP_OFF_LONG, "STOP_OFF_LONG_", "STOP_OFF [LONG]_clean", "StopOff_LONG")
    d_lat = _coord_col(COL_MERGED_DESTIN_LAT, "DESTIN_ADDRESS_LAT_", "DESTIN_ADDRESS [LAT]_clean")
    d_lon = _coord_col(COL_MERGED_DESTIN_LONG, "DESTIN_ADDRESS_LONG_", "DESTIN_ADDRESS [LONG]_clean")
    if not all([o_lat, o_lon, b_lat, b_lon, a_lat, a_lon, d_lat, d_lon]):
        logger.warning("Trip distance check: missing coord columns; passing all")
        logger.info("__(03) OD [TRIP DISTANCE CHECK]: %d ids (from %d)", len(step02_df), len(step02_df))
        return step02_df.copy()

    merged["ORIGIN_TO_BOARD"] = merged.apply(
        lambda r: fn_acos_distance_miles(
            *_trip_board_walk_start_coords(
                r, C, _prev_next_code_to_int(r.get(COL_PREV_TRANSFERS_CODE, 0)), o_lat, o_lon
            ),
            _safe_float(r.get(b_lat)),
            _safe_float(r.get(b_lon)),
        ),
        axis=1,
    )
    merged["BOARDING_TO_ALIGHTING"] = merged.apply(
        lambda r: fn_acos_distance_miles(
            _safe_float(r.get(a_lat)), _safe_float(r.get(a_lon)),
            _safe_float(r.get(b_lat)), _safe_float(r.get(b_lon)),
        ),
        axis=1,
    )
    merged["ORIGIN_TO_DESTINATION"] = merged.apply(
        lambda r: fn_acos_distance_miles(
            _safe_float(r.get(o_lat)), _safe_float(r.get(o_lon)),
            _safe_float(r.get(d_lat)), _safe_float(r.get(d_lon)),
        ),
        axis=1,
    )
    merged["ALIGHTING_TO_DESTINATION"] = merged.apply(
        lambda r: fn_acos_distance_miles(
            *_trip_final_walk_start_coords(
                r, C, _prev_next_code_to_int(r.get(COL_NEXT_TRANSFERS_CODE, 0)), a_lat, a_lon
            ),
            _safe_float(r.get(d_lat)),
            _safe_float(r.get(d_lon)),
        ),
        axis=1,
    )
    merged["O2B/O2D"] = merged["ORIGIN_TO_BOARD"] / merged["ORIGIN_TO_DESTINATION"]
    merged["B2A/OD"] = merged["BOARDING_TO_ALIGHTING"] / merged["ORIGIN_TO_DESTINATION"]
    merged["A2D/OD"] = merged["ALIGHTING_TO_DESTINATION"] / merged["ORIGIN_TO_DESTINATION"]

    origin_mode = merged.get(COL_ORIGIN_TRANSPORT_MODE, merged.get(COL_ORIGIN_TRANSPORT, ""))
    destin_mode = merged.get(COL_DESTIN_TRANSPORT_MODE, merged.get(COL_DESTIN_TRANSPORT, ""))
    if isinstance(origin_mode, pd.Series):
        origin_short = origin_mode.fillna("").astype(str).str.contains(TRANSPORT_SHORT, regex=False, na=False)
    else:
        origin_short = pd.Series(False, index=merged.index)
    if isinstance(destin_mode, pd.Series):
        destin_short = destin_mode.fillna("").astype(str).str.contains(TRANSPORT_SHORT, regex=False, na=False)
    else:
        destin_short = pd.Series(False, index=merged.index)
    # M code O-B_Dist_Check2 / A-D_Dist_Check2: SHORT = false AND MEDIUM = false (i.e. LONG or other)
    if isinstance(origin_mode, pd.Series):
        origin_medium = origin_mode.fillna("").astype(str).str.contains(TRANSPORT_MEDIUM, regex=False, na=False)
    else:
        origin_medium = pd.Series(False, index=merged.index)
    if isinstance(destin_mode, pd.Series):
        destin_medium = destin_mode.fillna("").astype(str).str.contains(TRANSPORT_MEDIUM, regex=False, na=False)
    else:
        destin_medium = pd.Series(False, index=merged.index)
    origin_not_short_not_medium = ~origin_short & ~origin_medium
    destin_not_short_not_medium = ~destin_short & ~destin_medium

    def _contains_pq(txt: Any, sub: str) -> bool:
        return sub in str(txt or "")

    p_code_s = merged.get(COL_PREV_TRANSFERS_CODE, pd.Series("0", index=merged.index)).fillna("0").astype(str).str.strip()
    n_code_s = merged.get(COL_NEXT_TRANSFERS_CODE, pd.Series("0", index=merged.index)).fillna("0").astype(str).str.strip()
    prev0 = p_code_s == "0"
    next0 = n_code_s == "0"

    # M: O-B_Dist_Check1/2/3, A-D_Dist_Check1/2/3, O-D 1/2/3, B-A 1/2, WheelchairAccessEgress
    o_b_1 = ((merged["ORIGIN_TO_BOARD"] > ORIGIN_TO_BOARD_MAX_WALK_MILES) & origin_short & prev0).astype(int)
    o_b_2 = ((merged["ORIGIN_TO_BOARD"] < ORIGIN_TO_BOARD_MIN_NON_WALK_MILES) & origin_not_short_not_medium).astype(int)
    o_b_3 = ((merged["ORIGIN_TO_BOARD"] < ORIGIN_TO_BOARD_MIN_WITH_PREV_MILES) & ~prev0).astype(int)
    a_d_1 = ((merged["ALIGHTING_TO_DESTINATION"] > ALIGHTING_TO_DEST_MAX_WALK_MILES) & destin_short & next0).astype(int)
    a_d_2 = ((merged["ALIGHTING_TO_DESTINATION"] < ALIGHTING_TO_DEST_MIN_NON_WALK_MILES) & destin_not_short_not_medium).astype(int)
    a_d_3 = ((merged["ALIGHTING_TO_DESTINATION"] < ALIGHTING_TO_DEST_MIN_WITH_NEXT_MILES) & ~next0).astype(int)
    o_d_1 = (merged["ORIGIN_TO_DESTINATION"] < ORIGIN_TO_DEST_MIN_VERY_CLOSE).astype(int)
    o_d_2 = (merged["ORIGIN_TO_DESTINATION"] < ORIGIN_TO_DEST_MIN_CLOSE).astype(int)
    o_d_3 = (merged["ORIGIN_TO_DESTINATION"] > ORIGIN_TO_DEST_MAX_FAR).astype(int)
    b_a_1 = (merged["B2A/OD"] > B2A_OD_RATIO_MAX).astype(int)

    def _ba_check2_row(r: pd.Series) -> int:
        try:
            if not (float(r["B2A/OD"]) < B2A_OD_RATIO_MIN_SHORT_SHORT):
                return 0
        except (TypeError, ValueError):
            return 0
        otm = str(r.get(COL_ORIGIN_TRANSPORT_MODE, "") or "")
        dtm = str(r.get(COL_DESTIN_TRANSPORT_MODE, "") or "")
        fdir = str(r.get(COL_FINAL_DIRECTION, "") or "")
        p0 = str(r.get(COL_PREV_TRANSFERS_CODE, "")) == "0"
        n0 = str(r.get(COL_NEXT_TRANSFERS_CODE, "")) == "0"
        if (
            _contains_pq(otm, TRANSPORT_SHORT)
            and _contains_pq(dtm, TRANSPORT_SHORT)
            and not _contains_pq(fdir, "LOOP")
            and p0
            and n0
        ):
            return 1
        return 0

    b_a_2 = merged.apply(_ba_check2_row, axis=1)

    def _wheelchair_row(r: pd.Series) -> int:
        o = str(r.get(COL_ORIGIN_TRANSPORT, "") or "")
        dest = str(r.get(COL_DESTIN_TRANSPORT, "") or "")
        return int(
            (_contains_pq(o, "Wheelchair") and not _contains_pq(dest, "Wheelchair"))
            or (not _contains_pq(o, "Wheelchair") and _contains_pq(dest, "Wheelchair"))
        )

    wheelchair_oe = merged.apply(_wheelchair_row, axis=1)

    def _val_mi_parse_local(v: Any) -> Any:
        if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v):
            return 0
        s = str(v).strip()
        if not s:
            return 0
        if s in ("is not available.", "NANmi."):
            return 0
        if "mi." not in s:
            return 0
        return s.split("mi.", 1)[0].strip()

    def _val_otopre(r: pd.Series) -> Any:
        for k in ("VAL_DIST_OtoPre0", "VAL_DIST_OTOPRE_0"):
            if k in r.index and not (_is_null_m(r.get(k)) if _is_null_m is not None else pd.isna(r.get(k))):
                return r.get(k)
        return None

    def _val_noff(r: pd.Series) -> Any:
        if COL_VAL_DIST_NOFF_TO_D in r.index:
            return r.get(COL_VAL_DIST_NOFF_TO_D)
        return None

    def _prev_dist_chk(r: pd.Series) -> Any:
        if str(r.get(COL_PREV_TRANSFERS_CODE, "0")).strip() in ("", "0"):
            return 0
        return _val_mi_parse_local(_val_otopre(r))

    def _next_dist_chk(r: pd.Series) -> Any:
        if str(r.get(COL_NEXT_TRANSFERS_CODE, "0")).strip() in ("", "0"):
            return 0
        return _val_mi_parse_local(_val_noff(r))

    merged["PREV_TRANSFER_DIST_CHECK"] = merged.apply(_prev_dist_chk, axis=1)
    merged["NEXT_TRANSFER_DIST_CHECK"] = merged.apply(_next_dist_chk, axis=1)
    merged["PREV_TRANSFER_DIST_CHECK"] = pd.to_numeric(merged["PREV_TRANSFER_DIST_CHECK"], errors="coerce").fillna(0)
    merged["NEXT_TRANSFER_DIST_CHECK"] = pd.to_numeric(merged["NEXT_TRANSFER_DIST_CHECK"], errors="coerce").fillna(0)

    def _prev_flag_row(r: pd.Series) -> int:
        if str(r.get(COL_PREV_TRANSFERS_CODE, "0")).strip() in ("", "0"):
            return 0
        o = str(r.get(COL_ORIGIN_TRANSPORT, "") or "")
        try:
            if float(r["PREV_TRANSFER_DIST_CHECK"]) > PREV_NEXT_TRANSFER_WALK_FLAG_MILES and (
                _contains_pq(o, "Walk") or _contains_pq(o, "Wheelchair") or _contains_pq(o, "Skateboard")
            ):
                return 1
        except (TypeError, ValueError):
            pass
        return 0

    def _last_flag_row(r: pd.Series) -> int:
        if str(r.get(COL_NEXT_TRANSFERS_CODE, "0")).strip() in ("", "0"):
            return 0
        dest = str(r.get(COL_DESTIN_TRANSPORT, "") or "")
        try:
            if float(r["NEXT_TRANSFER_DIST_CHECK"]) > PREV_NEXT_TRANSFER_WALK_FLAG_MILES and (
                _contains_pq(dest, "Walk") or _contains_pq(dest, "Wheelchair") or _contains_pq(dest, "Skateboard")
            ):
                return 1
        except (TypeError, ValueError):
            pass
        return 0

    merged["PREV_TRANSFER_DIST_FLAG"] = merged.apply(_prev_flag_row, axis=1)
    merged["LAST_TRANSFER_DIST_FLAG"] = merged.apply(_last_flag_row, axis=1)

    err_map = {
        "LAST_TRANSFER_DIST_FLAG": 1,
        "PREV_TRANSFER_DIST_FLAG": 1,
        "NEXT_TRANSFER_DIST_CHECK": 1,
        "PREV_TRANSFER_DIST_CHECK": 1,
        "WheelchairAccessEgress": 1,
        "B-A_Dist_Check2": 1,
        "B-A_Dist_Check1": 1,
        "O-D_Dist_Check3": 1,
        "O-D_Dist_Check2": 1,
        "O-D_Dist_Check1": 1,
        "A-D_Dist_Check3": 1,
        "A-D_Dist_Check2": 1,
        "A-D_Dist_Check1": 1,
        "O-B_Dist_Check3": 1,
        "O-B_Dist_Check2": 1,
        "O-B_Dist_Check1": 1,
    }
    merged["O-B_Dist_Check1"] = o_b_1
    merged["O-B_Dist_Check2"] = o_b_2
    merged["O-B_Dist_Check3"] = o_b_3
    merged["A-D_Dist_Check1"] = a_d_1
    merged["A-D_Dist_Check2"] = a_d_2
    merged["A-D_Dist_Check3"] = a_d_3
    merged["O-D_Dist_Check1"] = o_d_1
    merged["O-D_Dist_Check2"] = o_d_2
    merged["O-D_Dist_Check3"] = o_d_3
    merged["B-A_Dist_Check1"] = b_a_1
    merged["B-A_Dist_Check2"] = b_a_2
    merged["WheelchairAccessEgress"] = wheelchair_oe
    for col, val in err_map.items():
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(val)

    merged["OD [DISTANCE CHECK] FLAGS (TRANSFERS)"] = merged["PREV_TRANSFER_DIST_FLAG"] + merged["LAST_TRANSFER_DIST_FLAG"]
    merged["OD [DISTANCE CHECK] FLAGS"] = (
        merged["O-B_Dist_Check1"]
        + merged["O-B_Dist_Check2"]
        + merged["O-B_Dist_Check3"]
        + merged["A-D_Dist_Check1"]
        + merged["A-D_Dist_Check2"]
        + merged["A-D_Dist_Check3"]
        + merged["O-D_Dist_Check1"]
        + merged["O-D_Dist_Check2"]
        + merged["O-D_Dist_Check3"]
        + merged["B-A_Dist_Check1"]
        + merged["B-A_Dist_Check2"]
        + merged["WheelchairAccessEgress"]
    )
    filtered = merged[merged["OD [DISTANCE CHECK] FLAGS"] != 1]
    result = step02_df[step02_df[REQUIRED_ID].isin(filtered[REQUIRED_ID])].drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)
    logger.info("__(03) OD [TRIP DISTANCE CHECK]: %d ids (from %d)", len(result), len(step02_df))
    return result


def build_supervisor_only_ids(all_ids: pd.Series, approved_ids: pd.Series) -> pd.Series:
    """IDs that passed pipeline are approved; the rest are Supervisor Only. FROM M CODE: OD [Supervisor Only Checks]."""
    return all_ids[~all_ids.isin(approved_ids)].drop_duplicates()


def _compute_extra_short_do_not_approve_mask(df: pd.DataFrame) -> pd.Series:
    """
    Return a boolean Series (index aligned to df) True where record must not be approved:
    BOARDING_TO_ALIGHTING < 0.25, ORIGIN_TO_DESTINATION < 0.25, route surveyed missing, or boarding == alighting.
    Used for debugging/validation of extra "do not approve" rules (Field Approved records are not overridden).
    """
    out = pd.Series(False, index=df.index)
    C = _resolve_transfer_coord_columns(df)
    b_lat = C.get("STOP_ON_LAT") or (ALIAS_STOP_ON_LAT if ALIAS_STOP_ON_LAT in df.columns else None)
    b_lon = C.get("STOP_ON_LON") or (ALIAS_STOP_ON_LONG if ALIAS_STOP_ON_LONG in df.columns else None)
    a_lat = C.get("STOP_OFF_LAT") or (ALIAS_STOP_OFF_LAT if ALIAS_STOP_OFF_LAT in df.columns else None)
    a_lon = C.get("STOP_OFF_LON") or (ALIAS_STOP_OFF_LONG if ALIAS_STOP_OFF_LONG in df.columns else None)
    o_lat = COL_MERGED_ORIGIN_LAT if COL_MERGED_ORIGIN_LAT in df.columns else None
    o_lon = COL_MERGED_ORIGIN_LONG if COL_MERGED_ORIGIN_LONG in df.columns else None
    d_lat = COL_MERGED_DESTIN_LAT if COL_MERGED_DESTIN_LAT in df.columns else None
    d_lon = COL_MERGED_DESTIN_LONG if COL_MERGED_DESTIN_LONG in df.columns else None
    if not all([b_lat, b_lon, a_lat, a_lon]):
        return out
    b2a = df.apply(
        lambda r: fn_haversine_miles(
            _safe_float(r.get(b_lat)), _safe_float(r.get(b_lon)),
            _safe_float(r.get(a_lat)), _safe_float(r.get(a_lon)),
        ),
        axis=1,
    )
    ba_too_short = (b2a < 0.25).fillna(False)
    b2a_zero = (b2a < 1e-6).fillna(False)
    coord_equal = (
        (pd.to_numeric(df[b_lat], errors="coerce").round(6) == pd.to_numeric(df[a_lat], errors="coerce").round(6))
        & (pd.to_numeric(df[b_lon], errors="coerce").round(6) == pd.to_numeric(df[a_lon], errors="coerce").round(6))
    ).fillna(False)
    boarding_equals_alighting = coord_equal | b2a_zero
    if o_lat and o_lon and d_lat and d_lon:
        o2d = df.apply(
            lambda r: fn_haversine_miles(
                _safe_float(r.get(o_lat)), _safe_float(r.get(o_lon)),
                _safe_float(r.get(d_lat)), _safe_float(r.get(d_lon)),
            ),
            axis=1,
        )
        od_too_short = (o2d < 0.25).fillna(False)
    else:
        od_too_short = pd.Series(False, index=df.index)
    rs = df.get(COL_ROUTE_SURVEYED_CODE, df.get("ROUTE_SURVEYED[Code]", pd.Series("", index=df.index)))
    rs_missing = rs.fillna("").astype(str).str.strip().eq("")
    return (ba_too_short | od_too_short | rs_missing | boarding_equals_alighting)


def _compute_approval_debug_for_ids(df: pd.DataFrame, id_values: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    For each id in id_values, compute B2A, O2D, route surveyed, and the four do-not-approve flags.
    Returns dict id -> {b2a, o2d, route_surveyed, ba_too_short, od_too_short, rs_missing, boarding_equals_alighting,
    stop_on_lat, stop_on_lon, stop_off_lat, stop_off_lon} for debugging why a record was/wasn't approved.
    """
    result: Dict[str, Dict[str, Any]] = {}
    C = _resolve_transfer_coord_columns(df)
    b_lat = C.get("STOP_ON_LAT") or (ALIAS_STOP_ON_LAT if ALIAS_STOP_ON_LAT in df.columns else None)
    b_lon = C.get("STOP_ON_LON") or (ALIAS_STOP_ON_LONG if ALIAS_STOP_ON_LONG in df.columns else None)
    a_lat = C.get("STOP_OFF_LAT") or (ALIAS_STOP_OFF_LAT if ALIAS_STOP_OFF_LAT in df.columns else None)
    a_lon = C.get("STOP_OFF_LON") or (ALIAS_STOP_OFF_LONG if ALIAS_STOP_OFF_LONG in df.columns else None)
    o_lat = COL_MERGED_ORIGIN_LAT if COL_MERGED_ORIGIN_LAT in df.columns else None
    o_lon = COL_MERGED_ORIGIN_LONG if COL_MERGED_ORIGIN_LONG in df.columns else None
    d_lat = COL_MERGED_DESTIN_LAT if COL_MERGED_DESTIN_LAT in df.columns else None
    d_lon = COL_MERGED_DESTIN_LONG if COL_MERGED_DESTIN_LONG in df.columns else None
    if not all([b_lat, b_lon, a_lat, a_lon]):
        return result
    id_col = REQUIRED_ID if REQUIRED_ID in df.columns else None
    if not id_col:
        return result
    subset = df[df[id_col].astype(str).isin([str(i) for i in id_values])].copy()
    if subset.empty:
        return result
    b2a = subset.apply(
        lambda r: fn_haversine_miles(
            _safe_float(r.get(b_lat)), _safe_float(r.get(b_lon)),
            _safe_float(r.get(a_lat)), _safe_float(r.get(a_lon)),
        ),
        axis=1,
    )
    ba_too_short = (b2a < 0.25).fillna(False)
    b2a_zero = (b2a < 1e-6).fillna(False)
    coord_equal = (
        (pd.to_numeric(subset[b_lat], errors="coerce").round(6) == pd.to_numeric(subset[a_lat], errors="coerce").round(6))
        & (pd.to_numeric(subset[b_lon], errors="coerce").round(6) == pd.to_numeric(subset[a_lon], errors="coerce").round(6))
    ).fillna(False)
    boarding_equals_alighting = (coord_equal | b2a_zero)
    if o_lat and o_lon and d_lat and d_lon:
        o2d = subset.apply(
            lambda r: fn_haversine_miles(
                _safe_float(r.get(o_lat)), _safe_float(r.get(o_lon)),
                _safe_float(r.get(d_lat)), _safe_float(r.get(d_lon)),
            ),
            axis=1,
        )
        od_too_short = (o2d < 0.25).fillna(False)
    else:
        o2d = pd.Series(np.nan, index=subset.index)
        od_too_short = pd.Series(False, index=subset.index)
    rs = subset.get(COL_ROUTE_SURVEYED_CODE, subset.get("ROUTE_SURVEYED[Code]", pd.Series("", index=subset.index)))
    rs_missing = rs.fillna("").astype(str).str.strip().eq("")
    for idx, row in subset.iterrows():
        rid = str(row[id_col])
        result[rid] = {
            "b2a": b2a.loc[idx] if idx in b2a.index else None,
            "o2d": o2d.loc[idx] if idx in o2d.index else None,
            "route_surveyed": str(rs.loc[idx]) if idx in rs.index else "",
            "ba_too_short": bool(ba_too_short.loc[idx]) if idx in ba_too_short.index else False,
            "od_too_short": bool(od_too_short.loc[idx]) if idx in od_too_short.index else False,
            "rs_missing": bool(rs_missing.loc[idx]) if idx in rs_missing.index else False,
            "boarding_equals_alighting": bool(boarding_equals_alighting.loc[idx]) if idx in boarding_equals_alighting.index else False,
            "stop_on_lat": row.get(b_lat), "stop_on_lon": row.get(b_lon),
            "stop_off_lat": row.get(a_lat), "stop_off_lon": row.get(a_lon),
        }
    return result


def _log_approval_debug(
    out: pd.DataFrame,
    approved_set: set,
    debug_ids: List[str],
    step03_df: Optional[pd.DataFrame] = None,
) -> None:
    """Log why each debug_id was approved (or not): pipeline vs Field Approved, and do-not-approve flags."""
    if not debug_ids:
        return
    id_col = REQUIRED_ID if REQUIRED_ID in out.columns else None
    if not id_col:
        logger.warning("Debug approval: no id column in output")
        return
    debug_info = _compute_approval_debug_for_ids(out, debug_ids)
    for did in debug_ids:
        did_str = str(did).strip()
        row = out[out[id_col].astype(str) == did_str]
        if row.empty:
            logger.info("[DEBUG approval] ID %s: not found in output", did_str)
            continue
        row = row.iloc[0]
        final_usage = row.get("Final_Usage", "")
        final_reviewer = row.get("FINAL_REVIEWER", "")
        in_pipeline = did_str in approved_set
        def _norm_header_name(x: Any) -> str:
            return re.sub(r"[^a-z0-9]+", "", str(x).strip().lower())

        expected_elvis_norm = _norm_header_name(COL_ELVIS_STATUS)
        elvis_status = ""
        if COL_ELVIS_STATUS in out.columns:
            elvis_status = str(row.get(COL_ELVIS_STATUS, ""))
        else:
            for c in out.columns:
                if _norm_header_name(c) == expected_elvis_norm:
                    elvis_status = str(row.get(c, ""))
                    break
        field_approved = bool(_RE_APPROVED.search(elvis_status)) if _RE_APPROVED else ("approved" in elvis_status.lower())
        info = debug_info.get(did_str, {})
        b2a = info.get("b2a")
        o2d = info.get("o2d")
        rs = info.get("route_surveyed", "")
        ba_short = info.get("ba_too_short", False)
        od_short = info.get("od_too_short", False)
        rs_miss = info.get("rs_missing", False)
        b_eq_a = info.get("boarding_equals_alighting", False)
        on_lat, on_lon = info.get("stop_on_lat"), info.get("stop_on_lon")
        off_lat, off_lon = info.get("stop_off_lat"), info.get("stop_off_lon")
        def _num(s: Any) -> str:
            if s is None or (isinstance(s, float) and np.isnan(s)):
                return "n/a"
            try:
                return "%.6f" % float(s)
            except (TypeError, ValueError):
                return "n/a"
        logger.info(
            "[DEBUG approval] ID %s: Final_Usage=%s, FINAL_REVIEWER=%s | "
            "in_pipeline(step03)=%s, field_approved(ElvisStatus)=%s | "
            "B2A=%s, O2D=%s, route_surveyed=%r | "
            "ba_too_short=%s, od_too_short=%s, rs_missing=%s, boarding_equals_alighting=%s | "
            "STOP_ON=(%s, %s), STOP_OFF=(%s, %s)",
            did_str,
            final_usage,
            final_reviewer,
            in_pipeline,
            field_approved,
            _num(b2a),
            _num(o2d),
            (rs[:50] if rs else ""),
            ba_short,
            od_short,
            rs_miss,
            b_eq_a,
            on_lat,
            on_lon,
            off_lat,
            off_lon,
        )
        if (b_eq_a or ba_short) and final_usage == "Use":
            logger.info(
                "[DEBUG approval] ID %s: SHOULD NOT BE APPROVED (boarding=alighting or B2A<0.25) but Final_Usage=Use; "
                "check (03) trip_distance_check or Field Approved override.",
                did_str,
            )


def _log_supervisor_only_debug_for_ids(
    clean_df: pd.DataFrame,
    phase_all_df: pd.DataFrame,
    step01_df: pd.DataFrame,
    step02_df: pd.DataFrame,
    step03_df: pd.DataFrame,
    debug_ids: List[str],
) -> None:
    """
    For each debug_id, log why it became Supervisor Only:
    Phase-All -> (01) -> (02) -> (03) membership.
    """
    if not debug_ids:
        return
    if REQUIRED_ID not in clean_df.columns:
        logger.warning("[DEBUG supervisor] No id column in clean_df; cannot debug.")
        return

    def _to_set(df: pd.DataFrame) -> set:
        if df is None or df.empty or REQUIRED_ID not in df.columns:
            return set()
        return set(df[REQUIRED_ID].astype(str))

    phase_all_ids = _to_set(phase_all_df)
    step01_ids = _to_set(step01_df)
    step02_ids = _to_set(step02_df)
    step03_ids = _to_set(step03_df)

    group_map = {}
    if phase_all_df is not None and not phase_all_df.empty and COL_GROUP in phase_all_df.columns and REQUIRED_ID in phase_all_df.columns:
        group_map = phase_all_df.set_index(phase_all_df[REQUIRED_ID].astype(str))[COL_GROUP].to_dict()

    prev_ser = _get_transfer_code_series(clean_df, "prev")
    nxt_ser = _get_transfer_code_series(clean_df, "next")
    prev_next_map = {}
    # Only compute for debug IDs to keep it cheap
    id_mask = clean_df[REQUIRED_ID].astype(str).isin([str(x).strip() for x in debug_ids])
    if id_mask.any():
        tmp = clean_df.loc[id_mask, [REQUIRED_ID]].copy()
        tmp[REQUIRED_ID] = tmp[REQUIRED_ID].astype(str)
        # align prev/nxt with clean_df index
        prev_tmp = prev_ser[id_mask]
        nxt_tmp = nxt_ser[id_mask]
        for idx in tmp.index:
            did = str(clean_df.loc[idx, REQUIRED_ID])
            prev_next_map[did] = (prev_tmp.loc[idx], nxt_tmp.loc[idx])

    for did in debug_ids:
        did_str = str(did).strip()
        in_phase = did_str in phase_all_ids
        in01 = did_str in step01_ids
        in02 = did_str in step02_ids
        in03 = did_str in step03_ids
        # Determine failure step
        fail_step = ""
        if in_phase and not in01:
            fail_step = "(01) OD [DISTANCE TRANSFERS CHECK]"
        elif in_phase and in01 and not in02:
            fail_step = "(02) OD [TRANSFERS CHECK]"
        elif in_phase and in01 and in02 and not in03:
            fail_step = "(03) OD [TRIP DISTANCE CHECK]"
        elif in_phase and in03:
            fail_step = "PASSED (should not be Supervisor Only)"
        else:
            fail_step = "NOT in Phase-All (unexpected for Supervisor Only)"

        grp = group_map.get(did_str, "")
        prev_val, nxt_val = prev_next_map.get(did_str, ("", ""))
        logger.info(
            "[DEBUG supervisor-only] ID %s: group=%r | PREV/NEXT=(%s,%s) | in Phase-All=%s | in (01)=%s | in (02)=%s | in (03)=%s | fail_step=%s",
            did_str,
            grp,
            prev_val,
            nxt_val,
            in_phase,
            in01,
            in02,
            in03,
            fail_step,
        )


def _fn_count_stops_on_route_within_distance2(
    stops_df: Optional[pd.DataFrame],
    lat: float,
    lon: float,
    threshold_miles: float,
    route_code: str,
) -> int:
    """
    PowerQuery fnCountStopsOnRouteWithinDistance2 parity (from od_approved_and_supervisor_only.txt).
    HalfThreshold = threshold/1.5, filter to route, bounding box, haversine <= HalfThreshold, return count.
    """
    if stops_df is None or stops_df.empty or not route_code or threshold_miles is None:
        return 0
    try:
        thr = float(threshold_miles)
        if not (thr > 0):
            return 0
    except (TypeError, ValueError):
        return 0

    half = thr / 1.5
    # Accept either stop_lat6/stop_lon6 or stop_lat/stop_lon from details/xfers sheets
    lat_col = "stop_lat6" if "stop_lat6" in stops_df.columns else ("stop_lat" if "stop_lat" in stops_df.columns else None)
    lon_col = "stop_lon6" if "stop_lon6" in stops_df.columns else ("stop_lon" if "stop_lon" in stops_df.columns else None)
    if lat_col is None or lon_col is None:
        return 0

    # Route filter
    route_col = "ETC_ROUTE_ID" if "ETC_ROUTE_ID" in stops_df.columns else ("XFER_ROUTE_ID" if "XFER_ROUTE_ID" in stops_df.columns else ("route_id" if "route_id" in stops_df.columns else None))
    if route_col is None:
        return 0
    rc = "" if route_code is None or (isinstance(route_code, float) and pd.isna(route_code)) else str(route_code).strip()
    if not rc:
        return 0
    # PBIX xfer_stop-list preprocessing:
    # Split ETC_ROUTE_ID on '_' and keep the first token, then compare to routeCode.
    stops_route = stops_df[route_col].astype(str).str.strip().str.split("_", n=1).str[0].str.strip()
    df = stops_df[stops_route == rc]
    if df.empty:
        return 0

    # Bounding box (threshold is miles; original uses degrees-ish; keep parity with existing implementation style)
    lat_min, lat_max = lat - half, lat + half
    lon_min, lon_max = lon - half, lon + half
    df = df.copy()
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    box = df[(df[lat_col] >= lat_min) & (df[lat_col] <= lat_max) & (df[lon_col] >= lon_min) & (df[lon_col] <= lon_max)]
    if box.empty:
        return 0

    # Haversine filter (PBIX fnHaversineMiles uses haversine, not acos)
    try:
        d = box.apply(
            lambda r: fn_haversine_miles(
                lat,
                lon,
                _safe_float(r.get(lat_col)),
                _safe_float(r.get(lon_col)),
            )
            or 999999.0,
            axis=1,
        )
        return int((d <= half).sum())
    except Exception:
        return 0


def _compute_supervisor_only_checks_df(
    approved_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    og_df: pd.DataFrame,
    stops_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Implements OD [Supervisor Only Checks] from od_approved_and_supervisor_only.txt.
    Returns a dataframe of approved ids with component flags and the combined Supervisor View Only flag.
    """
    if approved_df.empty:
        return pd.DataFrame(columns=[REQUIRED_ID, "O2DtoFULLDISTANCE", "TRANSFER_onroute_FLAG_refined", "Transfer_toOrigin_StopCounts_FLAG", COL_SUPERVISOR_VIEW_ONLY])

    # Merge approved ids with OG + CLEAN columns needed
    ids = approved_df[[REQUIRED_ID]].copy()
    merged = ids.merge(
        clean_df[
            [
                c
                for c in [
                    REQUIRED_ID,
                    COL_MERGED_ORIGIN_LAT,
                    COL_MERGED_ORIGIN_LONG,
                    COL_MERGED_DESTIN_LAT,
                    COL_MERGED_DESTIN_LONG,
                ]
                if c in clean_df.columns
            ]
        ],
        on=REQUIRED_ID,
        how="left",
    )

    # OD [Supervisor Only Checks] needs STOP/PREV/NEXT coordinate columns + trip route codes.
    # Coordinate column naming varies across exports (PBIX/ls2/ls6); resolve dynamically.
    coord_map = _resolve_transfer_coord_columns(og_df)
    coord_cols = [v for v in coord_map.values() if v and v in og_df.columns]
    # Route code naming varies across exports:
    # - PBIX style: TRIP_FIRST_ROUTE[Code]
    # - Observed in your mapped CSV: TRIP_FIRST_ROUTECode (no brackets)
    # So merge all trip route code columns we might need.
    route_cols = [
        c
        for c in og_df.columns
        if c.startswith("TRIP_") and "ROUTE" in c and "Code" in c and ("Other" not in c)
    ]
    og_cols = []
    for c in [REQUIRED_ID] + coord_cols + route_cols:
        if c not in og_cols:
            og_cols.append(c)

    merged = merged.merge(
        og_df[og_cols],
        on=REQUIRED_ID,
        how="left",
        suffixes=("", "_og"),
    )

    # ODDist: origin -> destination
    od = merged.apply(
        lambda r: fn_acos_distance_miles(
            _safe_float(r.get(COL_MERGED_ORIGIN_LAT)),
            _safe_float(r.get(COL_MERGED_ORIGIN_LONG)),
            _safe_float(r.get(COL_MERGED_DESTIN_LAT)),
            _safe_float(r.get(COL_MERGED_DESTIN_LONG)),
        )
        or np.nan,
        axis=1,
    )
    od = pd.to_numeric(od, errors="coerce")

    # TRANSFER_onroute_FLAG + FullTravelDistance (FROM M CODE)
    # M CODE: FullTravelDistance = sum(Transfer1..8_onroute_Distance ?? 0)
    # where each Transfer*_onroute_Distance is the within-leg off->on distance.
    C = _resolve_transfer_coord_columns(merged)

    def _dist_series(lat1: Optional[str], lon1: Optional[str], lat2: Optional[str], lon2: Optional[str]) -> pd.Series:
        if not all([lat1, lon1, lat2, lon2]):
            return pd.Series(np.nan, index=merged.index)
        return merged.apply(
            lambda r: fn_acos_distance_miles(_safe_float(r.get(lat1)), _safe_float(r.get(lon1)), _safe_float(r.get(lat2)), _safe_float(r.get(lon2))),
            axis=1,
        )

    def _has_leg(lat1: Optional[str], lon1: Optional[str], lat2: Optional[str], lon2: Optional[str]) -> pd.Series:
        if not all([lat1, lon1, lat2, lon2]):
            return pd.Series(False, index=merged.index)
        return merged[lat1].notna() & merged[lon1].notna() & merged[lat2].notna() & merged[lon2].notna()

    onroute_short = pd.Series(False, index=merged.index)
    transfer_onroute_dists: List[pd.Series] = []
    for i in range(1, 5):
        lat1, lon1 = C.get(f"PREV{i}_OFF_LAT"), C.get(f"PREV{i}_OFF_LON")
        lat2, lon2 = C.get(f"PREV{i}_ON_LAT"), C.get(f"PREV{i}_ON_LON")
        d = _dist_series(lat1, lon1, lat2, lon2)
        transfer_onroute_dists.append(d)
        onroute_short = onroute_short | ((d < 0.25) & _has_leg(lat1, lon1, lat2, lon2))
    for i in range(1, 5):
        lat1, lon1 = C.get(f"NEXT{i}_OFF_LAT"), C.get(f"NEXT{i}_OFF_LON")
        lat2, lon2 = C.get(f"NEXT{i}_ON_LAT"), C.get(f"NEXT{i}_ON_LON")
        d = _dist_series(lat1, lon1, lat2, lon2)
        transfer_onroute_dists.append(d)
        onroute_short = onroute_short | ((d < 0.25) & _has_leg(lat1, lon1, lat2, lon2))
    transfer_onroute_flag = onroute_short.astype(int)

    full_travel_distance = pd.Series(0.0, index=merged.index)
    for d in transfer_onroute_dists:
        full_travel_distance = full_travel_distance + d.fillna(0.0)

    # O2DtoFULLDISTANCE (FROM M CODE)
    allowed = 1 + (2 / od.replace(0, np.nan))
    ratio = full_travel_distance / od.replace(0, np.nan)
    o2d_full_flag = (ratio > allowed).fillna(False).astype(int)

    # Transfer_toOrigin_StopCounts_FLAG: any transfer route has >=25 stops within HalfThreshold
    # (origin_lat/origin_lon variables are not needed; distances are computed row-wise below)

    def _resolve_trip_route_code_col(df: pd.DataFrame, base: str) -> Optional[str]:
        # Try common variants first
        for cand in (
            f"{base}[Code]",
            f"{base}Code",
            f"{base}_Code_",
            f"{base}_Code",
        ):
            if cand in df.columns:
                return cand
        # Heuristic: any column containing base and 'Code' (but not 'Other')
        for c in df.columns:
            if base in str(c) and "Code" in str(c) and "Other" not in str(c):
                return c
        return None

    transfer_defs = []
    resolved_first = _resolve_trip_route_code_col(merged, "TRIP_FIRST_ROUTE")
    if resolved_first:
        transfer_defs.append((resolved_first, "PREV1_ON_LAT", "PREV1_ON_LON"))
    resolved_second = _resolve_trip_route_code_col(merged, "TRIP_SECOND_ROUTE")
    if resolved_second:
        transfer_defs.append((resolved_second, "PREV2_ON_LAT", "PREV2_ON_LON"))
    resolved_third = _resolve_trip_route_code_col(merged, "TRIP_THIRD_ROUTE")
    if resolved_third:
        transfer_defs.append((resolved_third, "PREV3_ON_LAT", "PREV3_ON_LON"))
    resolved_fourth = _resolve_trip_route_code_col(merged, "TRIP_FOURTH_ROUTE")
    if resolved_fourth:
        transfer_defs.append((resolved_fourth, "PREV4_ON_LAT", "PREV4_ON_LON"))
    resolved_next = _resolve_trip_route_code_col(merged, "TRIP_NEXT_ROUTE")
    if resolved_next:
        transfer_defs.append((resolved_next, "NEXT1_ON_LAT", "NEXT1_ON_LON"))
    resolved_after = _resolve_trip_route_code_col(merged, "TRIP_AFTER_ROUTE")
    if resolved_after:
        transfer_defs.append((resolved_after, "NEXT2_ON_LAT", "NEXT2_ON_LON"))
    resolved_3rd = _resolve_trip_route_code_col(merged, "TRIP_3RD_ROUTE")
    if resolved_3rd:
        transfer_defs.append((resolved_3rd, "NEXT3_ON_LAT", "NEXT3_ON_LON"))
    resolved_last4 = _resolve_trip_route_code_col(merged, "TRIP_LAST4TH_RTE")
    if resolved_last4:
        transfer_defs.append((resolved_last4, "NEXT4_ON_LAT", "NEXT4_ON_LON"))
    stopcounts_flag = pd.Series(0, index=merged.index, dtype=int)
    for route_col, lat_key, lon_key in transfer_defs:
        if route_col not in merged.columns:
            continue
        lat_c, lon_c = C.get(lat_key), C.get(lon_key)
        if not lat_c or not lon_c:
            continue
        # distance from origin to transfer board point
        dist_to_origin = merged.apply(
            lambda r: fn_acos_distance_miles(
                _safe_float(r.get(COL_MERGED_ORIGIN_LAT)),
                _safe_float(r.get(COL_MERGED_ORIGIN_LONG)),
                _safe_float(r.get(lat_c)),
                _safe_float(r.get(lon_c)),
            ),
            axis=1,
        )
        dist_to_origin = pd.to_numeric(dist_to_origin, errors="coerce")

        # counts (row-wise; stops_df size usually manageable)
        counts = []
        for idx, r in merged.iterrows():
            rc = r.get(route_col)
            if rc is None or (isinstance(rc, float) and pd.isna(rc)):
                counts.append(0)
                continue
            olat = _safe_float(r.get(COL_MERGED_ORIGIN_LAT))
            olon = _safe_float(r.get(COL_MERGED_ORIGIN_LONG))
            thr = dist_to_origin.loc[idx]
            if olat is None or olon is None or pd.isna(thr):
                counts.append(0)
                continue
            counts.append(_fn_count_stops_on_route_within_distance2(stops_df, olat, olon, float(thr), str(rc)))
        counts_s = pd.Series(counts, index=merged.index)
        stopcounts_flag = stopcounts_flag | (counts_s.fillna(0) >= 25).astype(int)

    supervisor_view_only = ((o2d_full_flag + transfer_onroute_flag + stopcounts_flag) > 0).astype(int)
    out = pd.DataFrame({
        REQUIRED_ID: merged[REQUIRED_ID].astype(str),
        "O2DtoFULLDISTANCE": o2d_full_flag.astype(int),
        "TRANSFER_onroute_FLAG_refined": transfer_onroute_flag.astype(int),
        "Transfer_toOrigin_StopCounts_FLAG": stopcounts_flag.astype(int),
        COL_SUPERVISOR_VIEW_ONLY: supervisor_view_only.map({1: "Yes", 0: ""}),
    })
    out = out.dropna(subset=[REQUIRED_ID]).drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)
    return out


def _compute_supervisor_view_only_ids(
    approved_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    og_df: pd.DataFrame,
    stops_df: Optional[pd.DataFrame] = None,
) -> pd.Series:
    """Convenience: return ids where Supervisor View Only = Yes (subset of approved ids)."""
    checks = _compute_supervisor_only_checks_df(approved_df=approved_df, clean_df=clean_df, og_df=og_df, stops_df=stops_df)
    if checks.empty or COL_SUPERVISOR_VIEW_ONLY not in checks.columns:
        return pd.Series([], dtype=object)
    return checks.loc[checks[COL_SUPERVISOR_VIEW_ONLY] == "Yes", REQUIRED_ID].astype(str).drop_duplicates()


def final_auto_approval(
    step03_df: pd.DataFrame,
    supervisor_only_ids: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    ___auto-approved: FROM M CODE: Select id, GROUP from __(03), left join OD [Supervisor Only Checks], add Supervisor View Only.
    """
    result = step03_df[[REQUIRED_ID, COL_GROUP]].copy()
    if supervisor_only_ids is not None and len(supervisor_only_ids) > 0:
        result[COL_SUPERVISOR_VIEW_ONLY] = result[REQUIRED_ID].isin(supervisor_only_ids).map({True: "Yes", False: ""})
    else:
        result[COL_SUPERVISOR_VIEW_ONLY] = ""
    result = result.drop_duplicates(subset=[REQUIRED_ID]).reset_index(drop=True)
    logger.info("___auto-approved: %d ids", len(result))
    return result


def _is_home_place_type(series: pd.Series) -> pd.Series:
    """True where value indicates Home (same logic as auto_and_suggestions_more_transfers is_home)."""
    if series is None or series.empty:
        return pd.Series(False, index=series.index if hasattr(series, 'index') else [])
    s = series.fillna("").astype(str).str.strip().str.lower()
    return (
        (s == "home")
        | s.str.startswith("home ", na=False)
        | s.str.endswith(" home", na=False)
        | (s == "home address")
        | s.str.contains("home address", regex=False, na=False)
    )


def _remove_mask_origin_dest_home(df: pd.DataFrame) -> pd.Series:
    """Vectorized: True where Origin = Destination = Home (Remove reason). Uses place type columns (same as M code)."""
    o_col = _first_column(df, COL_ORIGIN_PLACE_TYPE, "OriginPlaceTypeCode", "OriginPlaceType", default_index=df.index)
    d_col = _first_column(df, COL_DESTIN_PLACE_TYPE, "DestinPlaceTypeCode", "DestinPlaceType", default_index=df.index)
    if o_col is None or d_col is None:
        return pd.Series(False, index=df.index)
    origin_is_home = _is_home_place_type(o_col)
    destin_is_home = _is_home_place_type(d_col)
    return origin_is_home & destin_is_home


def _resolve_origin_destin_addr_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """Resolve origin_addr and destin_addr column names (same as auto_and_suggestions_more_transfers detect_columns)."""
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    origin_addr = cols_lower.get("origin_address_addr") or next(
        (c for c in df.columns if "origin" in str(c).lower() and "address" in str(c).lower() and "addr" in str(c).lower() and "lat" not in str(c).lower() and "lon" not in str(c).lower() and "long" not in str(c).lower()),
        None,
    )
    if not origin_addr:
        origin_addr = next((c for c in df.columns if "origin" in str(c).lower() and "address" in str(c).lower() and "lat" not in str(c).lower() and "lon" not in str(c).lower() and "long" not in str(c).lower()), None)
    destin_addr = cols_lower.get("destin_address_addr") or next(
        (c for c in df.columns if "destin" in str(c).lower() and "address" in str(c).lower() and "addr" in str(c).lower() and "lat" not in str(c).lower() and "lon" not in str(c).lower() and "long" not in str(c).lower()),
        None,
    )
    if not destin_addr:
        destin_addr = next((c for c in df.columns if "destin" in str(c).lower() and "address" in str(c).lower() and "lat" not in str(c).lower() and "lon" not in str(c).lower() and "long" not in str(c).lower()), None)
    return origin_addr, destin_addr


def _remove_mask_origin_dest_home_address(df: pd.DataFrame) -> pd.Series:
    """Vectorized: True where Origin = Destination = Home by address text (same as auto_and_suggestions_more_transfers check_conditions_for_remove_vectorized)."""
    origin_col, destin_col = _resolve_origin_destin_addr_columns(df)
    if not origin_col or not destin_col or origin_col not in df.columns or destin_col not in df.columns:
        return pd.Series(False, index=df.index)
    o = df[origin_col].fillna("").astype(str).str.strip().str.lower()
    d = df[destin_col].fillna("").astype(str).str.strip().str.lower()
    origin_is_home = (o == "home") | o.str.startswith("home ", na=False) | o.str.endswith(" home", na=False) | (o == "home address") | o.str.contains("home address", regex=False, na=False)
    destin_is_home = (d == "home") | d.str.startswith("home ", na=False) | d.str.endswith(" home", na=False) | (d == "home address") | d.str.contains("home address", regex=False, na=False)
    return origin_is_home & destin_is_home


# Same as auto_and_suggestions_more_transfers.py (for transfer suggestion)
MAX_TRANSFERS_CHAIN = 4
WALK_THRESHOLD_MILES = 1.5
SUGGEST_MIN_GAP_MILES = 0.25
BETTER_ALTERNATIVE_WALK_PCT = 0.20


def clean_coordinate_value(val):
    """
    Clean coordinate values: strip non-numeric (except . and -), handle European decimals,
    convert to float. Returns None for invalid values.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)) and not pd.isna(val):
        try:
            v = float(val)
            if abs(v) < 1e10:
                return v
        except (TypeError, ValueError):
            pass
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null", ""):
        return None
    s = re.sub(r"[^\d.\-eE]", "", s)
    s = s.replace(",", ".")
    if not s or s in ("-", "."):
        return None
    try:
        v = float(s)
        if pd.isna(v) or abs(v) >= 1e10:
            return None
        return v
    except (ValueError, TypeError):
        return None


def clean_coordinate_series(ser):
    """
    Vectorized coordinate cleaning: apply same logic as clean_coordinate_value to an entire Series.
    Returns a float Series with NaN for invalid values (for fast lookup in coords_df).
    """
    if ser is None or len(ser) == 0:
        return ser
    s = ser.astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)
    s = s.str.replace(r"[^\d.\-eE]", "", regex=True)
    s = s.replace("", np.nan).replace("-", np.nan).replace(".", np.nan)
    s = s.mask(s.str.lower().isin(["nan", "none", "null"]), np.nan)
    out = pd.to_numeric(s, errors="coerce")
    out = out.where(out.abs() < 1e10)
    return out


def clean_numeric_value(val, default=0):
    """
    Clean numeric values: handle empty/NULL/None, extract first number from mixed text.
    Returns default for missing/invalid.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    if isinstance(val, (int, float)) and not pd.isna(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            pass
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null", ""):
        return default
    match = re.search(r"-?\d+\.?\d*", s.replace(",", "."))
    if match:
        try:
            return float(match.group(0))
        except (ValueError, TypeError):
            pass
    try:
        return float(s.replace(",", "."))
    except (ValueError, TypeError):
        return default


def _get_details_lat_lon(details_row):
    """Get (lat, lon) from a single row of details_df. Tries common column names (LIME/airport style)."""
    if details_row is None or not hasattr(details_row, "index"):
        return None, None
    candidates = [
        ("LAT6", "LON6"), ("stop_lat6", "stop_lon6"), ("lat6", "lng6"),
        ("lat6", "lon6"), ("stop_lat", "stop_lon"), ("LAT", "LON"),
        ("Latitude", "Longitude"), ("latitude", "longitude"),
    ]
    for lat_name, lon_name in candidates:
        if lat_name in details_row.index and lon_name in details_row.index:
            try:
                lat_val = details_row[lat_name]
                lon_val = details_row[lon_name]
                if pd.notna(lat_val) and pd.notna(lon_val):
                    la, lo = float(lat_val), float(lon_val)
                    if abs(la) <= 90 and abs(lo) <= 180:
                        return la, lo
            except (TypeError, ValueError, KeyError):
                continue
    return None, None


def _parse_transfer_count_from_text(val):
    """Parse transfer count from text like '(1) One', '(2) Two', '(0) None'. Returns int or None if unparseable."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if not s or s in ("nan", "none", "null"):
        return None
    if s in ("yes", "1", "true", "y"):
        return 1
    match = re.search(r"\((\d+)\)", s)
    if match:
        return int(match.group(1))
    n = int(clean_numeric_value(val, -1))
    return n if n >= 0 else None


def _get_existing_transfer_count(row, prev_or_next, col_map):
    """Get existing PREV or NEXT transfer leg count from row (data only). Returns int (0 if unknown)."""
    if prev_or_next == "PREV":
        count_col = col_map.get("prev_transfers_count")
        text_col = col_map.get("prev_transfers")
    else:
        count_col = col_map.get("next_transfers_count")
        text_col = col_map.get("next_transfers")
    count = 0
    if count_col and count_col in row.index:
        count = int(clean_numeric_value(row.get(count_col), 0))
    if count == 0 and text_col and text_col in row.index:
        v = row.get(text_col)
        parsed = _parse_transfer_count_from_text(v)
        if parsed is not None:
            count = parsed
    return max(0, min(count, 2))


def _get_existing_transfer_walk_miles(row, prev_or_next, col_map):
    """
    Get existing total walk distance for PREV or NEXT transfers if available from row.
    Returns float or None if not available.
    """
    try:
        if prev_or_next == "PREV":
            ob = col_map.get("transfer_on_dist") or next((c for c in row.index if "val_dist" in c.lower() and "ob" in c.lower()), None)
            ba = col_map.get("transfer_off_dist") or next((c for c in row.index if "val_dist" in c.lower() and "ba" in c.lower()), None)
            cols = [ob, ba]
        else:
            ad = col_map.get("transfer_on_dist") or next((c for c in row.index if "val_dist" in c.lower() and "ad" in c.lower()), None)
            cols = [ad]
        total = 0.0
        for c in cols:
            if c and c in row.index:
                v = clean_numeric_value(row.get(c), None)
                if v is not None:
                    total += v
        return total if total > 0 else None
    except (TypeError, ValueError, KeyError):
        return None


def _is_better_suggestion(suggested_legs, suggested_total_walk, existing_legs, existing_walk):
    """
    True if suggested route is better: fewer legs, or same legs and >= 20% less walk.
    """
    if suggested_legs < existing_legs:
        return True
    if suggested_legs == existing_legs and existing_walk is not None and existing_walk > 0:
        if suggested_total_walk is not None and suggested_total_walk <= existing_walk * (1 - BETTER_ALTERNATIVE_WALK_PCT):
            return True
    return False


def _get_suggested_total_walk(suggestions_list):
    """Sum total_walk_miles across suggestion legs."""
    total = 0.0
    for s in (suggestions_list or []):
        w = s.get("total_walk_miles")
        if w is not None:
            try:
                total += float(w)
            except (TypeError, ValueError):
                pass
    return total if total > 0 else None


def _get_end_stop_coords(best, stops_df, use_end=True):
    """
    Get (lat, lon) of the end stop of a suggested transfer leg for chaining.
    Prefers best['stop_near_end_lat'/'stop_near_end_lon'] if present; otherwise
    resolves stop_near_end_id (or stop_near_start_id if use_end=False) from stops_df.
    Returns (lat, lon) or (None, None) if not resolvable.
    """
    if use_end:
        lat = best.get("stop_near_end_lat")
        lon = best.get("stop_near_end_lon")
        stop_id = best.get("stop_near_end_id")
    else:
        lat = best.get("stop_near_start_lat")
        lon = best.get("stop_near_start_lon")
        stop_id = best.get("stop_near_start_id")
    try:
        if lat is not None and lon is not None and pd.notna(lat) and pd.notna(lon):
            return float(lat), float(lon)
    except (TypeError, ValueError):
        pass
    if stops_df is None or stop_id is None or pd.isna(stop_id):
        return None, None
    id_col = None
    for c in ["stop_id", "ETC_STOP_ID", "stop_id_clean"]:
        if c in stops_df.columns:
            id_col = c
            break
    if not id_col:
        norm = {str(c).lower().replace("_", ""): c for c in stops_df.columns}
        id_col = norm.get("stopid") or norm.get("etcstopid")
    lat_col = None
    lon_col = None
    for c in stops_df.columns:
        cl = c.lower()
        if "lat" in cl and ("stop" in cl or c == "stop_lat"):
            lat_col = c
        if ("lon" in cl or "long" in cl) and ("stop" in cl or c in ("stop_lon", "stop_long")):
            lon_col = c
    if not lat_col:
        lat_col = "stop_lat" if "stop_lat" in stops_df.columns else None
    if not lon_col:
        lon_col = "stop_lon" if "stop_lon" in stops_df.columns else next((c for c in stops_df.columns if "lon" in c.lower() or "long" in c.lower()), None)
    if not id_col or not lat_col or not lon_col or lat_col not in stops_df.columns or lon_col not in stops_df.columns:
        return None, None
    stop_id_str = str(stop_id).strip()
    match = stops_df[stops_df[id_col].astype(str).str.strip() == stop_id_str]
    if len(match) == 0:
        match = stops_df[stops_df[id_col].astype(str) == stop_id_str]
    if len(match) > 0:
        r = match.iloc[0]
        try:
            la, lo = float(r[lat_col]), float(r[lon_col])
            if pd.notna(la) and pd.notna(lo) and abs(la) < 90 and abs(lo) < 180:
                return la, lo
        except (TypeError, ValueError, KeyError):
            pass
    return None, None


def _run_transfer_chain(stops_df, current_start, current_end, transfer_type, rid, surveyed_route,
                        suggest_haversine_miles_fn, chain_debug_counts=None):
    """
    Run iterative transfer chaining: find best route from current_start to current_end,
    emit one row per leg, then move current_start to end stop and repeat until
    total_walk < WALK_THRESHOLD_MILES or MAX_TRANSFERS_CHAIN (4) reached. Prevents repeated stops.
    Returns list of suggestion dicts (one per leg).
    If chain_debug_counts is provided (dict), increments skipped_chaining_* keys when breaking for those reasons.
    """
    suggestions = []
    visited_stop_ids = set()
    last_end_stop_id = None
    transfer_num = 1
    start_lat, start_lon = current_start[0], current_start[1]
    end_lat, end_lon = current_end[0], current_end[1]

    while transfer_num <= MAX_TRANSFERS_CHAIN:
        remaining_direct = suggest_haversine_miles_fn(start_lat, start_lon, end_lat, end_lon)
        if remaining_direct < WALK_THRESHOLD_MILES:
            if chain_debug_counts is not None:
                chain_debug_counts["skipped_chaining_already_close"] = chain_debug_counts.get("skipped_chaining_already_close", 0) + 1
            break
        best = find_most_suitable_route(stops_df, start_lat, start_lon, end_lat, end_lon)
        if not best:
            break
        gap_miles = suggest_haversine_miles_fn(start_lat, start_lon, end_lat, end_lon)
        end_stop_id = best.get("stop_near_end_id")
        start_stop_id = best.get("stop_near_start_id")
        end_sid = str(end_stop_id) if end_stop_id is not None else ""
        start_sid = str(start_stop_id) if start_stop_id is not None else ""
        if end_sid and end_sid in visited_stop_ids:
            if chain_debug_counts is not None:
                chain_debug_counts["skipped_chaining_loop"] = chain_debug_counts.get("skipped_chaining_loop", 0) + 1
            break
        if start_sid and start_sid in visited_stop_ids and (last_end_stop_id is None or start_sid != str(last_end_stop_id)):
            if chain_debug_counts is not None:
                chain_debug_counts["skipped_chaining_loop"] = chain_debug_counts.get("skipped_chaining_loop", 0) + 1
            break
        visited_stop_ids.add(start_sid)
        visited_stop_ids.add(end_sid)
        last_end_stop_id = end_stop_id

        suggested_route_id = str(best["route_id"]).strip()
        suggested_route_name = str(best["route_name"]).strip()
        route_matches = False
        if surveyed_route:
            surveyed_lower = surveyed_route.lower()
            route_id_lower = suggested_route_id.lower()
            route_name_lower = suggested_route_name.lower()
            route_matches = (
                route_id_lower in surveyed_lower or route_name_lower in surveyed_lower
                or surveyed_lower in route_id_lower or surveyed_lower in route_name_lower
            )
        else:
            route_matches = True

        gap_miles_rounded = round(gap_miles, 2)
        row = {
            "id": rid,
            "transfer_type": transfer_type,
            "transfer_number": transfer_num,
            "gap_miles": gap_miles_rounded,
            "suggested_route_id": suggested_route_id,
            "suggested_route_name": suggested_route_name,
            "total_walk_miles": best["total_walk_miles"],
            "route_matches_surveyed": route_matches,
        }
        if transfer_type == "PREV":
            row["stop_near_origin_lat"] = best.get("stop_near_start_id")
            row["stop_near_boarding_lat"] = best.get("stop_near_end_id")
        else:
            row["stop_near_alighting_lat"] = best.get("stop_near_start_id")
            row["stop_near_dest_lat"] = best.get("stop_near_end_id")
        suggestions.append(row)

        next_lat, next_lon = _get_end_stop_coords(best, stops_df, use_end=True)
        if next_lat is None or next_lon is None:
            if chain_debug_counts is not None:
                chain_debug_counts["skipped_chaining_no_progress"] = chain_debug_counts.get("skipped_chaining_no_progress", 0) + 1
            break
        remaining_walk = suggest_haversine_miles_fn(next_lat, next_lon, end_lat, end_lon)
        if remaining_walk < WALK_THRESHOLD_MILES:
            if chain_debug_counts is not None:
                chain_debug_counts["skipped_chaining_already_close"] = chain_debug_counts.get("skipped_chaining_already_close", 0) + 1
            break
        if remaining_walk >= remaining_direct - 1e-6:
            if chain_debug_counts is not None:
                chain_debug_counts["skipped_chaining_no_progress"] = chain_debug_counts.get("skipped_chaining_no_progress", 0) + 1
            break
        if best["total_walk_miles"] < WALK_THRESHOLD_MILES:
            if chain_debug_counts is not None:
                chain_debug_counts["skipped_chaining_already_close"] = chain_debug_counts.get("skipped_chaining_already_close", 0) + 1
            break
        start_lat, start_lon = next_lat, next_lon
        transfer_num += 1

    return suggestions


def is_missing(value):
    """Check if a value is missing (NaN, None, empty string)"""
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


_DETECT_COLUMNS_CACHE = {}


def detect_columns(df_columns):
    """
    Detect all required columns once - much more efficient than searching for each row.
    Results are cached by tuple of column names so repeated calls with same schema are O(1).
    Returns a dictionary of column names.
    """
    key = tuple(sorted(str(c) for c in df_columns))
    if key in _DETECT_COLUMNS_CACHE:
        return _DETECT_COLUMNS_CACHE[key].copy()
    cols = {}

    cols["origin_addr"] = next((c for c in df_columns if c.lower() == "origin_address_addr" or ("origin" in c.lower() and "address" in c.lower() and "addr" in c.lower() and "lat" not in c.lower() and "lon" not in c.lower() and "long" not in c.lower())), None)
    if not cols["origin_addr"]:
        cols["origin_addr"] = next((c for c in df_columns if "origin" in c.lower() and "address" in c.lower() and "lat" not in c.lower() and "lon" not in c.lower() and "long" not in c.lower()), None)

    cols["destin_addr"] = next((c for c in df_columns if c.lower() == "destin_address_addr" or ("destin" in c.lower() and "address" in c.lower() and "addr" in c.lower() and "lat" not in c.lower() and "lon" not in c.lower() and "long" not in c.lower())), None)
    if not cols["destin_addr"]:
        cols["destin_addr"] = next((c for c in df_columns if "destin" in c.lower() and "address" in c.lower() and "lat" not in c.lower() and "lon" not in c.lower() and "long" not in c.lower()), None)

    cols["origin_lat"] = next((c for c in df_columns if "origin" in c.lower() and "address" in c.lower() and "lat" in c.lower()), None)
    if not cols["origin_lat"]:
        cols["origin_lat"] = next((c for c in df_columns if "origin" in c.lower() and "lat" in c.lower()), None)

    cols["origin_lon"] = next((c for c in df_columns if c.lower() == "origin_address_long" or c.lower() == "origin_address_lon" or ("origin" in c.lower() and "address" in c.lower() and ("lon" in c.lower() or "long" in c.lower()))), None)
    if not cols["origin_lon"]:
        cols["origin_lon"] = next((c for c in df_columns if "origin" in c.lower() and ("lon" in c.lower() or "long" in c.lower()) and "address" not in c.lower()), None)

    cols["destin_lat"] = next((c for c in df_columns if "destin" in c.lower() and "address" in c.lower() and "lat" in c.lower()), None)
    if not cols["destin_lat"]:
        cols["destin_lat"] = next((c for c in df_columns if "destin" in c.lower() and "lat" in c.lower()), None)

    cols["destin_lon"] = next((c for c in df_columns if c.lower() == "destin_address_long" or c.lower() == "destin_address_lon" or ("destin" in c.lower() and "address" in c.lower() and ("lon" in c.lower() or "long" in c.lower()))), None)
    if not cols["destin_lon"]:
        cols["destin_lon"] = next((c for c in df_columns if "destin" in c.lower() and ("lon" in c.lower() or "long" in c.lower()) and "address" not in c.lower()), None)

    cols["boarding_lat"] = next((c for c in df_columns if "stop" in c.lower() and "on" in c.lower() and "lat" in c.lower()), None)
    if not cols["boarding_lat"]:
        cols["boarding_lat"] = next((c for c in df_columns if "boarding" in c.lower() and "lat" in c.lower()), None)

    cols["boarding_lon"] = next((c for c in df_columns if c.lower() == "stop_on_long" or c.lower() == "stop_on_lon" or ("stop" in c.lower() and "on" in c.lower() and ("lon" in c.lower() or "long" in c.lower()))), None)
    if not cols["boarding_lon"]:
        cols["boarding_lon"] = next((c for c in df_columns if "boarding" in c.lower() and ("lon" in c.lower() or "long" in c.lower())), None)

    cols["alighting_lat"] = next((c for c in df_columns if "stop" in c.lower() and "off" in c.lower() and "lat" in c.lower()), None)
    if not cols["alighting_lat"]:
        cols["alighting_lat"] = next((c for c in df_columns if "alighting" in c.lower() and "lat" in c.lower()), None)

    cols["alighting_lon"] = next((c for c in df_columns if c.lower() == "stop_off_long" or c.lower() == "stop_off_lon" or ("stop" in c.lower() and "off" in c.lower() and ("lon" in c.lower() or "long" in c.lower()))), None)
    if not cols["alighting_lon"]:
        cols["alighting_lon"] = next((c for c in df_columns if "alighting" in c.lower() and ("lon" in c.lower() or "long" in c.lower())), None)

    cols["origin_transport"] = next((c for c in df_columns if "origin" in c.lower() and "transport" in c.lower() and "code" not in c.lower()), None)
    if not cols["origin_transport"]:
        cols["origin_transport"] = next((c for c in df_columns if "origin" in c.lower() and "transport" in c.lower()), None)

    cols["destin_transport"] = next((c for c in df_columns if "destin" in c.lower() and "transport" in c.lower() and "code" not in c.lower()), None)
    if not cols["destin_transport"]:
        cols["destin_transport"] = next((c for c in df_columns if "destin" in c.lower() and "transport" in c.lower()), None)

    cols["surveyed_route"] = next((c for c in df_columns if "surveyed" in c.lower() and "route" in c.lower() and "code" not in c.lower()), None)
    if not cols["surveyed_route"]:
        cols["surveyed_route"] = next((c for c in df_columns if "surveyed" in c.lower() and "route" in c.lower()), None)

    cols["transfer_count"] = next((c for c in df_columns if "val_count" in c.lower() and ("prevtrans" in c.lower() or "nexttrans" in c.lower())), None)
    if not cols["transfer_count"]:
        cols["transfer_count"] = next((c for c in df_columns if ("prev_transfers" in c.lower() or "next_transfers" in c.lower()) and "code" not in c.lower()), None)

    cols["transfer_match"] = next((c for c in df_columns if "transfer" in c.lower() and "match" in c.lower()), None)
    if not cols["transfer_match"]:
        cols["transfer_match"] = next((c for c in df_columns if "route" in c.lower() and "match" in c.lower()), None)

    cols["transfer_on_dist"] = next((c for c in df_columns if "val_dist" in c.lower() and ("ob" in c.lower() or "ad" in c.lower())), None)
    if not cols["transfer_on_dist"]:
        cols["transfer_on_dist"] = next((c for c in df_columns if "transfer" in c.lower() and "on" in c.lower() and "dist" in c.lower() and "0.25" not in c.lower()), None)

    cols["transfer_off_dist"] = next((c for c in df_columns if "val_dist" in c.lower() and ("ba" in c.lower() or "ad" in c.lower())), None)
    if not cols["transfer_off_dist"]:
        cols["transfer_off_dist"] = next((c for c in df_columns if "transfer" in c.lower() and "off" in c.lower() and "dist" in c.lower() and "0.25" not in c.lower()), None)

    cols["prev_transfers"] = next((c for c in df_columns if "prev_transfers" in c.lower() and "code" not in c.lower()), None)
    cols["next_transfers"] = next((c for c in df_columns if "next_transfers" in c.lower() and "code" not in c.lower()), None)
    cols["prev_transfers_count"] = next((c for c in df_columns if "val_count_prevtrans" in c.lower()), None)
    if not cols["prev_transfers_count"]:
        cols["prev_transfers_count"] = next((c for c in df_columns if "val" in c.lower() and "count" in c.lower() and "prevtrans" in c.lower()), None)
    cols["next_transfers_count"] = next((c for c in df_columns if "val_count_nexttrans" in c.lower()), None)
    if not cols["next_transfers_count"]:
        cols["next_transfers_count"] = next((c for c in df_columns if "val" in c.lower() and "count" in c.lower() and "nexttrans" in c.lower()), None)

    cols["home_lat"] = next((c for c in df_columns if "home" in c.lower() and "address" in c.lower() and "lat" in c.lower()), None)
    if not cols["home_lat"]:
        cols["home_lat"] = next((c for c in df_columns if "home" in c.lower() and "lat" in c.lower()), None)
    cols["home_lon"] = next((c for c in df_columns if "home" in c.lower() and "address" in c.lower() and ("lon" in c.lower() or "long" in c.lower())), None)
    if not cols["home_lon"]:
        cols["home_lon"] = next((c for c in df_columns if "home" in c.lower() and ("lon" in c.lower() or "long" in c.lower())), None)
    cols["origin_place_type"] = next((c for c in df_columns if "origin" in c.lower() and "place" in c.lower() and "type" in c.lower()), None)
    if not cols["origin_place_type"]:
        cols["origin_place_type"] = next((c for c in df_columns if str(c).strip().upper() == "ORIGIN_PLACE_TYPE"), None)
    cols["destin_place_type"] = next((c for c in df_columns if "destin" in c.lower() and "place" in c.lower() and "type" in c.lower()), None)
    if not cols["destin_place_type"]:
        cols["destin_place_type"] = next((c for c in df_columns if str(c).strip().upper() == "DESTIN_PLACE_TYPE"), None)
    cols["airport_code"] = next((c for c in df_columns if "destin" in c.lower() and "airport" in c.lower() and "code" in c.lower()), None)
    if not cols["airport_code"]:
        cols["airport_code"] = next((c for c in df_columns if "airport" in c.lower() and "code" in c.lower()), None)

    _DETECT_COLUMNS_CACHE[key] = cols.copy()
    return cols


def resolve_elvis_cols(df):
    if suggest_resolve_elvis_columns is None:
        return {}
    try:
        return suggest_resolve_elvis_columns(df)
    except Exception:
        return {}


def _run_transfer_suggestions_for_records(
    full_record: pd.DataFrame, stops_path: str
) -> Optional[pd.DataFrame]:
    if not TRANSFER_SUGGESTION_AVAILABLE or full_record.empty or not stops_path or not os.path.isfile(stops_path):
        logger.info(
            "Transfer suggestions skipped: available=%s, full_record_empty=%s, stops_path=%s, file_exists=%s",
            TRANSFER_SUGGESTION_AVAILABLE,
            full_record.empty,
            stops_path,
            os.path.isfile(stops_path) if stops_path else False,
        )
        return None
    if suggest_haversine_miles is None:
        logger.warning("Transfer suggestions skipped: suggest_haversine_miles is unavailable")
        return None

    final_test = full_record
    df1 = full_record

    try:
        logger.info("Generating suggested transfer routes...")
        if str(stops_path).lower().endswith((".xlsx", ".xls")):
            stops_df = load_stops_df(details_path=stops_path)
            logger.info("Loading stops from details workbook using combined STOPS + XFER_STOPS sheets")
        else:
            stops_df = load_stops_df(xfers_path=stops_path)
            logger.info("Loading stops from stops file: %s", stops_path)
        if stops_df is None or stops_df.empty:
            logger.warning("Transfer suggestions skipped: loaded stops dataframe is empty")
            return None
        logger.info("Loaded %d stops for transfer suggestions", len(stops_df))
    except Exception:
        logger.exception("Transfer suggestions skipped: could not load stops dataframe")
        return None

    _skip_transfer_suggestions = (
        os.environ.get("ELVIS_FAST_RUN", "").strip() == "1"
        or os.environ.get("ELVIS_SKIP_TRANSFER_SUGGESTIONS", "").strip() == "1"
    )
    if _skip_transfer_suggestions:
        logger.info("Skipping transfer suggestions due to ELVIS_FAST_RUN or ELVIS_SKIP_TRANSFER_SUGGESTIONS")
        return None

    try:
        col_map_early = detect_columns(final_test.columns)
        col_map_df1_early = detect_columns(df1.columns)
        for key in col_map_early:
            if not col_map_early.get(key) and col_map_df1_early.get(key):
                col_map_early[key] = col_map_df1_early[key]

        elvis_cols = resolve_elvis_cols(final_test)
        o_lat = elvis_cols.get("ORIGIN_ADDRESS_LAT") or col_map_early.get("origin_lat")
        o_lon = elvis_cols.get("ORIGIN_ADDRESS_LONG") or col_map_early.get("origin_lon")
        b_lat = elvis_cols.get("STOP_ON_LAT") or col_map_early.get("boarding_lat")
        b_lon = elvis_cols.get("STOP_ON_LONG") or col_map_early.get("boarding_lon")
        a_lat = elvis_cols.get("STOP_OFF_LAT") or col_map_early.get("alighting_lat")
        a_lon = elvis_cols.get("STOP_OFF_LONG") or col_map_early.get("alighting_lon")
        d_lat = elvis_cols.get("DESTIN_ADDRESS_LAT") or col_map_early.get("destin_lat")
        d_lon = elvis_cols.get("DESTIN_ADDRESS_LONG") or col_map_early.get("destin_lon")
        o_tr = elvis_cols.get("ORIGIN_TRANSPORT") or col_map_early.get("origin_transport")
        d_tr = elvis_cols.get("DESTIN_TRANSPORT") or col_map_early.get("destin_transport")

        prev_transfers_col = col_map_early.get("prev_transfers")
        prev_transfers_count_col = col_map_early.get("prev_transfers_count")
        next_transfers_col = col_map_early.get("next_transfers")
        next_transfers_count_col = col_map_early.get("next_transfers_count")
        surveyed_route_col = col_map_early.get("surveyed_route")

        coord_cols = [c for c in [o_lat, o_lon, b_lat, b_lon, a_lat, a_lon, d_lat, d_lon] if c and c in final_test.columns]
        home_lat_col = col_map_early.get("home_lat")
        home_lon_col = col_map_early.get("home_lon")
        for h in [home_lat_col, home_lon_col]:
            if h and h not in coord_cols and h in final_test.columns:
                coord_cols.append(h)
        if coord_cols:
            _coords_df = final_test[coord_cols].copy()
            for c in coord_cols:
                _coords_df[c] = clean_coordinate_series(_coords_df[c])
        else:
            _coords_df = None

        has_prev = pd.Series(False, index=final_test.index)
        if prev_transfers_count_col and prev_transfers_count_col in final_test.columns:
            v = pd.to_numeric(final_test[prev_transfers_count_col], errors="coerce").fillna(0)
            has_prev = has_prev | (v > 0)
        if prev_transfers_col and prev_transfers_col in final_test.columns:
            s = final_test[prev_transfers_col].astype(str).str.strip().str.lower()
            has_prev = has_prev | s.isin(["yes", "1", "true", "y"]) | (s.str.isdigit() & (pd.to_numeric(s, errors="coerce") > 0))

        has_next = pd.Series(False, index=final_test.index)
        if next_transfers_count_col and next_transfers_count_col in final_test.columns:
            v = pd.to_numeric(final_test[next_transfers_count_col], errors="coerce").fillna(0)
            has_next = has_next | (v > 0)
        if next_transfers_col and next_transfers_col in final_test.columns:
            s = final_test[next_transfers_col].astype(str).str.strip().str.lower()
            has_next = has_next | s.isin(["yes", "1", "true", "y"]) | (s.str.isdigit() & (pd.to_numeric(s, errors="coerce") > 0))

        origin_addr_col = col_map_early.get("origin_addr")
        destin_addr_col = col_map_early.get("destin_addr")
        skip_remove = pd.Series(False, index=final_test.index)
        if origin_addr_col and destin_addr_col and origin_addr_col in final_test.columns and destin_addr_col in final_test.columns:
            o = final_test[origin_addr_col].fillna("").astype(str).str.strip().str.lower()
            d = final_test[destin_addr_col].fillna("").astype(str).str.strip().str.lower()
            origin_is_home = (o == "home") | o.str.startswith("home ", na=False) | o.str.endswith(" home", na=False) | (o == "home address") | o.str.contains("home address", regex=False, na=False)
            destin_is_home = (d == "home") | d.str.startswith("home ", na=False) | d.str.endswith(" home", na=False) | (d == "home address") | d.str.contains("home address", regex=False, na=False)
            skip_remove = origin_is_home & destin_is_home
        skip_test = pd.Series(False, index=final_test.index)
        if "INTERV_INIT" in final_test.columns:
            interv = final_test["INTERV_INIT"]
            skip_test = (interv == 999) | (interv.astype(str).str.strip() == "999")
        skip_no_5min = pd.Series(False, index=final_test.index)
        if "HAVE_5_MIN_FOR_SURVECode" in final_test.columns:
            v5 = pd.to_numeric(final_test["HAVE_5_MIN_FOR_SURVECode"], errors="coerce")
            skip_no_5min = (v5 != 1)

        n_rows = len(final_test)
        id_vals = final_test["id"].values if "id" in final_test.columns else final_test.index.values
        has_prev_arr = has_prev.values
        has_next_arr = has_next.values
        surveyed_vals = final_test[surveyed_route_col].fillna("").astype(str).str.strip().values if surveyed_route_col and surveyed_route_col in final_test.columns else np.array([""] * n_rows, dtype=object)
        origin_addr_vals = final_test[origin_addr_col].fillna("").astype(str).str.strip().str.lower().values if origin_addr_col and origin_addr_col in final_test.columns else np.array([""] * n_rows, dtype=object)
        destin_addr_vals = final_test[destin_addr_col].fillna("").astype(str).str.strip().str.lower().values if destin_addr_col and destin_addr_col in final_test.columns else np.array([""] * n_rows, dtype=object)
        skip_remove_arr = skip_remove.values
        skip_test_arr = skip_test.values
        skip_no_5min_arr = skip_no_5min.values

        details_df = None
        try:
            details_df = details_dataframe(stops_path) if stops_path and os.path.isfile(stops_path) else None
        except Exception:
            details_df = None
        _lime_col = "LIME_CODE" if (details_df is not None and hasattr(details_df, "columns") and "LIME_CODE" in details_df.columns) else None
        airport_lime_to_latlon = {}
        if details_df is not None and _lime_col and _lime_col in details_df.columns:
            for j in range(len(details_df)):
                try:
                    code = str(details_df.iloc[j].get(_lime_col, "") or "").strip()
                    if code and code not in airport_lime_to_latlon:
                        la, lo = _get_details_lat_lon(details_df.iloc[j])
                        if la is not None and lo is not None:
                            airport_lime_to_latlon[code] = (la, lo)
                except (KeyError, TypeError, ValueError):
                    pass

        _o_lat_arr = _coords_df[o_lat].values if _coords_df is not None and o_lat and o_lat in _coords_df.columns else None
        _o_lon_arr = _coords_df[o_lon].values if _coords_df is not None and o_lon and o_lon in _coords_df.columns else None
        _b_lat_arr = _coords_df[b_lat].values if _coords_df is not None and b_lat and b_lat in _coords_df.columns else None
        _b_lon_arr = _coords_df[b_lon].values if _coords_df is not None and b_lon and b_lon in _coords_df.columns else None
        _a_lat_arr = _coords_df[a_lat].values if _coords_df is not None and a_lat and a_lat in _coords_df.columns else None
        _a_lon_arr = _coords_df[a_lon].values if _coords_df is not None and a_lon and a_lon in _coords_df.columns else None
        _d_lat_arr = _coords_df[d_lat].values if _coords_df is not None and d_lat and d_lat in _coords_df.columns else None
        _d_lon_arr = _coords_df[d_lon].values if _coords_df is not None and d_lon and d_lon in _coords_df.columns else None

        home_lat_col = col_map_early.get("home_lat")
        home_lon_col = col_map_early.get("home_lon")
        origin_place_type_col = col_map_early.get("origin_place_type")
        destin_place_type_col = col_map_early.get("destin_place_type")
        airport_code_col = col_map_early.get("airport_code")

        def _get_coords_with_resolution(row, lat_col, lon_col, addr_str, role, rid):
            raw_lat = row.get(lat_col) if lat_col and lat_col in row.index else None
            raw_lon = row.get(lon_col) if lon_col and lon_col in row.index else None
            lat, lon = clean_coordinate_value(raw_lat), clean_coordinate_value(raw_lon)
            if lat is not None and lon is not None:
                return lat, lon, False
            place_type_col = origin_place_type_col if role == "origin" else destin_place_type_col
            place_type = ""
            if place_type_col and place_type_col in row.index:
                pt_val = row.get(place_type_col)
                if pd.notna(pt_val):
                    place_type = str(pt_val).strip().lower()
            if not place_type and addr_str:
                place_type = addr_str
            if ("hotel" in place_type or "home" in place_type) and home_lat_col and home_lon_col and home_lat_col in row.index and home_lon_col in row.index:
                fallback_lat = row.get(home_lat_col)
                fallback_lng = row.get(home_lon_col)
                la = clean_coordinate_value(fallback_lat)
                lo = clean_coordinate_value(fallback_lng)
                if la is not None and lo is not None:
                    if "hotel" in place_type:
                        skip_counts["coords_resolved_from_hotel"] += 1
                    else:
                        skip_counts["coords_resolved_from_home"] += 1
                    return la, lo, True
            if "airport" in place_type and airport_code_col and airport_code_col in row.index and airport_lime_to_latlon:
                airport_code_val = row.get(airport_code_col)
                if pd.notna(airport_code_val):
                    code_str = str(airport_code_val).strip()
                    if code_str in airport_lime_to_latlon:
                        la, lo = airport_lime_to_latlon[code_str]
                        skip_counts["coords_resolved_from_airport"] += 1
                        return la, lo, True
            return None, None, False

        skip_counts = {
            "total_records_processed": 0,
            "skipped_remove_condition": 0,
            "skipped_test_record": 0,
            "skipped_no_5min": 0,
            "skipped_both_transfers_exist": 0,
            "skipped_missing_coords_prev": 0,
            "skipped_distance_too_small_prev": 0,
            "skipped_missing_coords_next": 0,
            "skipped_distance_too_small_next": 0,
            "skipped_no_route_found_prev": 0,
            "skipped_no_route_found_next": 0,
            "skipped_chaining_loop": 0,
            "skipped_chaining_no_progress": 0,
            "skipped_chaining_already_close": 0,
            "skipped_invalid_data": 0,
            "suggestions_generated_prev": 0,
            "suggestions_generated_next": 0,
            "total_legs_suggested": 0,
            "existing_transfers_evaluated": 0,
            "better_suggestions_prev": 0,
            "better_suggestions_next": 0,
            "better_suggestion_walk_improvements": [],
            "coords_resolved_from_home": 0,
            "coords_resolved_from_airport": 0,
            "coords_resolved_from_hotel": 0,
            "coords_still_missing_after_resolution": 0,
            "data_cleaned_success": 0,
            "data_cleaned_failed": 0,
        }

        suggested_transfers_list = []
        _progress_interval = max(1, n_rows // 20)
        for i in range(n_rows):
            if i > 0 and i % _progress_interval == 0:
                logger.info("Transfer suggestions: %d/%d rows (%d%%)", i, n_rows, 100 * i // n_rows)
            skip_counts["total_records_processed"] += 1
            rid = id_vals[i]
            if skip_remove_arr[i]:
                skip_counts["skipped_remove_condition"] += 1
                continue
            if skip_test_arr[i]:
                skip_counts["skipped_test_record"] += 1
                continue
            if skip_no_5min_arr[i]:
                skip_counts["skipped_no_5min"] += 1
                continue
            idx = final_test.index[i]
            row = None
            surveyed_route = surveyed_vals[i] if not is_missing(surveyed_vals[i]) else ""
            if not isinstance(surveyed_route, str):
                surveyed_route = str(surveyed_route).strip() if surveyed_route is not None else ""
            else:
                surveyed_route = surveyed_route.strip()
            evaluating_existing = bool(has_prev_arr[i] and has_next_arr[i])
            if evaluating_existing:
                skip_counts["existing_transfers_evaluated"] += 1
            origin_addr_str = origin_addr_vals[i] if origin_addr_vals is not None else ""
            destin_addr_str = destin_addr_vals[i] if destin_addr_vals is not None else ""
            if not isinstance(origin_addr_str, str):
                origin_addr_str = str(origin_addr_str or "").strip().lower()
            else:
                origin_addr_str = origin_addr_str.strip().lower()
            if not isinstance(destin_addr_str, str):
                destin_addr_str = str(destin_addr_str or "").strip().lower()
            else:
                destin_addr_str = destin_addr_str.strip().lower()

            if o_lat and o_lon and b_lat and b_lon and all(c in final_test.columns for c in [o_lat, o_lon, b_lat, b_lon]):
                o1 = float(_o_lat_arr[i]) if _o_lat_arr is not None and pd.notna(_o_lat_arr[i]) else None
                o2 = float(_o_lon_arr[i]) if _o_lon_arr is not None and pd.notna(_o_lon_arr[i]) else None
                b1 = float(_b_lat_arr[i]) if _b_lat_arr is not None and pd.notna(_b_lat_arr[i]) else None
                b2 = float(_b_lon_arr[i]) if _b_lon_arr is not None and pd.notna(_b_lon_arr[i]) else None
                if o1 is None or o2 is None:
                    if row is None:
                        row = final_test.iloc[i]
                    o1, o2, _ = _get_coords_with_resolution(row, o_lat, o_lon, origin_addr_str, "origin", rid)
                if o1 is None or o2 is None:
                    skip_counts["coords_still_missing_after_resolution"] += 1
                if (o1 is not None and o2 is not None and b1 is not None and b2 is not None):
                    skip_counts["data_cleaned_success"] += 1
                    try:
                        origin_xy = (float(o1), float(o2))
                        board_xy = (float(b1), float(b2))
                        d_ob = suggest_haversine_miles(origin_xy[0], origin_xy[1], board_xy[0], board_xy[1])
                        if d_ob is None or d_ob < SUGGEST_MIN_GAP_MILES:
                            if not has_prev_arr[i]:
                                skip_counts["skipped_distance_too_small_prev"] += 1
                        else:
                            prev_suggestions = _run_transfer_chain(
                                stops_df, origin_xy, board_xy, "PREV", rid, surveyed_route,
                                suggest_haversine_miles,
                                chain_debug_counts=skip_counts,
                            )
                            if not prev_suggestions:
                                if not has_prev_arr[i]:
                                    skip_counts["skipped_no_route_found_prev"] += 1
                            else:
                                is_better_prev = False
                                if evaluating_existing and has_prev_arr[i]:
                                    if row is None:
                                        row = final_test.iloc[i]
                                    existing_legs = _get_existing_transfer_count(row, "PREV", col_map_early)
                                    existing_walk = _get_existing_transfer_walk_miles(row, "PREV", col_map_early)
                                    sug_walk = _get_suggested_total_walk(prev_suggestions)
                                    is_better_prev = _is_better_suggestion(len(prev_suggestions), sug_walk, existing_legs, existing_walk)
                                    if is_better_prev:
                                        skip_counts["better_suggestions_prev"] += 1
                                        if existing_walk and sug_walk is not None and existing_walk > 0:
                                            pct = (1 - sug_walk / existing_walk) * 100
                                            skip_counts["better_suggestion_walk_improvements"].append(pct)
                                if not evaluating_existing or not has_prev_arr[i] or is_better_prev:
                                    skip_counts["suggestions_generated_prev"] += 1
                                    skip_counts["total_legs_suggested"] += len(prev_suggestions)
                                    suggested_transfers_list.extend(prev_suggestions)
                    except (ValueError, TypeError, KeyError):
                        skip_counts["skipped_invalid_data"] += 1
                        skip_counts["data_cleaned_failed"] += 1
                else:
                    skip_counts["data_cleaned_failed"] += 1
                    if not has_prev_arr[i]:
                        skip_counts["skipped_missing_coords_prev"] += 1
            else:
                if not has_prev_arr[i]:
                    skip_counts["skipped_missing_coords_prev"] += 1

            if True:
                d1 = float(_d_lat_arr[i]) if _d_lat_arr is not None and pd.notna(_d_lat_arr[i]) else None
                d2 = float(_d_lon_arr[i]) if _d_lon_arr is not None and pd.notna(_d_lon_arr[i]) else None
                a1 = float(_a_lat_arr[i]) if _a_lat_arr is not None and pd.notna(_a_lat_arr[i]) else None
                a2 = float(_a_lon_arr[i]) if _a_lon_arr is not None and pd.notna(_a_lon_arr[i]) else None
                if d1 is None or d2 is None:
                    if row is None:
                        row = final_test.iloc[i]
                    d1, d2, _ = _get_coords_with_resolution(row, d_lat, d_lon, destin_addr_str, "destination", rid)
                if d1 is None or d2 is None:
                    if row is None:
                        row = final_test.iloc[i]
                    if d_lat and d_lon and d_lat in row.index and d_lon in row.index:
                        d1 = clean_coordinate_value(row.get(d_lat))
                        d2 = clean_coordinate_value(row.get(d_lon))
                if d1 is None or d2 is None:
                    if row is None:
                        row = final_test.iloc[i]
                    d1, d2, _ = _get_coords_with_resolution(row, d_lat, d_lon, destin_addr_str, "destination", rid)
                if not (a_lat and a_lon and d_lat and d_lon and all(c in final_test.columns for c in [a_lat, a_lon, d_lat, d_lon])):
                    if not has_next_arr[i]:
                        skip_counts["skipped_missing_coords_next"] += 1
                else:
                    if a1 is None or a2 is None:
                        a1 = clean_coordinate_value(final_test.at[idx, a_lat]) if a_lat in final_test.columns else None
                        a2 = clean_coordinate_value(final_test.at[idx, a_lon]) if a_lon in final_test.columns else None
                    if d1 is None or d2 is None:
                        skip_counts["coords_still_missing_after_resolution"] += 1
                        if not has_next_arr[i]:
                            skip_counts["skipped_missing_coords_next"] += 1
                    else:
                        try:
                            if (a1 is not None and a2 is not None and d1 is not None and d2 is not None):
                                skip_counts["data_cleaned_success"] += 1
                                off_xy = (float(a1), float(a2))
                                dest_xy = (float(d1), float(d2))
                                d_ad = suggest_haversine_miles(off_xy[0], off_xy[1], dest_xy[0], dest_xy[1])
                                if d_ad is None or d_ad < SUGGEST_MIN_GAP_MILES:
                                    if not has_next_arr[i]:
                                        skip_counts["skipped_distance_too_small_next"] += 1
                                else:
                                    next_suggestions = _run_transfer_chain(
                                        stops_df, off_xy, dest_xy, "NEXT", rid, surveyed_route,
                                        suggest_haversine_miles,
                                        chain_debug_counts=skip_counts,
                                    )
                                    if not next_suggestions:
                                        if not has_next_arr[i]:
                                            skip_counts["skipped_no_route_found_next"] += 1
                                    else:
                                        is_better_next = False
                                        if evaluating_existing and has_next_arr[i]:
                                            if row is None:
                                                row = final_test.iloc[i]
                                            existing_legs = _get_existing_transfer_count(row, "NEXT", col_map_early)
                                            existing_walk = _get_existing_transfer_walk_miles(row, "NEXT", col_map_early)
                                            sug_walk = _get_suggested_total_walk(next_suggestions)
                                            is_better_next = _is_better_suggestion(len(next_suggestions), sug_walk, existing_legs, existing_walk)
                                            if is_better_next:
                                                skip_counts["better_suggestions_next"] += 1
                                                if existing_walk and sug_walk is not None and existing_walk > 0:
                                                    pct = (1 - sug_walk / existing_walk) * 100
                                                    skip_counts["better_suggestion_walk_improvements"].append(pct)
                                        if not evaluating_existing or not has_next_arr[i] or is_better_next:
                                            skip_counts["suggestions_generated_next"] += 1
                                            skip_counts["total_legs_suggested"] += len(next_suggestions)
                                            suggested_transfers_list.extend(next_suggestions)
                            else:
                                skip_counts["data_cleaned_failed"] += 1
                                if not has_next_arr[i]:
                                    skip_counts["skipped_missing_coords_next"] += 1
                        except (ValueError, TypeError, KeyError):
                            skip_counts["skipped_invalid_data"] += 1
                            skip_counts["data_cleaned_failed"] += 1

        if suggested_transfers_list:
            logger.info("Generated %d transfer suggestions", len(suggested_transfers_list))
            return pd.DataFrame(suggested_transfers_list)
        logger.info("No transfer suggestions generated")
        return None
    except Exception as e:
        logger.warning("Could not generate transfer suggestions: %s", e)
        return None


def _merge_suggested_transfers_into_output(
    final_test: pd.DataFrame, suggested_transfers_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge SUGGESTED_PREV/NEXT_TRANSFER_* columns into output (same as original script)."""
    out = final_test.copy()
    for transfer_type in ["PREV", "NEXT"]:
        type_transfers = suggested_transfers_df[suggested_transfers_df["transfer_type"] == transfer_type]
        if len(type_transfers) == 0:
            continue
        for idx in range(1, 5):
            col_prefix = f"SUGGESTED_{transfer_type}_TRANSFER" if idx == 1 else f"SUGGESTED_{transfer_type}_TRANSFER_{idx}"
            for suf in ["_ROUTE_ID", "_ROUTE_NAME", "_TOTAL_WALK_MILES"]:
                c = col_prefix + suf
                if c not in out.columns:
                    out[c] = ""
        ids_out = out["id"] if "id" in out.columns else pd.Series(out.index, index=out.index)
        for sug_id, group in type_transfers.groupby("id"):
            mask = ids_out == sug_id
            if not mask.any():
                continue
            group_sorted = group.sort_values("transfer_number") if "transfer_number" in group.columns else group
            for i, (_, sug_row) in enumerate(group_sorted.iterrows(), 1):
                if i > 4:
                    break
                col_prefix = f"SUGGESTED_{transfer_type}_TRANSFER" if i == 1 else f"SUGGESTED_{transfer_type}_TRANSFER_{i}"
                route_id_val = sug_row.get("suggested_route_id", "") or ""
                route_name_val = sug_row.get("suggested_route_name", "") or ""
                walk_val = sug_row.get("total_walk_miles", "")
                if route_id_val is None or (isinstance(route_id_val, float) and pd.isna(route_id_val)):
                    route_id_val = ""
                else:
                    route_id_val = str(route_id_val).strip()
                if route_name_val is None or (isinstance(route_name_val, float) and pd.isna(route_name_val)):
                    route_name_val = ""
                else:
                    route_name_val = str(route_name_val).strip()
                try:
                    walk_val = round(float(walk_val), 2) if walk_val not in (None, "", pd.NA) and not (isinstance(walk_val, float) and pd.isna(walk_val)) else ""
                except (TypeError, ValueError):
                    walk_val = ""
                out.loc[mask, f"{col_prefix}_ROUTE_ID"] = route_id_val
                out.loc[mask, f"{col_prefix}_ROUTE_NAME"] = route_name_val
                out.loc[mask, f"{col_prefix}_TOTAL_WALK_MILES"] = walk_val
    return out


def build_full_record_output(
    full_df: pd.DataFrame,
    approved_ids: pd.Series,
    supervisor_only_ids: pd.Series,
    step03_df: pd.DataFrame,
    debug_approval_ids: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Build full-record output matching auto_and_suggestions_more_transfers.py structure.
    Same semantics: Field Approved,  , Auto-Remove (Origin=Destination=Home),
    Auto-Use (pipeline-approved), Supervisor View Only.
    Difference: Auto-approved -> FINAL_REVIEWER='Tosia', Final_Usage='Use';
    Supervisor only -> FINAL_REVIEWER='Tosia', Final_Usage left empty.
    """
    out = full_df.copy()
    n = len(out)
    id_col = REQUIRED_ID if REQUIRED_ID in out.columns else out.index
    ids = out[REQUIRED_ID] if REQUIRED_ID in out.columns else pd.Series(out.index.values, index=out.index)

    # Columns to match auto_and_suggestions_more_transfers.py output (same names and defaults)
    from datetime import date as date_type
    if "Elvis_Date" not in out.columns:
        out.insert(0, "Elvis_Date", date_type.today())
    if "elvis_id" not in out.columns:
        out["elvis_id"] = out[REQUIRED_ID] if REQUIRED_ID in out.columns else out.index
    for col, default in [
        ("SUPERVISOR_COMMENT", " "),
        ("route_match_flag", "Elvis_Review"),
        ("distance_flag", "Elvis_Review"),
        ("POSSIBLE ERRORS", " "),
        ("REASON FOR REMOVAL [Other]", " "),
        ("REASON FOR REMOVAL", " "),
        ("FINAL_REVIEWER", " "),
        ("Final_Usage", ""),
        ("APPROVAL_BASIS", ""),
        ("ZIGZAG_FLAG", ""),
        ("NEXT_TRANSFER_COUNT", ""),
        ("PREV_TRANSFER_COUNT", ""),
        ("PATTERN_NUMBER", ""),
        ("2nd Cleaner", " "),
        ("1st Cleaner", " "),
    ]:
        if col not in out.columns:
            out[col] = default
    if "ELVIS_COMMENT" in out.columns:
        out["SUPERVISOR_COMMENT"] = out["ELVIS_COMMENT"].fillna(" ").astype(str)

    approved_set = set(approved_ids.dropna().astype(str))
    supervisor_set = set(supervisor_only_ids.dropna().astype(str))

    # Field Approved: if a record is field approved, it stays Use and does not go through other conditions.
    def _norm_header_name(x: Any) -> str:
        # Normalize headers for resilient matching across `ElvisStatus` vs `ELVIS_STATUS` vs `Elvis Status`.
        return re.sub(r"[^a-z0-9]+", "", str(x).strip().lower())

    expected_elvis_norm = _norm_header_name(COL_ELVIS_STATUS)
    elvis_col = None
    elvis_col_name: Optional[str] = None
    if COL_ELVIS_STATUS in out.columns:
        elvis_col = out[COL_ELVIS_STATUS]
        elvis_col_name = COL_ELVIS_STATUS
    else:
        # Prefer exact normalized match without accidentally selecting "...Code" variants.
        for c in out.columns:
            if _norm_header_name(c) == expected_elvis_norm:
                elvis_col = out[c]
                elvis_col_name = str(c)
                break
    if elvis_col is not None:
        logger.info("Field Approved: using ElvisStatus source column: %s", elvis_col_name)
        field_approved_mask = elvis_col.astype(str).str.contains(_RE_APPROVED, na=False)
        out.loc[field_approved_mask, "Final_Usage"] = "Use"
        out.loc[field_approved_mask, "FINAL_REVIEWER"] = "Field Approved"
        out.loc[field_approved_mask, "1st Cleaner"] = "Field Approved"
        # Field Approved records stay Use; we do not override them for boarding=alighting or extra-short/missing-route.

    # Test (INTERV_INIT == 999)
    # Try PBIX/ls2 and ls6 column names (mapping may not have been applied)
    if COL_INTERV_INIT in out.columns:
        interv_col = out[COL_INTERV_INIT]
    elif "IntervInit" in out.columns:
        interv_col = out["IntervInit"]
    else:
        interv_col = None
    if interv_col is not None:
        test_mask = (
            (interv_col.astype(str).str.strip() == "999") | (interv_col == 999)
        ) & (out["Final_Usage"] != "Use")
        out.loc[test_mask, "Final_Usage"] = "Remove"
        out.loc[test_mask, "FINAL_REVIEWER"] = "Test/No 5 MIN"
        out.loc[test_mask, "1st Cleaner"] = "Test"
    else:
        logger.warning("INTERV_INIT / IntervInit column not found; Test (999) Remove condition skipped")

    # No 5 MIN (HAVE_5_MIN_FOR_SURVECode != 1) - do not override Use
    expected_have5_norm = _norm_header_name(COL_HAVE_5_MIN)
    have5_col = None
    have5_col_name: Optional[str] = None
    if COL_HAVE_5_MIN in out.columns:
        have5_col = out[COL_HAVE_5_MIN]
        have5_col_name = COL_HAVE_5_MIN
    else:
        # Normalized header lookup for `HAVE_5_MIN_FOR_SURVECode` <-> `HAVE_5_MIN_FOR_SURVE_Code_`
        for c in out.columns:
            if _norm_header_name(c) == expected_have5_norm:
                have5_col = out[c]
                have5_col_name = str(c)
                break
        if have5_col is None and "Have5MinForSurveCode" in out.columns:
            have5_col = out["Have5MinForSurveCode"]
            have5_col_name = "Have5MinForSurveCode"
    if have5_col is not None:
        logger.info("No 5 MIN: using HAVE_5_MIN source column: %s", have5_col_name)
        v5 = pd.to_numeric(have5_col, errors="coerce").fillna(-1)
        no_5min = (v5 != 1) & (out["Final_Usage"] != "Use")
        out.loc[no_5min, "Final_Usage"] = "Remove"
        out.loc[no_5min, "FINAL_REVIEWER"] = "Test/No 5 MIN"
        out.loc[no_5min, "1st Cleaner"] = "No 5 MIN"
    else:
        logger.warning("HAVE_5_MIN_FOR_SURVECode / Have5MinForSurveCode column not found; No 5 MIN Remove condition skipped")

    # Auto-Remove: Origin = Destination = Home (place-type or address-based, same as original)
    remove_home_mask = (_remove_mask_origin_dest_home(out) | _remove_mask_origin_dest_home_address(out)) & (out["Final_Usage"] != "Use")
    out.loc[remove_home_mask, "Final_Usage"] = "Remove"
    out.loc[remove_home_mask, "FINAL_REVIEWER"] = "Auto-Remove"
    out.loc[remove_home_mask, "1st Cleaner"] = "Origin = Destination = Home"
    out.loc[remove_home_mask, "REASON FOR REMOVAL"] = "Origin = Destination = Home"

    # Phase 1/2 approval: if a record passes any Phase 1 or Phase 2 condition, it is Use (Tosia),
    # and then (01)(02)(03) flags/checks can downgrade it to Supervisor Only if it does not pass.
    id_str = ids.astype(str).replace("nan", "")
    not_use = out["Final_Usage"] != "Use"
    not_remove = out["Final_Usage"] != "Remove"

    # Use if passed (01)(02)(03) (these ids are a subset of Phase-All)
    # Exclude records that are marked Supervisor Only by OD [Supervisor Only Checks]
    auto_use_mask = id_str.isin(approved_set) & (~id_str.isin(supervisor_set)) & not_use & not_remove
    out.loc[auto_use_mask, "Final_Usage"] = "Use"
    out.loc[auto_use_mask, "FINAL_REVIEWER"] = "Tosia"
    out.loc[auto_use_mask, "1st Cleaner"] = "HereAPI"
    out.loc[auto_use_mask, "APPROVAL_BASIS"] = "EXISTING_TRANSFERS"
    if COL_GROUP in step03_df.columns and not step03_df.empty:
        grp_map = step03_df.set_index(step03_df[REQUIRED_ID].astype(str))[COL_GROUP].to_dict()
        out.loc[auto_use_mask, COL_GROUP] = id_str.loc[auto_use_mask].map(grp_map).fillna(out.loc[auto_use_mask, COL_GROUP])

    # Supervisor Only: passed Phase 1/2 but did not pass (01)(02)(03)
    supervisor_mask = ids.astype(str).isin(supervisor_set) & not_use & not_remove
    out.loc[supervisor_mask, "FINAL_REVIEWER"] = "Tosia"
    out.loc[supervisor_mask, "Final_Usage"] = ""

    # Match original script's extra status columns
    out["ROUTE_STATUS"] = "Elvis_Review"
    out["Stops_Status"] = "Elvis_Review"
    out["Test_Status"] = "Tested"
    if "DATE_SUBMITTED" in out.columns and "DATE" not in out.columns:
        try:
            # format='mixed' (pandas 2.0+) avoids "Could not infer format" warning when column has mixed formats
            date_series = pd.to_datetime(out["DATE_SUBMITTED"], errors="coerce", format="mixed").dt.strftime("%m/%d/%Y")
        except (TypeError, ValueError):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                try:
                    date_series = pd.to_datetime(out["DATE_SUBMITTED"], errors="coerce").dt.strftime("%m/%d/%Y")
                except Exception:
                    date_series = pd.Series("", index=out.index)
        insert_at = out.columns.get_loc("DATE_SUBMITTED") + 1
        out.insert(insert_at, "DATE", date_series)

    # Final output columns: match auto_and_suggestions_more_transfers.py output exactly (same column set + order).
    # Source: header of LACMTA_FEEDER_KINGElvis_auto_approval_20260315.csv (original script output).
    desired_cols = [
        "Elvis_Date",
        "elvis_id",
        "1st Cleaner",
        "2nd Cleaner",
        "PATTERN_NUMBER",
        "PREV_TRANSFER_COUNT",
        "NEXT_TRANSFER_COUNT",
        "ZIGZAG_FLAG",
        "APPROVAL_BASIS",
        "Final_Usage",
        "FINAL_REVIEWER",
        "REASON FOR REMOVAL",
        "REASON FOR REMOVAL [Other]",
        "POSSIBLE ERRORS",
        "distance_flag",
        "route_match_flag",
        "SUPERVISOR_COMMENT",
        "HOME_ADDRESS_LAT",
        "HOME_ADDRESS_LONG",
        "id",
        "DATE_SUBMITTED",
        "DATE",
        "INTERV_INIT",
        "ROUTE_SURVEYEDCode",
        "ROUTE_SURVEYED",
        "HAVE_5_MIN_FOR_SURVECode",
        "HAVE_5_MIN_FOR_SURVE",
        "ORIGIN_PLACE_TYPE",
        "ORIGIN_TRANSPORTCode",
        "ORIGIN_TRANSPORT",
        "DESTIN_PLACE_TYPE",
        "DESTIN_TRANSPORTCode",
        "DESTIN_TRANSPORT",
        "ELVIS_COMMENT",
        "ElvisStatus",
        "ORIGIN_ADDRESS_ADDR",
        "ORIGIN_ADDRESS_LAT",
        "ORIGIN_ADDRESS_LONG",
        "DESTIN_ADDRESS_ADDR",
        "DESTIN_ADDRESS_LAT",
        "DESTIN_ADDRESS_LONG",
        "STOP_ON_LAT",
        "STOP_ON_LONG",
        "STOP_OFF_LAT",
        "STOP_OFF_LONG",
        "PREV_TRANSFERS",
        "NEXT_TRANSFERS",
        "VAL_COUNT_PREVTRANS",
        "VAL_COUNT_NEXTTRANS",
        "VAL_DIST_OB",
        "VAL_DIST_BA",
        "VAL_DIST_AD",
        "VAL_DIST_OD",
        "PREV_TRAN_1_ON_BUS_LAT",
        "PREV_TRAN_1_ON_BUS_LONG",
        "PREV_TRAN_1_OFF_BUS_LAT",
        "PREV_TRAN_1_OFF_BUS_LONG",
        "PREV_TRAN_2_ON_BUS_LAT",
        "PREV_TRAN_2_ON_BUS_LONG",
        "PREV_TRAN_2_OFF_BUS_LAT",
        "PREV_TRAN_2_OFF_BUS_LONG",
        "PREV_TRAN_3_ON_BUS_LAT",
        "PREV_TRAN_3_ON_BUS_LONG",
        "PREV_TRAN_3_OFF_BUS_LAT",
        "PREV_TRAN_3_OFF_BUS_LONG",
        "PREV_TRAN_4_ON_BUS_LAT",
        "PREV_TRAN_4_ON_BUS_LONG",
        "PREV_TRAN_4_OFF_BUS_LAT",
        "PREV_TRAN_4_OFF_BUS_LONG",
        "NEXT_TRAN_1_ON_BUS_LAT",
        "NEXT_TRAN_1_ON_BUS_LONG",
        "NEXT_TRAN_1_OFF_BUS_LAT",
        "NEXT_TRAN_1_OFF_BUS_LONG",
        "ROUTE_STATUS",
        "Stops_Status",
        "Test_Status",
        "SUGGESTED_PREV_TRANSFER_ROUTE_ID",
        "SUGGESTED_PREV_TRANSFER_ROUTE_NAME",
        "SUGGESTED_PREV_TRANSFER_TOTAL_WALK_MILES",
        "SUGGESTED_PREV_TRANSFER_2_ROUTE_ID",
        "SUGGESTED_PREV_TRANSFER_2_ROUTE_NAME",
        "SUGGESTED_PREV_TRANSFER_2_TOTAL_WALK_MILES",
        "SUGGESTED_PREV_TRANSFER_3_ROUTE_ID",
        "SUGGESTED_PREV_TRANSFER_3_ROUTE_NAME",
        "SUGGESTED_PREV_TRANSFER_3_TOTAL_WALK_MILES",
        "SUGGESTED_PREV_TRANSFER_4_ROUTE_ID",
        "SUGGESTED_PREV_TRANSFER_4_ROUTE_NAME",
        "SUGGESTED_PREV_TRANSFER_4_TOTAL_WALK_MILES",
        "SUGGESTED_NEXT_TRANSFER_ROUTE_ID",
        "SUGGESTED_NEXT_TRANSFER_ROUTE_NAME",
        "SUGGESTED_NEXT_TRANSFER_TOTAL_WALK_MILES",
        "SUGGESTED_NEXT_TRANSFER_2_ROUTE_ID",
        "SUGGESTED_NEXT_TRANSFER_2_ROUTE_NAME",
        "SUGGESTED_NEXT_TRANSFER_2_TOTAL_WALK_MILES",
        "SUGGESTED_NEXT_TRANSFER_3_ROUTE_ID",
        "SUGGESTED_NEXT_TRANSFER_3_ROUTE_NAME",
        "SUGGESTED_NEXT_TRANSFER_3_TOTAL_WALK_MILES",
        "SUGGESTED_NEXT_TRANSFER_4_ROUTE_ID",
        "SUGGESTED_NEXT_TRANSFER_4_ROUTE_NAME",
        "SUGGESTED_NEXT_TRANSFER_4_TOTAL_WALK_MILES",
    ]
    for c in desired_cols:
        if c not in out.columns:
            out[c] = ""
    out = out.loc[:, desired_cols]

    logger.info(
        "Full output: Use=%d, Remove=%d, Supervisor View Only=%d",
        (out["Final_Usage"] == "Use").sum(),
        (out["Final_Usage"] == "Remove").sum(),
        0,
    )
    if debug_approval_ids:
        _log_approval_debug(out, approved_set, debug_approval_ids, step03_df)
    return out


# ---------------------------------------------------------------------------
# Section 7: Main Processing Function
# ---------------------------------------------------------------------------

def validate_input_columns(df: pd.DataFrame) -> List[str]:
    """Check for required columns; return list of missing column names."""
    required = [REQUIRED_ID, COL_ORIGIN_PLACE_TYPE, COL_DESTIN_PLACE_TYPE, COL_MERGED_ORIGIN_LAT, COL_MERGED_ORIGIN_LONG, COL_MERGED_DESTIN_LAT, COL_MERGED_DESTIN_LONG]
    missing = [c for c in required if c not in df.columns]
    if not missing:
        return []
    alt = {"ORIGIN_ADDRESS_LAT_": COL_MERGED_ORIGIN_LAT, "ORIGIN_ADDRESS_LONG_": COL_MERGED_ORIGIN_LONG, "DESTIN_ADDRESS_LAT_": COL_MERGED_DESTIN_LAT, "DESTIN_ADDRESS_LONG_": COL_MERGED_DESTIN_LONG}
    for a, r in alt.items():
        if a in df.columns and r in missing:
            missing.remove(r)
    return missing


def run_improved_auto_approval(
    input_data: Union[str, pd.DataFrame, List[Dict]],
    run_transfer_suggestions: bool = True,
    suggestion_record_ids: Optional[set] = None,
    stops_path: Optional[str] = None,
    output_path: Optional[str] = None,
    mapping_file: Optional[str] = None,
    details_file: Optional[str] = None,
    details_df: Optional[pd.DataFrame] = None,
    mapping_sheet: str = "Example",
    full_output: bool = True,
    debug_supervisor_flags: bool = False,
    debug_approval_ids: Optional[List[str]] = None,
    debug_transfers: bool = False,
    debug_supervisor_ids: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Run the full improved auto-approval pipeline.
    Input can be Elvis export (ls6 columns): use mapping_file and details_file to prepare
    data to PBIX-style columns before running the pipeline (same as auto_and_suggestions_more_transfers.py).

    Parameters
    ----------
    input_data : str, DataFrame, or list of dicts
        Path to CSV (e.g. elvis_transit_ls6_733524_export_odbc.csv) or DataFrame from DB.
    mapping_file : str, optional
        Excel with Headers-ls6 -> FormattedHeader-ls2 (e.g. request_20250708_ls6tols2-headers.xlsx).
    details_file : str, optional
        Details Excel for home/airport/hotel (e.g. details_lacmta-feeder_733524_od_excel.xlsx).
        Worksheets ``STOPS`` and/or ``XFER_STOPS`` also supply __(01) ``xfer_list`` (agency route table for
        ``AGNECY_TRANSFERS-*`` merges when those columns exist; otherwise one row per ``ETC_ROUTE_ID``).
    mapping_sheet : str
        Sheet name in mapping_file (default "Example").
    full_output : bool
        If True (default), return full-record DataFrame with Final_Usage, FINAL_REVIEWER, 1st Cleaner,
        REASON FOR REMOVAL, POSSIBLE ERRORS, Supervisor View Only, etc. (same schema as original).
        If False, return only ___auto-approved table (id, GROUP, Supervisor View Only).

    Returns
    -------
    If full_output True: full DataFrame with all input columns plus Final_Usage, FINAL_REVIEWER, etc.
    If full_output False: DataFrame with columns id, GROUP, Supervisor View Only.
    """
    if isinstance(input_data, str):
        if input_data.lower().endswith(".csv"):
            df = pd.read_csv(input_data, low_memory=False)
        else:
            df = pd.read_excel(input_data)
        logger.info("Loaded input from %s: %d rows", input_data, len(df))
    elif isinstance(input_data, pd.DataFrame):
        df = input_data.copy()
    elif isinstance(input_data, list) and input_data and isinstance(input_data[0], dict):
        df = pd.DataFrame(input_data)
    else:
        raise ValueError("input_data must be file path, DataFrame, or list of dicts")

    if df.empty:
        logger.warning("Input is empty")
        return pd.DataFrame(columns=[REQUIRED_ID, COL_GROUP, COL_SUPERVISOR_VIEW_ONLY])

    # Match the legacy script: if no explicit stops file is provided, use the details workbook.
    effective_stops_path = stops_path or details_file
    if effective_stops_path and not os.path.isabs(effective_stops_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        candidate_path = os.path.join(script_dir, effective_stops_path)
        if os.path.isfile(candidate_path):
            effective_stops_path = candidate_path

    # Prepare data: Elvis ls6 -> ls2 (header mapping) -> MERGED coords + PBIX column names
    if mapping_file or details_file or details_df is not None:
        df = prepare_elvis_data_for_pipeline(
            df,
            mapping_file=mapping_file,
            mapping_sheet=mapping_sheet,
            details_file=details_file,
            details_df=details_df,
        )

    # Ensure 'id' column exists (pipeline and full output use it)
    id_col = _resolve_id_column(df)
    if id_col is not None and REQUIRED_ID not in df.columns:
        df[REQUIRED_ID] = df[id_col].astype(str)
        logger.info("Using column '%s' as record id", id_col)

    missing = validate_input_columns(df)
    if missing:
        logger.warning("Missing columns (may use aliases): %s", missing)

    clean_df = _ensure_clean_dataset_columns(df)
    if COL_GROUP not in clean_df.columns:
        clean_df[COL_GROUP] = ""

    # Strict Phase prerequisite computation (O2DtoFIRSTBLASTA prereqs). Nearby stops are computed below on candidates only.
    clean_df = _compute_clean_prereq_columns(clean_df)
    stops_df_for_near = details_df if details_df is not None and not details_df.empty else None

    # Candidate-only nearby stop counts (strict PQ parity but fast enough)
    if stops_df_for_near is not None:
        try:
            # compute for potential Phase1/Phase2 candidates only (prev/next <=2 and O2DtoFIRSTBLASTA=0 and coords exist)
            prev_s = _get_transfer_code_series(clean_df, "prev")
            nxt_s = _get_transfer_code_series(clean_df, "next")
            cand = (
                clean_df[COL_MERGED_ORIGIN_LAT].notna()
                & clean_df[COL_MERGED_ORIGIN_LONG].notna()
                & clean_df[COL_MERGED_DESTIN_LAT].notna()
                & clean_df[COL_MERGED_DESTIN_LONG].notna()
                & (clean_df["O2DtoFIRSTBLASTA"] == 0)
                & prev_s.isin(["0", "1", "2"])
                & nxt_s.isin(["0", "1", "2"])
            )
            near_df = _compute_number_of_nearby_stops(clean_df.loc[cand], df, stops_df_for_near)
            clean_df = clean_df.merge(near_df, on=REQUIRED_ID, how="left")
        except Exception as e:
            logger.warning("Nearby stops computation failed: %s", e)

    # Ensure transfer leg distances for Phase1/Phase2 filters (Transfer1/2/5/6_Distance)
    clean_df = _ensure_transfer_leg_distances(clean_df)

    phase1 = phase1_walk_walk_evaluation(clean_df)
    phase2 = phase2_not_walk_options(clean_df)
    phase_all = _phase_all(phase1, phase2)
    all_ids = phase_all[REQUIRED_ID]

    og_df = df if REQUIRED_ID in df.columns else clean_df
    xfer_list = None
    try:
        if details_file and os.path.isfile(details_file):
            xfer_list = load_xfer_list_from_details_excel(details_file)
        elif details_df is not None and not details_df.empty:
            xfer_list = build_xfer_list_from_details_stops_union(details_df)
    except Exception as e:
        logger.warning("Could not build xfer_list from details for __(01): %s", e)
        xfer_list = None
    if xfer_list is not None and not xfer_list.empty:
        logger.info("__(01) xfer_list from details: %d rows", len(xfer_list))
    # Checks (01)(02)(03) removed per request:
    # We bypass OD [DISTANCE TRANSFERS CHECK], OD [TRANSFERS CHECK], and OD [TRIP DISTANCE CHECK]
    # while keeping the rest of the pipeline (supervisor-only logic + final output) unchanged.
    step01 = phase_all.copy()
    step02 = phase_all.copy()
    step03 = phase_all.copy()
    approved_ids = step03[REQUIRED_ID]

    if debug_transfers:
        _debug_transfer_approval(clean_df, phase1, phase2, phase_all, step01, step02, step03)

    if debug_supervisor_ids:
        _log_supervisor_only_debug_for_ids(
            clean_df=clean_df,
            phase_all_df=phase_all,
            step01_df=step01,
            step02_df=step02,
            step03_df=step03,
            debug_ids=debug_supervisor_ids,
        )
    # Supervisor View Only flag for ___auto-approved table (refined conditions from od_approved_and_supervisor_only.txt)
    supervisor_checks_df = _compute_supervisor_only_checks_df(
        approved_df=step03,
        clean_df=clean_df,
        og_df=og_df,
        stops_df=details_df if details_df is not None else None,
    )
    supervisor_view_only_ids = (
        supervisor_checks_df.loc[supervisor_checks_df[COL_SUPERVISOR_VIEW_ONLY] == "Yes", REQUIRED_ID].astype(str)
        if not supervisor_checks_df.empty and COL_SUPERVISOR_VIEW_ONLY in supervisor_checks_df.columns
        else pd.Series([], dtype=object)
    )
    # Final "Supervisor Only" reviewer logic aligns to OD [Supervisor Only Checks]
    supervisor_only_ids = supervisor_view_only_ids
    result = final_auto_approval(step03, supervisor_only_ids=supervisor_view_only_ids)

    if full_output:
        full_record = build_full_record_output(
            full_df=df,
            approved_ids=approved_ids,
            supervisor_only_ids=supervisor_only_ids,
            step03_df=step03,
            debug_approval_ids=debug_approval_ids,
        )
        logger.info(
            "Transfer suggestion gate: run=%s, available=%s, effective_stops_path=%s, exists=%s",
            run_transfer_suggestions,
            TRANSFER_SUGGESTION_AVAILABLE,
            effective_stops_path,
            os.path.isfile(effective_stops_path) if effective_stops_path else False,
        )
        if run_transfer_suggestions and TRANSFER_SUGGESTION_AVAILABLE and effective_stops_path and os.path.isfile(effective_stops_path):
            try:
                suggestion_source = df
                if suggestion_record_ids is not None:
                    id_series = df[REQUIRED_ID].astype(str).str.strip() if REQUIRED_ID in df.columns else pd.Series([], dtype=str)
                    allowed = {str(x).strip() for x in suggestion_record_ids if str(x).strip()}
                    suggestion_source = df[id_series.isin(allowed)].copy()
                    logger.info(
                        "Transfer suggestions limited to %d new/selected record(s) (of %d total)",
                        len(suggestion_source),
                        len(df),
                    )
                if suggestion_source.empty:
                    logger.info("Transfer suggestions skipped: no eligible new records")
                    suggested_transfers_df = None
                else:
                    suggested_transfers_df = _run_transfer_suggestions_for_records(
                        suggestion_source, effective_stops_path
                    )
                if suggested_transfers_df is not None and len(suggested_transfers_df) > 0:
                    full_record = _merge_suggested_transfers_into_output(full_record, suggested_transfers_df)
            except Exception as e:
                logger.warning("Transfer suggestion merge failed: %s", e)
        else:
            logger.warning("Transfer suggestions not run because the gate condition failed")
        if output_path:
            full_record.to_csv(output_path, index=False)
            logger.info("Wrote full output to %s", output_path)
        logger.info("Pipeline complete (full output): %d approved, %d supervisor only", len(approved_ids), len(supervisor_only_ids))
        return full_record

    # Optional debug columns for validating OD [Supervisor Only Checks] parity (minimal output only)
    if debug_supervisor_flags and not supervisor_checks_df.empty:
        try:
            result = result.merge(
                supervisor_checks_df[[REQUIRED_ID, "O2DtoFULLDISTANCE", "TRANSFER_onroute_FLAG_refined", "Transfer_toOrigin_StopCounts_FLAG"]],
                on=REQUIRED_ID,
                how="left",
            )
        except Exception:
            pass

    if run_transfer_suggestions and TRANSFER_SUGGESTION_AVAILABLE:
        logger.info("Transfer suggestions requested; use full_output=True and --stops for SUGGESTED_* columns")

    if output_path:
        result.to_csv(output_path, index=False)
        logger.info("Wrote output to %s", output_path)

    logger.info("Pipeline complete: %d approved, %d supervisor only", len(result), len(supervisor_only_ids))
    return result


# ---------------------------------------------------------------------------
# Section 8: Default inputs (used when running: python improved_auto_approval.py)
# ---------------------------------------------------------------------------
# PARK CITY : "elvis_transit_ls6_154732_export_odbc.csv", "details_ParkCity_154732_od_excel.xlsx"
#IndyGo : elvis_transit_ls6_574774_export_odbc, "details_lndyGO_574774_od_excel.xlsx"
# Edit these paths for your environment; leave None to not use that option.
DEFAULT_INPUT = "elvis_transit_ls6_574774_export_odbc.csv"
DEFAULT_NAME_KE = "INDY_GO"
DEFAULT_OUTPUT = None  # if None, default matches auto_and_suggestions_more_transfers.py naming
DEFAULT_MAPPING_FILE = "request_20250708_ls6tols2-headers.xlsx"
DEFAULT_MAPPING_SHEET = "Example"
DEFAULT_DETAILS_FILE = "details_lndyGO_574774_od_excel.xlsx"
DEFAULT_STOPS_PATH = None  # e.g. "stops.csv" if you want transfer suggestions
DEFAULT_SKIP_TRANSFER_SUGGESTIONS = False
DEFAULT_MINIMAL_OUTPUT = False

# Database load: when True, load input from DB (env HOST, USER, PASSWORD) or from CSV cache.
DEFAULT_USE_DB_LOAD = False
DEFAULT_SELECT_QUERY = "SELECT * FROM elvis_transit_ls6_574774_export_odbc"
DEFAULT_DB_NAME = "transit-ls6"
# When using DB load, CSV cache filename is derived from the table name in the query (e.g. elvis_transit_ls6_733524_export_odbc.csv)
# Set to True to use details file as STOPS + XFER_STOPS sheets (combined); False uses helper's AIR sheet for home/airport/hotel.
DEFAULT_DETAILS_STOPS_XFER = True


def _load_input_from_db_or_csv(
    select_query: str,
    csv_filename: Optional[str] = None,
    db_host: Optional[str] = None,
    db_user: Optional[str] = None,
    db_password: Optional[str] = None,
    db_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load input from CSV cache if file exists, else from database using select_query.
    CSV filename defaults to table name from query + '.csv' (e.g. elvis_transit_ls6_733524_export_odbc.csv).
    """
    if csv_filename is None:
        # Derive from query: last token is table name
        csv_filename = select_query.split()[-1].strip() + ".csv"
    try:
        df = pd.read_csv(csv_filename, low_memory=False)
        logger.info("FILE ALREADY EXISTS WITH THIS NAME: %s", csv_filename)
        return df
    except FileNotFoundError:
        pass
    if not all([db_host, db_user, db_password, db_name]):
        raise ValueError("Database env vars (HOST, USER, PASSWORD, DATABASE) must be set when CSV cache is missing")
    try:
        from database import DatabaseConnector
    except ImportError:
        raise ImportError("database.DatabaseConnector required for DB load; install mysql-connector-python")
    db_connector = DatabaseConnector(db_host, db_name, db_user, db_password)
    db_connector.connect()
    try:
        df = pd.read_sql(select_query, db_connector.connection)
        logger.info("Loaded %d rows from database; saving to %s", len(df), csv_filename)
        df.to_csv(csv_filename, index=False)
        return df
    finally:
        db_connector.disconnect()


# Details workbook: worksheet names must match the details file exactly — 'STOPS' and 'XFER_STOPS'.
DETAILS_SHEET_STOPS = "STOPS"
DETAILS_SHEET_XFER_STOPS = "XFER_STOPS"

XFER_STOPS_COLUMN_RENAME: Dict[str, str] = {
    "GTFS_VER": "gtfs_ver",
    "GTFS_DATE": "gtfs_date",
    "SEQUENCE_FIXED": "seq_fixed",
    "LAT6": "stop_lat6",
    "LON6": "stop_lon6",
    "route_short_name": "route_short_name",
    "route_long_name": "route_long_name",
    "stop_id": "stop_id",
    "stop_name": "stop_name",
    "stop_lat": "stop_lat",
    "stop_lon": "stop_lon",
    "ETC_STOP_ID": "ETC_STOP_ID",
    "ETC_STOP_NAME": "ETC_STOP_NAME",
    "XFER_ROUTE_ID": "XFER_ROUTE_ID",
}


def _try_read_excel_sheet(file_path: str, sheet_name: str) -> Optional[pd.DataFrame]:
    """Return sheet DataFrame or None if the worksheet is missing or empty."""
    try:
        out = pd.read_excel(file_path, sheet_name=sheet_name)
    except ValueError as e:
        msg = str(e).lower()
        if "worksheet named" in msg or "not found" in msg or sheet_name.lower() in msg:
            return None
        raise
    if not isinstance(out, pd.DataFrame) or out.empty:
        return None
    return out


def _apply_xfer_stops_renames(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={k: v for k, v in XFER_STOPS_COLUMN_RENAME.items() if k in df.columns})


def build_xfer_list_from_details_stops_union(stops_xfer_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Build M-style xfer_list for __(01) from combined STOPS + XFER_STOPS rows (or either sheet alone).

    Uses power_query ``build_xfer_list`` when an ``agency`` column has values; otherwise one row per
    ``ETC_ROUTE_ID`` (after mapping ``XFER_ROUTE_ID``) plus leading OTH row for merges.
    """
    if stops_xfer_df is None or stops_xfer_df.empty:
        return None
    work = stops_xfer_df.copy()
    if "ETC_ROUTE_ID" not in work.columns:
        if "XFER_ROUTE_ID" in work.columns:
            work = work.rename(columns={"XFER_ROUTE_ID": "ETC_ROUTE_ID"})
        else:
            logger.info("Details stops data has no ETC_ROUTE_ID or XFER_ROUTE_ID; __(01) xfer_list skipped")
            return None
    if "AGNECY_TRANSFERS" not in work.columns:
        work["AGNECY_TRANSFERS"] = None

    use_pq = (
        _pq_build_xfer_list is not None
        and "agency" in work.columns
        and work["agency"].notna().any()
        and (work["agency"].astype(str).str.strip() != "").any()
    )
    if use_pq:
        xl = _pq_build_xfer_list(work)
    else:
        sub = work.loc[work["ETC_ROUTE_ID"].notna()].copy()
        sub = sub[sub["ETC_ROUTE_ID"].astype(str).str.strip() != ""]
        sub = sub.drop_duplicates(subset=["ETC_ROUTE_ID"], keep="first")
        keep_cols = ["ETC_ROUTE_ID", "AGNECY_TRANSFERS"]
        for c in ("ETC_ROUTE_NAME", "NOTES"):
            if c in sub.columns:
                keep_cols.append(c)
        oth = pd.DataFrame(
            [{"ETC_ROUTE_ID": "OTH", "ETC_ROUTE_NAME": "Other", "NOTES": "ALL", "AGNECY_TRANSFERS": np.nan}]
        )
        if sub.empty:
            xl = oth
        else:
            sub = sub[[c for c in keep_cols if c in sub.columns]].copy()
            for c in sub.columns:
                if c not in oth.columns:
                    oth[c] = np.nan
            for c in oth.columns:
                if c not in sub.columns:
                    sub[c] = np.nan
            sub = sub[[c for c in oth.columns]]
            xl = pd.concat([oth, sub], ignore_index=True)

    if xl is None or xl.empty:
        return None
    return xl


def load_xfer_list_from_details_excel(file_path: str) -> Optional[pd.DataFrame]:
    """Read STOPS and/or XFER_STOPS from the details workbook and build xfer_list for __(01)."""
    if not file_path or not os.path.isfile(file_path):
        return None
    parts: List[pd.DataFrame] = []
    for name in (DETAILS_SHEET_STOPS, DETAILS_SHEET_XFER_STOPS):
        chunk = _try_read_excel_sheet(file_path, name)
        if chunk is not None and not chunk.empty:
            if name == DETAILS_SHEET_XFER_STOPS:
                chunk = _apply_xfer_stops_renames(chunk.copy())
            parts.append(chunk)
    if not parts:
        logger.info("No %s or %s sheet in %s; __(01) xfer_list omitted", DETAILS_SHEET_STOPS, DETAILS_SHEET_XFER_STOPS, file_path)
        return None
    combined = pd.concat(parts, ignore_index=True)
    return build_xfer_list_from_details_stops_union(combined)


def load_details_stops_xfer(file_path: str) -> pd.DataFrame:
    """
    Load details from Excel: sheets STOPS and/or XFER_STOPS.

    If both exist, apply XFER_STOPS column renames and concat (XFER columns aligned to STOPS).
    If only one sheet exists, use that sheet alone (same behavior as user expectation for details file).
    """
    stops_df = _try_read_excel_sheet(file_path, DETAILS_SHEET_STOPS)
    xfer_raw = _try_read_excel_sheet(file_path, DETAILS_SHEET_XFER_STOPS)

    if stops_df is None and xfer_raw is None:
        raise ValueError(
            f"No worksheets {DETAILS_SHEET_STOPS!r} or {DETAILS_SHEET_XFER_STOPS!r} found in {file_path!r}"
        )

    if stops_df is not None and xfer_raw is None:
        logger.info("Sheet %r not found in %s; using %r only", DETAILS_SHEET_XFER_STOPS, file_path, DETAILS_SHEET_STOPS)
        logger.info("Total stops in details now: %d", len(stops_df))
        return stops_df.copy()

    if stops_df is None and xfer_raw is not None:
        logger.info("Sheet %r not found in %s; using %r only", DETAILS_SHEET_STOPS, file_path, DETAILS_SHEET_XFER_STOPS)
        detail_df = _apply_xfer_stops_renames(xfer_raw.copy())
        logger.info("Total stops in details now: %d", len(detail_df))
        return detail_df

    assert stops_df is not None and xfer_raw is not None
    xfer_df = _apply_xfer_stops_renames(xfer_raw.copy())
    for col in stops_df.columns:
        if col not in xfer_df.columns:
            xfer_df[col] = None
    xfer_df = xfer_df[stops_df.columns]
    detail_df = pd.concat([stops_df, xfer_df], ignore_index=True)
    logger.info("Total stops in details now: %d", len(detail_df))
    return detail_df


# ---------------------------------------------------------------------------
# Section 8b: Pipeline logic reference (written when running CLI or when requested)
# ---------------------------------------------------------------------------

LOGIC_REFERENCE_FILENAME = "improved_auto_approval_pipeline_logic.txt"


def build_pipeline_logic_reference_text() -> str:
    """
    Human-readable description of thresholds and conditions implemented in this module.
    Kept in sync with Section 3 constants and Phase / (01)(02)(03) functions.
    """
    pq = "power_query_od_pipeline.py / power_query_m_checks_supervisor.py"
    return f"""improved_auto_approval.py — pipeline logic and conditions
Generated from module constants and rules (M-code / PQ parity target: {pq}).

================================================================================
1) NUMERIC THRESHOLDS (module constants)
================================================================================
TRANSFER_DISTANCE_FLAG_MILES (__(01) leg segments)     = {TRANSFER_DISTANCE_FLAG_MILES}
ORIGIN_TO_BOARD_MAX_WALK_MILES (O-B check 1)           = {ORIGIN_TO_BOARD_MAX_WALK_MILES}
ORIGIN_TO_BOARD_MIN_NON_WALK_MILES (O-B check 2)       = {ORIGIN_TO_BOARD_MIN_NON_WALK_MILES}
ORIGIN_TO_BOARD_MIN_WITH_PREV_MILES (O-B check 3)      = {ORIGIN_TO_BOARD_MIN_WITH_PREV_MILES}
ALIGHTING_TO_DEST_MAX_WALK_MILES (A-D check 1)         = {ALIGHTING_TO_DEST_MAX_WALK_MILES}
ALIGHTING_TO_DEST_MIN_NON_WALK_MILES (A-D check 2)     = {ALIGHTING_TO_DEST_MIN_NON_WALK_MILES}
ALIGHTING_TO_DEST_MIN_WITH_NEXT_MILES (A-D check 3)    = {ALIGHTING_TO_DEST_MIN_WITH_NEXT_MILES}
ORIGIN_TO_DEST_MIN_VERY_CLOSE (O-D check 1)            = {ORIGIN_TO_DEST_MIN_VERY_CLOSE}
ORIGIN_TO_DEST_MIN_CLOSE (O-D check 2)                 = {ORIGIN_TO_DEST_MIN_CLOSE}
ORIGIN_TO_DEST_MAX_FAR (O-D check 3)                   = {ORIGIN_TO_DEST_MAX_FAR}
B2A_OD_RATIO_MAX (B-A check 1)                         = {B2A_OD_RATIO_MAX}
B2A_OD_RATIO_MIN_SHORT_SHORT (B-A check 2 threshold)   = {B2A_OD_RATIO_MIN_SHORT_SHORT}
PREV_NEXT_TRANSFER_WALK_FLAG_MILES (VAL / walk flags)  = {PREV_NEXT_TRANSFER_WALK_FLAG_MILES}
EARTH_RADIUS_MILES (great-circle)                      = {EARTH_RADIUS_MILES}

Transport mode categories: ORIGIN_TRANSPORT / DESTIN_TRANSPORT → SHORT (walk/wheelchair),
MEDIUM (bike/scooter), LONG (drive) via _transport_mode_category.

================================================================================
2) PHASE 1 — Walk-Walk (phase1_walk_walk_evaluation)
================================================================================
Prerequisites for all groups:
  - ORIGIN_Transport_Mode and DESTIN_Transport_Mode both contain "SHORT" (_text_contains_short).
  - O2DtoFIRSTBLASTA == 0.
  - MERGED_ORIGIN / MERGED_DESTIN lat-long all non-null.

Group rules (prev = PREV_TRANSFERS[Code], nxt = NEXT_TRANSFERS[Code] as "0","1","2"):
  [No Transfers]     prev=0, nxt=0 | ORIGIN2Transfer1_Distance<=1.5, DESTIN2Transfer1<=1.5 | ORIGIN_TO_DESTIN>=0.25 | origin_near<=5, destin_near<=5
  [1 PREV]           prev=1, nxt=0 | O2T1<=1, D2T1<=1 | O2D>=0.50 | Transfer1_Distance<=0.10 | near <=5/5
  [2 PREV]           prev=2, nxt=0 | O2T1<=1, D2T1<=1 | O2D>=1 | T1<=0.10, T2<=0.10 | near <=5/5
  [1 NEXT]           prev=0, nxt=1 | O2T1<=1, D2T1<=1 | O2D>=0.50 | Transfer5_Distance<=0.10 | near <=5/5
  [2 NEXT]           prev=0, nxt=1 | O2T1<=1, D2T1<=1 | O2D>=1 | T5<=0.10, T6<=0.10 | near <=5/5
                     (M / PQ: group label [2 NEXT] but NEXT_TRANSFERS_Code_ == "1"; distances via pd.to_numeric like PQ)
  [1 PREV-1 NEXT]    prev=1, nxt=1 | O2T1<=1, D2T1<=1 | O2D>=1 | T1<=0.10, T5<=0.10 | near <=5/5

Nearby: ORIGIN_fnCountStopsOnRouteWithinDistance / DESTIN_fnCountStopsOnRouteWithinDistance (inf if missing → fail).

================================================================================
3) PHASE 2 — NOTwalk / Walk-NOTwalk (phase2_not_walk_options)
================================================================================
Base: O2DtoFIRSTBLASTA==0; merged O/D coords non-null.

_origin_access_ok: ORIGIN LONG → ORIGIN2Transfer1<=10; ORIGIN MEDIUM → <=5.
_destin_access_ok: DESTIN LONG → DESTIN2Transfer1<=10; DESTIN MEDIUM → <=5.

  NOTWalk-Walk [No Transfers]: ~origin_short & destin_short, prev=0 nxt=0 | origin_access | D2T1<=1.5 | O2D>=0.25 | near 10/5
  Walk-NOTWalk [No Transfers]: origin_short & ~destin_short, 0/0 | O2T1<=1.5 | destin_access | O2D>=0.25 | near 5/10

  NOTWalk-Walk [1/2 PREV]: ~origin_short & destin_short | origin_access | D2T1<=1 | O2D>=0.50 | T1<=0.10 (+T2 for 2 PREV) | near 10/5
  NOTWalk-Walk [1 NEXT]: prev=0 nxt=1 | … | T5<=0.10 | near 10/5
  NOTWalk-Walk [2 NEXT]: prev=0 nxt=2 | origin_access | D2T1<=1 | O2D>=0.50 | T5<=0.10, T6<=0.10 | near 10/5
                     (M / PQ: NEXT_TRANSFERS_Code_ == "2"; distances via pd.to_numeric like PQ)

  Walk-NOTWalk [1/2 PREV]: origin_short & ~destin_short | destin_access | O2T1<=1 | O2D>=0.50 | T legs | near 5/10
  Walk-NOTWalk [1 NEXT]: nxt=1 | T5<=0.10 | near 5/10
  Walk-NOTWalk [2 NEXT]: nxt=1 | T5<=0.10, T6<=0.10 | near 5/10  (aligned with PQ; NOTWalk-Walk [2 NEXT] uses nxt=2)

================================================================================
4) PHASE-ALL
================================================================================
Concat Phase1 + Phase2, drop_duplicates(id) keep first, sort by id.

================================================================================
5) __(01) OD [DISTANCE TRANSFERS CHECK] (distance_transfer_check)
================================================================================
Input: phase_all ids merged with OG (+ optional xfer_list from details STOPS/XFER_STOPS).
  - Transfer1..8_Distance: inter-stop transfer legs (haversine); TRANSFER FLAG DISTANCE = 1 if any > {TRANSFER_DISTANCE_FLAG_MILES}.
  - Transfer*_onroute_Distance: within-leg off→on; TRANSFER_onroute_FLAG = 1 if any computed leg distance == 0.
  - #ofTransferGPS: non-null count over M GPS_COUNT_COLS (or coord fallback) / 4.
  - #ofTranfers: PREV[Code]+NEXT[Code] minus count of non-null AGNECY_TRANSFERS-1..8 when xfer_list merges applied.
  - # OF TRANSFER POINT CHECK = 1 if (#ofTransferGPS != #ofTranfers).
  - DISTANCE TRANSFER CHECK = onroute_flag + transfer_flag_distance + point_check. Keep rows where sum == 0.

================================================================================
6) __(02) OD [TRANSFERS CHECK] (transfers_check)
================================================================================
  - PREV_TRANSFER CHECK: non-null count among TRIP_FIRST..FOURTH_ROUTE[Code] (_nz) must equal int(PREV_TRANSFERS[Code]).
  - NEXT_TRANSFER CHECK: same for TRIP_NEXT, AFTER, 3RD, LAST4TH vs NEXT_TRANSFERS[Code].
  - Duplicate Transfers: among ROUTE_SURVEYED[Code] + all TRIP_*[Code], distinct count < non-null count → flag.
  - OD [TRANSFERS CHECK] FLAGS = PREV_CHECK + NEXT_CHECK + Duplicate. Keep rows where FLAGS != 1.

================================================================================
7) __(03) OD [TRIP DISTANCE CHECK] (trip_distance_check)
================================================================================
Merge clean + optional OG columns (FINAL_DIRECTION, VAL_DIST_*).
Distances: ORIGIN_TO_BOARD (transfer-aware), BOARDING_TO_ALIGHTING, ORIGIN_TO_DESTINATION, ALIGHTING_TO_DESTINATION (transfer-aware).
Ratios: O2B/O2D, B2A/OD, A2D/OD via direct division by ORIGIN_TO_DESTINATION (M-style).

Main flag bits (then error-map fillna where applicable):
  O-B_Dist_Check1..3, A-D_Dist_Check1..3, O-D_Dist_Check1..3, B-A_Dist_Check1..2, WheelchairAccessEgress
PREV/LAST transfer walk flags from VAL distances and ORIGIN_TRANSPORT/DESTIN_TRANSPORT (Walk/Wheelchair/Skateboard) vs {PREV_NEXT_TRANSFER_WALK_FLAG_MILES} mi.

OD [DISTANCE CHECK] FLAGS = sum of the twelve checks above (not the PREV/LAST transfer sub-flags).
OD [DISTANCE CHECK] FLAGS (TRANSFERS) = PREV_TRANSFER_DIST_FLAG + LAST_TRANSFER_DIST_FLAG (informational).
Keep rows where OD [DISTANCE CHECK] FLAGS != 1.

================================================================================
8) XFER LIST (details workbook)
================================================================================
Details Excel worksheet names (exact, case-sensitive): 'STOPS', 'XFER_STOPS'.
If both exist, rows are combined for xfer_list and (with DEFAULT_DETAILS_STOPS_XFER) for nearby-stop details.
If only one worksheet exists, that sheet alone is used.
XFER_STOPS column renames applied per XFER_STOPS_COLUMN_RENAME. Route key: ETC_ROUTE_ID or XFER_ROUTE_ID.
If agency column populated → build_xfer_list (PQ). Else dedupe by ETC_ROUTE_ID + OTH row for __(01) merges.

================================================================================
9) FINAL OUTPUT (full record path)
================================================================================
After step03: approved ids drive auto-Use (Tosia) unless OD Supervisor View Only applies.
Remove/Use rules also include: Field Approved (ElvisStatus), test INTERV_INIT=999, HAVE_5_MIN, Origin=Destination=Home —
see final_auto_approval and related helpers in this file.

================================================================================
END OF REFERENCE
================================================================================
"""


def write_pipeline_logic_reference_file(path: str) -> None:
    """Write build_pipeline_logic_reference_text() to UTF-8 text file."""
    stamp = f"Written at: {datetime.now().isoformat(timespec='seconds')}\n" + ("=" * 80) + "\n\n"
    text = stamp + build_pipeline_logic_reference_text()
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


# ---------------------------------------------------------------------------
# Section 9: Command Line Interface
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Improved auto-approval pipeline (M code logic)")
    parser.add_argument("input", nargs="?", default=None, help="Input CSV or Excel (optional if defaults set)")
    parser.add_argument("-o", "--output", default=None, help="Output CSV path")
    parser.add_argument("--mapping", dest="mapping_file", default=None, help="Header mapping Excel (Headers-ls6 -> FormattedHeader-ls2)")
    parser.add_argument(
        "--details",
        dest="details_file",
        default=None,
        help="Details Excel; stop/xfer data uses sheets named exactly STOPS and/or XFER_STOPS",
    )
    parser.add_argument("--skip-transfer-suggestions", action="store_true", help="Do not run transfer suggestions")
    parser.add_argument(
        "--suggestion-ids-file",
        default=None,
        help="Text file of record ids (one per line). When set, SUGGESTED_* transfer routes are generated only for these ids.",
    )
    parser.add_argument("--stops", default=None, help="Stops file path for transfer suggestions")
    parser.add_argument("--minimal", action="store_true", help="Output only ___auto-approved table (id, GROUP, Supervisor View Only)")
    parser.add_argument(
        "--debug-supervisor-flags",
        action="store_true",
        help="(minimal output only) Add OD [Supervisor Only Checks] component flags to output for validation",
    )
    parser.add_argument(
        "--debug-approval-ids",
        type=str,
        default=None,
        metavar="ID1,ID2,...",
        help="Log why these record IDs were approved (e.g. 7001,5680,15749) for debugging boarding=alighting cases",
    )
    parser.add_argument(
        "--debug-supervisor-ids",
        type=str,
        default=None,
        metavar="ID1,ID2,...",
        help="Log why these record IDs became Supervisor Only (failed (01)(02)(03) after passing Phase-All)",
    )
    parser.add_argument(
        "--debug-transfers",
        action="store_true",
        help="Log PREV/NEXT transfer code distribution and pipeline funnel to debug why only 0-transfer records are approved",
    )
    parser.add_argument(
        "--logic-txt",
        dest="logic_txt",
        default=None,
        metavar="PATH",
        help="Write all pipeline logics/conditions to this UTF-8 text file (default: %s next to -o or cwd)"
        % LOGIC_REFERENCE_FILENAME,
    )
    parser.add_argument(
        "--no-logic-txt",
        action="store_true",
        help="Do not write the pipeline logic reference text file",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_path = args.input or DEFAULT_INPUT
    output_path = args.output if args.output is not None else DEFAULT_OUTPUT
    mapping_file = args.mapping_file if args.mapping_file is not None else DEFAULT_MAPPING_FILE
    mapping_sheet = DEFAULT_MAPPING_SHEET
    details_file = args.details_file if args.details_file is not None else DEFAULT_DETAILS_FILE
    stops_path = args.stops if args.stops is not None else DEFAULT_STOPS_PATH
    skip_transfer_suggestions = args.skip_transfer_suggestions or DEFAULT_SKIP_TRANSFER_SUGGESTIONS
    suggestion_record_ids = None
    suggestion_ids_file = getattr(args, "suggestion_ids_file", None)
    if suggestion_ids_file and os.path.isfile(suggestion_ids_file):
        with open(suggestion_ids_file, "r", encoding="utf-8") as handle:
            suggestion_record_ids = {
                line.strip() for line in handle if line.strip() and not line.strip().startswith("#")
            }
        print("Suggestion ids file: %d id(s) eligible for SUGGESTED_* generation" % len(suggestion_record_ids))
    minimal = args.minimal or DEFAULT_MINIMAL_OUTPUT
    debug_approval_ids = None
    if getattr(args, "debug_approval_ids", None) and str(args.debug_approval_ids).strip():
        debug_approval_ids = [x.strip() for x in str(args.debug_approval_ids).split(",") if x.strip()]
    debug_supervisor_ids = None
    if getattr(args, "debug_supervisor_ids", None) and str(args.debug_supervisor_ids).strip():
        debug_supervisor_ids = [x.strip() for x in str(args.debug_supervisor_ids).split(",") if x.strip()]
    use_db_load = DEFAULT_USE_DB_LOAD
    details_stops_xfer = DEFAULT_DETAILS_STOPS_XFER

    if not input_path and not use_db_load:
        print("Usage: python improved_auto_approval.py [input.csv] [-o output.csv] [--mapping ...] [--details ...]")
        print("Or set DEFAULT_INPUT / DEFAULT_USE_DB_LOAD at the top of this script.")
        sys.exit(1)

    # Match auto_and_suggestions_more_transfers.py default output naming
    if (output_path is None or str(output_path).strip() == "") and not minimal:
        from datetime import date as _date
        today_date = "".join(str(_date.today()).split("-"))
        output_path = f"{DEFAULT_NAME_KE}_KINGElvis_auto_approval_{today_date}.csv"

    # Input: from DB (with CSV cache) or from file
    if use_db_load:
        HOST = os.getenv("HOST")
        USER = os.getenv("USER")
        PASSWORD = os.getenv("PASSWORD")
        DATABASE = os.getenv("DATABASE")
        select_query = DEFAULT_SELECT_QUERY
        db_name = DEFAULT_DB_NAME
        csv_filename = select_query.split()[-1].strip() + ".csv"
        try:
            input_data = _load_input_from_db_or_csv(
                select_query,
                csv_filename=csv_filename,
                db_host=HOST,
                db_user=USER,
                db_password=PASSWORD,
                db_name=db_name,
            )
        except Exception as e:
            print("DB/CSV load failed: %s" % e)
            sys.exit(1)
    else:
        if not os.path.isfile(input_path):
            print("Input file not found: %s" % input_path)
            print("Edit DEFAULT_INPUT in the script or pass input as first argument.")
            sys.exit(1)
        input_data = input_path

    # Details: optional STOPS + XFER_STOPS combined DataFrame
    details_df = None
    if details_file and os.path.isfile(details_file) and details_stops_xfer:
        try:
            details_df = load_details_stops_xfer(details_file)
            print("Total stops in details now: %d" % len(details_df))
        except Exception as e:
            logger.warning("load_details_stops_xfer failed: %s", e)

    logic_ref_path: Optional[str] = None
    if not args.no_logic_txt:
        if args.logic_txt:
            logic_ref_path = os.path.abspath(args.logic_txt)
        else:
            _logic_dir = os.getcwd()
            if output_path and str(output_path).strip():
                _d = os.path.dirname(os.path.abspath(output_path))
                if _d:
                    _logic_dir = _d
            logic_ref_path = os.path.join(_logic_dir, LOGIC_REFERENCE_FILENAME)
    if logic_ref_path:
        try:
            write_pipeline_logic_reference_file(logic_ref_path)
            print("Wrote pipeline logic reference: %s" % logic_ref_path)
        except OSError as e:
            logger.warning("Could not write pipeline logic reference: %s", e)

    try:
        result = run_improved_auto_approval(
            input_data,
            run_transfer_suggestions=not skip_transfer_suggestions,
            suggestion_record_ids=suggestion_record_ids,
            stops_path=stops_path,
            output_path=output_path,
            mapping_file=mapping_file,
            details_file=details_file,
            details_df=details_df,
            mapping_sheet=mapping_sheet,
            full_output=not minimal,
            debug_supervisor_flags=args.debug_supervisor_flags,
            debug_approval_ids=debug_approval_ids,
            debug_supervisor_ids=debug_supervisor_ids,
            debug_transfers=getattr(args, "debug_transfers", False),
        )
        print("\nSummary:")
        if "Final_Usage" in result.columns:
            print("  Use:", (result["Final_Usage"] == "Use").sum())
            print("  Remove:", (result["Final_Usage"] == "Remove").sum())
            print("  Supervisor View Only:", result.get(COL_SUPERVISOR_VIEW_ONLY, pd.Series()).eq("Yes").sum())
        else:
            print("  Total rows:", len(result))
            print("  Supervisor View Only:", result.get(COL_SUPERVISOR_VIEW_ONLY, pd.Series()).eq("Yes").sum())
    except Exception as e:
        logger.exception("Pipeline failed")
        print("Error:", e)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Example usage (when imported as module)
# ---------------------------------------------------------------------------
# if __name__ != "__main__":
#     df = pd.read_csv("your_od_data.csv")
#     approved = run_improved_auto_approval(df, output_path="___auto_approved.csv")
#     print(approved.head())


if __name__ == "__main__":
    main()
