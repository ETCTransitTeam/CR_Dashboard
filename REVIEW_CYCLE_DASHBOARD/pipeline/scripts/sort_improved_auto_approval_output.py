# -*- coding: utf-8 -*-
"""
sort_improved_auto_approval_output.py
======================================
Standalone utility: reads a table (CSV from improved_auto_approval.py or an Excel
workbook such as ``INDYGO_BRT_2026_KINGElvis.xlsx`` sheet ``Elvis_Review``) and
reorders rows for review / reporting.

Sorting priority (lower sort key = earlier in output):
  0 - Final Usage = 'Use' AND Final Reviewer = 'Tosia'                     (auto-approved Use)
  1 - Final Usage empty/blank AND Final Reviewer = 'Tosia'                  (supervisor-only style)
  2 - Final Usage = 'Use' AND Final Reviewer = 'Field Approved'            (field-approved Use)
  3 - Final Usage = 'Remove' AND Final Reviewer = 'Tosia' OR 'Jason'       (Remove with Tosia or Jason)
  4 - Final Usage = 'Remove' AND Final Reviewer = 'Test/No 5 MIN'           (test / no-5-min removes)
  5 - Final Usage is 'Use' OR 'Remove' AND reviewer is neither 'Tosia'
      nor 'Test/No 5 MIN'                                                   (e.g. Auto-Remove, Remove + Field Approved;
      'Use' + 'Field Approved' is tier 2; 'Remove' + Tosia/Jason is tier 3 — by evaluation order)
  6 - Final Usage empty AND Final Reviewer empty                            (unset both)
  7 - All other combinations (catch-all, last), e.g. blank usage with a non-Tosia reviewer.

Rules are evaluated in order listed above; the first match wins (catch-all tier 7 last). Empty values treat '', NaN, None,
and whitespace-only strings as empty. String compares for Use/Remove/Tosia/reviewer labels
are case-insensitive after strip (so 'use' matches 'Use').

Does not modify improved_auto_approval.py; all columns and values are preserved.

Usage:
  python sort_improved_auto_approval_output.py
  python sort_improved_auto_approval_output.py -i path/to/file.csv -o out.csv
  python sort_improved_auto_approval_output.py -i book.xlsx --sheet Elvis_Review
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Optional, Tuple

import pandas as pd

# Default input if you prefer editing a variable instead of CLI (--input overrides this when passed).
DEFAULT_INPUT_PATH = "INDYGO_BRT_2026_KINGElvis.xlsx"
# Worksheet used when the input path is Excel (.xlsx / .xls / .xlsm); ignored for CSV.
DEFAULT_EXCEL_SHEET = "Elvis_Review"

# Canonical labels (comparison is case-insensitive after strip).
LABEL_USE = "use"
LABEL_REMOVE = "remove"
REVIEWER_TOSIA = "tosia"
REVIEWER_TEST_NO5 = "test/no 5 min"  # normalized form (see _norm_reviewer)
REVIEWER_FIELD_APPROVED = "field approved"  # normalized; matches "Field Approved"
REVIEWER_JASON = "jason"
# Remove + reviewer in this set sorts before Test/No 5 MIN removes.
REMOVE_REVIEWER_TOSIA_OR_JASON = frozenset({REVIEWER_TOSIA, REVIEWER_JASON})


def _alnum_lower(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def _find_column(df: pd.DataFrame, semantic: str) -> Optional[str]:
    """
    Map semantic 'final_usage' or 'final_reviewer' to an actual column name in df.
    Matches Final_Usage, Final Usage, FINAL_USAGE, etc. by normalized key.
    """
    target = _alnum_lower(semantic)
    preferred: tuple[str, ...] = ()
    if target == "finalusage":
        preferred = ("Final_Usage", "FINAL_USAGE", "Final Usage")
    elif target == "finalreviewer":
        preferred = ("FINAL_REVIEWER", "Final Reviewer", "Final_Reviewer")
    for name in preferred:
        if name in df.columns:
            return name
    for col in df.columns:
        if _alnum_lower(col) == target:
            return col
    return None


def _cell_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if s.lower() in ("nan", "none", "<na>"):
        return ""
    return s


def _usage_empty(s: str) -> bool:
    return _cell_str(s) == ""


def _norm_usage(s: str) -> str:
    return _cell_str(s).lower()


def _norm_reviewer(s: str) -> str:
    # Collapse spaces around slash for stable match with 'Test/No 5 MIN'
    t = _cell_str(s).lower()
    t = re.sub(r"\s*/\s*", "/", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _priority_tier(usage_raw, reviewer_raw) -> int:
    """
    Return integer tier 0..7 (0 = highest priority in output).
    """
    u = _norm_usage(usage_raw)
    r = _norm_reviewer(reviewer_raw)
    u_empty = _usage_empty(usage_raw)
    r_empty = _cell_str(reviewer_raw) == ""

    # Tier 0: Use + Tosia
    if u == LABEL_USE and r == REVIEWER_TOSIA:
        return 0
    # Tier 1: blank usage + Tosia
    if u_empty and r == REVIEWER_TOSIA:
        return 1
    # Tier 2: Use + Field Approved
    if u == LABEL_USE and r == REVIEWER_FIELD_APPROVED:
        return 2
    # Tier 3: Remove + Tosia or Jason
    if u == LABEL_REMOVE and r in REMOVE_REVIEWER_TOSIA_OR_JASON:
        return 3
    # Tier 4: Remove + Test/No 5 MIN
    if u == LABEL_REMOVE and r == REVIEWER_TEST_NO5:
        return 4
    # Tier 5: Use or Remove + reviewer not Tosia and not Test/No 5 MIN
    if u in (LABEL_USE, LABEL_REMOVE) and r not in (REVIEWER_TOSIA, REVIEWER_TEST_NO5) and not r_empty:
        return 5
    if u in (LABEL_USE, LABEL_REMOVE) and r not in (REVIEWER_TOSIA, REVIEWER_TEST_NO5) and r_empty:
        # Use/Remove with empty reviewer — not covered by spec; treat as catch-all
        return 7
    # Tier 6: both empty
    if u_empty and r_empty:
        return 6
    return 7


def _resolve_columns(df: pd.DataFrame) -> Tuple[str, str]:
    u_col = _find_column(df, "final_usage")
    r_col = _find_column(df, "final_reviewer")
    if u_col is None or r_col is None:
        print("Could not resolve required columns.", file=sys.stderr)
        print("Columns in file:", list(df.columns), file=sys.stderr)
        missing = []
        if u_col is None:
            missing.append("Final Usage (expected patterns: Final_Usage, Final Usage, …)")
        if r_col is None:
            missing.append("Final Reviewer (expected patterns: FINAL_REVIEWER, Final Reviewer, …)")
        raise SystemExit(
            "Missing: " + "; ".join(missing) + "\n"
            "Normalize names by removing spaces/underscores/punctuation for matching."
        )
    return u_col, r_col


def sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    u_col, r_col = _resolve_columns(df)
    orig_idx = pd.Series(range(len(df)), index=df.index, dtype="int64")
    tmp = df.copy()
    tmp["_sort_tier"] = [_priority_tier(r[u_col], r[r_col]) for _, r in tmp.iterrows()]
    tmp["_orig_order"] = orig_idx.values
    tmp = tmp.sort_values(by=["_sort_tier", "_orig_order"], kind="mergesort")
    tmp = tmp.drop(columns=["_sort_tier", "_orig_order"])
    return tmp.reset_index(drop=True)


def default_output_path(input_path: str) -> str:
    directory, base = os.path.split(os.path.abspath(input_path))
    name, ext = os.path.splitext(base)
    if ext.lower() != ".csv":
        ext = ".csv"
    out_name = f"sorted_{name}{ext}"
    return os.path.join(directory, out_name)


def _is_excel_path(path: str) -> bool:
    return os.path.splitext(path)[1].lower() in (".xlsx", ".xlsm", ".xls")


def load_input_table(path: str, excel_sheet: Optional[str]) -> pd.DataFrame:
    """Load CSV or Excel; for Excel, ``excel_sheet`` defaults to DEFAULT_EXCEL_SHEET."""
    if _is_excel_path(path):
        sheet = excel_sheet if excel_sheet is not None else DEFAULT_EXCEL_SHEET
        return pd.read_excel(path, sheet_name=sheet, dtype=object)
    return pd.read_csv(path, dtype=object, low_memory=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sort rows by Final_Usage / FINAL_REVIEWER tiers (CSV or Excel input)."
    )
    parser.add_argument(
        "-i",
        "--input",
        dest="input_path",
        default=None,
        help="Path to input CSV or Excel workbook",
    )
    parser.add_argument(
        "--sheet",
        dest="excel_sheet",
        default=None,
        help=f"Excel worksheet name (default: {DEFAULT_EXCEL_SHEET!r}). Ignored for CSV.",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output_path",
        default=None,
        help="Path for sorted CSV (default: sorted_<input_basename>.csv next to input)",
    )
    args = parser.parse_args()

    inp = args.input_path or (DEFAULT_INPUT_PATH.strip() or None)
    if not inp:
        parser.error("Provide --input PATH or set DEFAULT_INPUT_PATH at top of script.")

    if not os.path.isfile(inp):
        raise SystemExit(f"Input file not found: {inp}")

    df = load_input_table(inp, args.excel_sheet)
    out_path = args.output_path or default_output_path(inp)

    if _is_excel_path(inp):
        _sn = args.excel_sheet if args.excel_sheet is not None else DEFAULT_EXCEL_SHEET
        print(f"Excel sheet: {_sn!r}")

    # Validate / announce resolved columns
    u_col, r_col = _resolve_columns(df)
    print(f"Using columns: Final Usage -> {u_col!r}, Final Reviewer -> {r_col!r}")
    print(f"Rows: {len(df)}")

    sorted_df = sort_dataframe(df)
    sorted_df.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
