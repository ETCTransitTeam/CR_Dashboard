"""Shape pipeline output to match KingElvis Elvis_Review sheet layout."""

from __future__ import annotations

import pandas as pd

# Column order from HART-TAMPA_2026_KINGElvis.xlsx Elvis_Review (53 columns).
ELVIS_REVIEW_COLUMNS: list[str] = [
    "Elvis_Date",
    "elvis_id",
    "1st Cleaner",
    "Final_Usage",
    "FINAL_REVIEWER",
    "REASON FOR REMOVAL",
    "REASON FOR REMOVAL [Other]",
    "POSSIBLE ERRORS",
    "distance_flag",
    "route_match_flag",
    "id",
    "Completed",
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
    "ROUTE_STATUS",
    "Stops_Status",
    "Test_Status",
    "SUGGESTED_PREV_TRANSFER_1_ROUTE_NAME",
    "SUGGESTED_PREV_TRANSFER_2_ROUTE_NAME",
    "SUGGESTED_PREV_TRANSFER_3_ROUTE_NAME",
    "SUGGESTED_PREV_TRANSFER_4_ROUTE_NAME",
    "SUGGESTED_NEXT_TRANSFER_1_ROUTE_NAME",
    "SUGGESTED_NEXT_TRANSFER_2_ROUTE_NAME",
    "SUGGESTED_NEXT_TRANSFER_3_ROUTE_NAME",
    "SUGGESTED_NEXT_TRANSFER_4_ROUTE_NAME",
    "Traditional_Check",
    "StopListValidation_Check",
    "OD_Distance_Check",
    "Transfer_Distance_Check",
    "2X_REVIEW_CHECK",
    "2X_REVIEW_CHECK.1",
    "2x_REVIEWED_BY",
    "2x_REVIEWED_FLAG",
    "ADMIN_APPROVED",
    "SURVEY_RECOVERY",
    "SURVEY_RECOVERY_REVIEWED_BY",
    "Recovery Check",
    "2x_REVIEWED_BY.1",
    "2x_REVIEWED_FLAG.1",
    "ADMIN_APPROVED.1",
    "RECORD_INFO",
]

# improved_auto_approval uses leg-1 names without "_1_" in the middle.
SUGGESTION_RENAMES: dict[str, str] = {
    "SUGGESTED_PREV_TRANSFER_ROUTE_NAME": "SUGGESTED_PREV_TRANSFER_1_ROUTE_NAME",
    "SUGGESTED_NEXT_TRANSFER_ROUTE_NAME": "SUGGESTED_NEXT_TRANSFER_1_ROUTE_NAME",
    "PREV_TRANS_1_Suggested_Route_Name": "SUGGESTED_PREV_TRANSFER_1_ROUTE_NAME",
    "NEXT_TRANS_1_Suggested_Route_Name": "SUGGESTED_NEXT_TRANSFER_1_ROUTE_NAME",
}


def _is_nonempty(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip() != ""


# Alternate source columns when shaping to Elvis_Review layout.
ELVIS_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "ROUTE_SURVEYEDCode": ("RouteSurveyedCode", "ROUTE_SURVEYED_CODE", "RouteSurveyedCodeCode"),
    "ROUTE_SURVEYED": ("RouteSurveyed",),
    "INTERV_INIT": ("IntervInit", "Interv_Init"),
    "Completed": ("DATE_SUBMITTED", "Date_submitted", "DATE", "Elvis_Date", "Date_started"),
    "DATE_SUBMITTED": ("Date_submitted",),
}


def _coalesce_column(out: pd.DataFrame, col: str) -> pd.Series:
    """Pick the first non-empty value across canonical and alias columns."""
    candidates: list[str] = []
    if col in out.columns:
        candidates.append(col)
    for alias in ELVIS_COLUMN_ALIASES.get(col, ()):
        if alias in out.columns and alias not in candidates:
            candidates.append(alias)
    if not candidates:
        return pd.Series([""] * len(out), index=out.index)
    series = out[candidates[0]].map(_is_nonempty)
    result = out[candidates[0]].where(series, other=None)
    for alias in candidates[1:]:
        alt = out[alias].where(out[alias].map(_is_nonempty), other=None)
        result = result.combine_first(alt)
    return result.fillna("")


def shape_to_elvis_review(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with Elvis_Review column names and order."""
    out = df.copy()
    for old, new in SUGGESTION_RENAMES.items():
        if old not in out.columns:
            continue
        if new in out.columns:
            out[new] = out[new].where(out[new].map(_is_nonempty), out[old])
            out = out.drop(columns=[old])
        else:
            out = out.rename(columns={old: new})

    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated()]

    shaped: dict[str, pd.Series] = {}
    for col in ELVIS_REVIEW_COLUMNS:
        if col in ELVIS_COLUMN_ALIASES or col in out.columns:
            shaped[col] = _coalesce_column(out, col)
        else:
            shaped[col] = out[col] if col in out.columns else ""
    return pd.DataFrame(shaped)


def is_suggestion_field(key: str) -> bool:
    upper = str(key).upper()
    return upper.startswith("SUGGESTED_") or "SUGGESTED_ROUTE" in upper


def normalize_suggestion_keys(payload: dict) -> dict:
    """Apply pipeline→Elvis renames and return only non-empty suggestion fields."""
    out: dict = {}
    for key, value in payload.items():
        if not _is_nonempty(value):
            continue
        canon = SUGGESTION_RENAMES.get(key, key)
        if is_suggestion_field(canon) or is_suggestion_field(key):
            out[canon] = value
    return out


def merge_suggestion_fields(existing: dict, pipeline: dict) -> dict | None:
    """Merge pipeline suggestion fields into an existing payload without touching other edits."""
    updates = normalize_suggestion_keys(pipeline)
    if not updates:
        return None
    merged = {**existing, **updates}
    return merged if merged != existing else None


def merge_supervisor_comment_fields(existing: dict, pipeline: dict) -> dict | None:
    """Refresh ELVIS_COMMENT / SUPERVISOR_COMMENT from pipeline without overwriting other edits."""
    merged = dict(existing)
    changed = False
    for key in ("ELVIS_COMMENT", "SUPERVISOR_COMMENT"):
        value = pipeline.get(key)
        if _is_nonempty(value) and _norm(merged.get(key)) != _norm(value):
            merged[key] = value
            changed = True
    if _is_nonempty(merged.get("ELVIS_COMMENT")) and not _is_nonempty(merged.get("SUPERVISOR_COMMENT")):
        merged["SUPERVISOR_COMMENT"] = merged["ELVIS_COMMENT"]
        changed = True
    return merged if changed else None


def _norm(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def has_transfer_suggestions(payload: dict) -> bool:
    for key, value in payload.items():
        if is_suggestion_field(key) and _is_nonempty(value):
            return True
    return bool(normalize_suggestion_keys(payload))


SUGGESTION_DISPLAY_COLUMNS = [
    "SUGGESTED_PREV_TRANSFER_1_ROUTE_NAME",
    "SUGGESTED_NEXT_TRANSFER_1_ROUTE_NAME",
]


def enrich_suggestion_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure suggestion route columns exist and are easy to scan in the queue table."""
    out = df.copy()
    leg1_fallbacks = {
        "SUGGESTED_PREV_TRANSFER_1_ROUTE_NAME": "SUGGESTED_PREV_TRANSFER_ROUTE_NAME",
        "SUGGESTED_NEXT_TRANSFER_1_ROUTE_NAME": "SUGGESTED_NEXT_TRANSFER_ROUTE_NAME",
    }
    for col, fallback in leg1_fallbacks.items():
        if fallback in out.columns:
            if col not in out.columns:
                out[col] = out[fallback]
            else:
                out[col] = out[col].where(out[col].map(_is_nonempty), out.get(fallback))
    for col in SUGGESTION_DISPLAY_COLUMNS:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def suggestion_route_value(payload: dict, leg: int, direction: str) -> str:
    """Read prev/next suggested route name for a leg (handles pipeline vs Elvis column names)."""
    if leg == 1:
        keys = (
            f"SUGGESTED_{direction}_TRANSFER_1_ROUTE_NAME",
            f"SUGGESTED_{direction}_TRANSFER_ROUTE_NAME",
        )
    else:
        keys = (f"SUGGESTED_{direction}_TRANSFER_{leg}_ROUTE_NAME",)
    for key in keys:
        value = payload.get(key)
        if _is_nonempty(value):
            return str(value).strip()
    return ""
