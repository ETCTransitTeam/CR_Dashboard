"""Reusable record-queue filters for the Streamlit views."""

from __future__ import annotations

import re

import pandas as pd
import streamlit as st

REVIEWER_TOSIA = "tosia"

ROUTE_CANDIDATES = ["ROUTE_SURVEYEDCode", "ROUTE_SURVEYED_CODE", "ROUTE_SURVEYED"]
INTERVIEWER_CANDIDATES = ["INTERV_INIT"]
DATE_CANDIDATES = ["DATE", "Elvis_Date", "DATE_SUBMITTED"]
USAGE_CANDIDATES = ["Final_Usage", "Final Usage", "FINAL_USAGE"]
ID_CANDIDATES = ["elvis_id", "id", "RECORD_ID"]


def _first_present(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def record_id_column(df: pd.DataFrame) -> str | None:
    return _first_present(df, ID_CANDIDATES)


def subset_records_for_display(display: pd.DataFrame, records: pd.DataFrame) -> pd.DataFrame:
    """Keep only RECORDS rows that appear in a filtered Elvis_Review display frame."""
    if records.empty or display.empty:
        return records.iloc[0:0].copy()
    id_col = record_id_column(display)
    if not id_col or "RECORD_ID" not in records.columns:
        return records.copy()
    visible_ids = set(display[id_col].astype(str))
    return records[records["RECORD_ID"].astype(str).isin(visible_ids)].copy()


def is_empty_final_usage(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "<na>"}


def norm_reviewer(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    if text in {"nan", "none", "<na>"}:
        return ""
    text = re.sub(r"\s*/\s*", "/", text)
    return re.sub(r"\s+", " ", text).strip()


def supervisor_tosia_mask(records: pd.DataFrame) -> pd.Series:
    """Blank Final Usage + FINAL_REVIEWER = Tosia (HRTVA supervisor-view tier)."""
    if records.empty:
        return pd.Series(dtype=bool)
    usage = records["FINAL_USAGE"] if "FINAL_USAGE" in records.columns else pd.Series("", index=records.index)
    reviewer = records["FINAL_REVIEWER"] if "FINAL_REVIEWER" in records.columns else pd.Series("", index=records.index)
    return usage.map(is_empty_final_usage) & reviewer.map(lambda v: norm_reviewer(v) == REVIEWER_TOSIA)


def filter_supervisor_tosia_records(records: pd.DataFrame) -> pd.DataFrame:
    if records.empty:
        return records.copy()
    return records[supervisor_tosia_mask(records)].copy()


def review_dashboard_record_ids(project: str) -> set[str]:
    """Record IDs visible on review-team pages (Combined Checks + Supervisor View)."""
    from core.data_access import load_combined_checks, load_records

    ids: set[str] = set()
    checks = load_combined_checks(project)
    if not checks.empty:
        flagged = checks[pd.to_numeric(checks["SUM_ALL_CHECKS"], errors="coerce").fillna(0) > 0]
        ids.update(flagged["RECORD_ID"].astype(str).tolist())
    records = load_records(project)
    if not records.empty:
        ids.update(filter_supervisor_tosia_records(records)["RECORD_ID"].astype(str).tolist())
    return ids


def filter_review_team_history(history: pd.DataFrame, record_ids: set[str]) -> pd.DataFrame:
    if history.empty or not record_ids:
        return history.iloc[0:0].copy()
    if "ACTOR_ROLE" not in history.columns or "RECORD_ID" not in history.columns:
        return history.iloc[0:0].copy()
    return history[
        history["RECORD_ID"].astype(str).isin(record_ids)
        & (history["ACTOR_ROLE"].astype(str).str.lower() == "review")
    ].copy()


def apply_record_filters(
    display: pd.DataFrame,
    key_prefix: str,
    include_usage: bool = True,
) -> pd.DataFrame:
    """Render route/interviewer/date/usage filters and return the filtered frame."""
    if display.empty:
        return display

    route_col = _first_present(display, ROUTE_CANDIDATES)
    interviewer_col = _first_present(display, INTERVIEWER_CANDIDATES)
    date_col = _first_present(display, DATE_CANDIDATES)
    usage_col = _first_present(display, USAGE_CANDIDATES) if include_usage else None

    cols = st.columns(4)
    filtered = display.copy()

    route_filter = []
    if route_col:
        routes = sorted(filtered[route_col].dropna().astype(str).unique())
        route_filter = cols[0].multiselect("Route", routes, key=f"{key_prefix}_route")
    interviewer_filter = []
    if interviewer_col:
        interviewers = sorted(filtered[interviewer_col].dropna().astype(str).unique())
        interviewer_filter = cols[1].multiselect("Interviewer", interviewers, key=f"{key_prefix}_interv")
    usage_filter = []
    if usage_col:
        usages = sorted(x for x in filtered[usage_col].fillna("").astype(str).unique() if x != "")
        usage_filter = cols[2].multiselect("Final_Usage", usages, key=f"{key_prefix}_usage")
    date_text = ""
    if date_col:
        date_text = cols[3].text_input(
            "Date contains",
            help="Filter rows where DATE, Elvis_Date, or DATE_SUBMITTED contains this text (e.g. 2026-06).",
            key=f"{key_prefix}_date",
        )

    if route_filter:
        filtered = filtered[filtered[route_col].astype(str).isin(route_filter)]
    if interviewer_filter:
        filtered = filtered[filtered[interviewer_col].astype(str).isin(interviewer_filter)]
    if usage_filter:
        filtered = filtered[filtered[usage_col].fillna("").astype(str).isin(usage_filter)]
    if date_text:
        filtered = filtered[filtered[date_col].astype(str).str.contains(date_text, case=False, na=False)]

    return filtered
