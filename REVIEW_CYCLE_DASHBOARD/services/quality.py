"""Data quality service: demographic anomaly checks and quality alerts.

Demographic results land in DEMOGRAPHIC_CHECKS; aggregate alerts land in
QUALITY_ALERTS. Both are derived; re-running replaces the project's rows.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA, WORKSPACE_DIR
from core.data_access import load_records, records_to_dataframe
from core.snowflake_conn import append_rows, execute, fetch_df, fetch_df_optional, merge_upsert
from services import analytics

# HRTVA od_demographics_checks.py flag columns (value 1 = fail).
HRTVA_DEMO_FLAG_COLUMNS = [
    "student_od_flag",
    "workplace_od_flag",
    "OLD_K12_STUDENT",
    "YOUNG_OLD_COLLEGE_STUDENT",
    "EMPLOYED_IN_HH_1",
    "EMPLOYED_IN_HH_2",
    "EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE",
    "TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE",
    "COUNT_VH_HH_FLAG",
    "YOUNG_DRIVER",
    "FARE_STUDENT_FLAG",
    "FARE_EMPLOYMENT_FLAG",
]

DEMOGRAPHICS_STABLE_CSV = "latest_DemoGraphic_Checks(01).csv"


def demographics_output_filename(project_name: str) -> str:
    """Script CSV name: {prefix}_DemoGraphic_Checks(01).csv."""
    from pipeline.runner import _demographics_output_prefix, build_context

    ctx = build_context(project_name)
    prefix = _demographics_output_prefix(ctx)
    return f"{prefix}_DemoGraphic_Checks(01).csv"


def resolve_demographics_output_csv(project_name: str) -> Path | None:
    """Latest od_demographics_checks.py CSV for a project."""
    stable = WORKSPACE_DIR / project_name / DEMOGRAPHICS_STABLE_CSV
    if stable.exists():
        return stable

    try:
        expected = demographics_output_filename(project_name)
    except ValueError:
        expected = None

    project_dir = WORKSPACE_DIR / project_name
    if not project_dir.is_dir():
        return None
    for day_dir in sorted(project_dir.iterdir(), reverse=True):
        if not day_dir.is_dir():
            continue
        if expected:
            candidate = day_dir / expected
            if candidate.exists():
                return candidate
        matches = sorted(day_dir.glob("*_DemoGraphic_Checks(01).csv"))
        if matches:
            return matches[0]
    return None


def load_demographics_script_output(project_name: str) -> pd.DataFrame:
    """Raw flagged rows from od_demographics_checks.py (SUM_ALL_CHECKS = 1)."""
    path = resolve_demographics_output_csv(project_name)
    if path is None:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def _persist_demographics_csv(project_name: str, csv_path: Path) -> Path:
    dest = WORKSPACE_DIR / project_name / DEMOGRAPHICS_STABLE_CSV
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(csv_path, dest)
    return dest


# Default alert thresholds (percent).
ROUTE_REMOVAL_THRESHOLD = 15.0
INTERVIEWER_REMOVAL_THRESHOLD = 15.0
REVIEWER_REMOVAL_THRESHOLD = 15.0
CLEANER_REMOVAL_THRESHOLD = 15.0
REVIEWER_OVERRIDE_THRESHOLD = 50.0
REVIEWER_MIN_CHANGES = 10
MIN_GROUP_SIZE = 5


def _find_col(columns: list[str], *candidates: str) -> str | None:
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    for cand in candidates:
        for low, original in lowered.items():
            if cand.lower() in low:
                return original
    return None


def _to_number(value: Any) -> float | None:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return float(str(value).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def run_demographic_checks(project_name: str) -> pd.DataFrame:
    """Scan records for demographic anomalies and persist per-record results."""
    records = load_records(project_name)
    if records.empty:
        return pd.DataFrame()
    payloads = records_to_dataframe(records)
    columns = list(payloads.columns)
    record_id_col = "elvis_id" if "elvis_id" in columns else ("id" if "id" in columns else None)
    if record_id_col is None:
        return pd.DataFrame()

    age_col = _find_col(columns, "AGE")
    income_col = _find_col(columns, "INCOME", "HOUSEHOLD_INCOME")
    race_col = _find_col(columns, "RACE", "ETHNICITY")

    rows: list[dict[str, Any]] = []
    for _, row in payloads.iterrows():
        record_id = str(row.get(record_id_col) or "").strip()
        if not record_id:
            continue

        if age_col:
            age = _to_number(row.get(age_col))
            if age is None:
                rows.append(_chk(project_name, record_id, "age", "missing", "Age missing or non-numeric"))
            elif age < 5 or age > 100:
                rows.append(_chk(project_name, record_id, "age", "fail", f"Age out of range: {age:g}"))
            else:
                rows.append(_chk(project_name, record_id, "age", "pass", ""))

        if income_col:
            income = _to_number(row.get(income_col))
            if income is None:
                rows.append(_chk(project_name, record_id, "income", "missing", "Income missing"))
            elif income < 0 or income > 1_000_000:
                rows.append(_chk(project_name, record_id, "income", "fail", f"Income out of range: {income:g}"))
            else:
                rows.append(_chk(project_name, record_id, "income", "pass", ""))

        if race_col:
            race = str(row.get(race_col) or "").strip()
            status = "missing" if not race else "pass"
            detail = "Race/ethnicity missing" if not race else ""
            rows.append(_chk(project_name, record_id, "race", status, detail))

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    execute(
        f"DELETE FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_CHECKS WHERE PROJECT_NAME = %s",
        (project_name,),
    )
    merge_upsert(
        result,
        "DEMOGRAPHIC_CHECKS",
        key_columns=["PROJECT_NAME", "RECORD_ID", "CHECK_TYPE"],
    )
    return result


def ingest_demographics_from_csv(project_name: str, csv_path: str | Path) -> pd.DataFrame:
    """Load HRTVA demographics CSV (flagged rows only) into DEMOGRAPHIC_CHECKS."""
    path = Path(csv_path)
    if not path.exists():
        return pd.DataFrame()
    _persist_demographics_csv(project_name, path)
    df = pd.read_csv(path, low_memory=False)
    if df.empty:
        return df
    id_col = "id" if "id" in df.columns else ("elvis_id" if "elvis_id" in df.columns else None)
    if id_col is None:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        record_id = str(row.get(id_col) or "").strip()
        if not record_id:
            continue
        for flag in HRTVA_DEMO_FLAG_COLUMNS:
            if flag not in df.columns:
                continue
            val = row.get(flag)
            try:
                is_flagged = int(val) == 1
            except (TypeError, ValueError):
                is_flagged = str(val).strip() in ("1", "True", "true")
            if is_flagged:
                rows.append(_chk(project_name, record_id, flag, "fail", f"HRTVA demographic flag: {flag}"))

    result = pd.DataFrame(rows)
    execute(
        f"DELETE FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_CHECKS WHERE PROJECT_NAME = %s",
        (project_name,),
    )
    if not result.empty:
        merge_upsert(
            result,
            "DEMOGRAPHIC_CHECKS",
            key_columns=["PROJECT_NAME", "RECORD_ID", "CHECK_TYPE"],
        )
    return result


def demographics_data_fingerprint(project_name: str) -> str:
    """Change token so demographic checks refresh when Elvis Review records change."""
    records = load_records(project_name)
    if records.empty:
        return "empty"
    max_updated = records["UPDATED_AT"].max()
    return f"{len(records)}_{max_updated}"


def generate_demographic_checks_from_review(project_name: str) -> pd.DataFrame:
    """Evaluate configured demographic rules and persist failed checks."""
    from services import demographic_rules

    return demographic_rules.evaluate_project_rules(project_name, persist=True)


def run_hrtva_demographic_checks(project_name: str) -> pd.DataFrame:
    """Run od_demographics_checks.py pipeline and ingest flagged rows."""
    return generate_demographic_checks_from_review(project_name)


def _chk(project_name: str, record_id: str, check_type: str, status: str, detail: str) -> dict[str, Any]:
    return {
        "PROJECT_NAME": project_name,
        "RECORD_ID": record_id,
        "CHECK_TYPE": check_type,
        "STATUS": status,
        "DETAIL": detail[:4000],
    }


def load_demographic_checks(project_name: str, status: str | None = None) -> pd.DataFrame:
    from services import demographic_rules

    return demographic_rules.load_demographic_results(project_name, status=status)


def compute_quality_alerts(project_name: str) -> pd.DataFrame:
    """Recompute aggregate data-quality alerts for a project."""
    alerts: list[dict[str, Any]] = []

    routes = analytics.removal_by(project_name, "ROUTE_SURVEYED_CODE")
    for _, r in routes.iterrows():
        if r["REMOVAL_RATE"] >= ROUTE_REMOVAL_THRESHOLD and r["TOTAL"] >= MIN_GROUP_SIZE:
            alerts.append(
                _alert(
                    project_name,
                    "high_route_removal",
                    str(r["ROUTE_SURVEYED_CODE"]),
                    r["REMOVAL_RATE"],
                    ROUTE_REMOVAL_THRESHOLD,
                    f"Route {r['ROUTE_SURVEYED_CODE']} removal {r['REMOVAL_RATE']}% ({r['REMOVED']}/{r['TOTAL']})",
                )
            )

    interviewers = analytics.removal_by(project_name, "INTERV_INIT")
    for _, r in interviewers.iterrows():
        if r["REMOVAL_RATE"] >= INTERVIEWER_REMOVAL_THRESHOLD and r["TOTAL"] >= MIN_GROUP_SIZE:
            alerts.append(
                _alert(
                    project_name,
                    "high_interviewer_error",
                    str(r["INTERV_INIT"]),
                    r["REMOVAL_RATE"],
                    INTERVIEWER_REMOVAL_THRESHOLD,
                    f"Interviewer {r['INTERV_INIT']} removal {r['REMOVAL_RATE']}% ({r['REMOVED']}/{r['TOTAL']})",
                )
            )

    reviewers = analytics.removal_by(project_name, "FINAL_REVIEWER")
    for _, r in reviewers.iterrows():
        reviewer = str(r["FINAL_REVIEWER"]).strip()
        if not reviewer or reviewer == "(blank)":
            continue
        if r["REMOVAL_RATE"] >= REVIEWER_REMOVAL_THRESHOLD and r["TOTAL"] >= MIN_GROUP_SIZE:
            alerts.append(
                _alert(
                    project_name,
                    "high_reviewer_removal",
                    reviewer,
                    r["REMOVAL_RATE"],
                    REVIEWER_REMOVAL_THRESHOLD,
                    f"Reviewer {reviewer} removal {r['REMOVAL_RATE']}% ({r['REMOVED']}/{r['TOTAL']})",
                )
            )

    cleaners = analytics.removal_by(project_name, "FIRST_CLEANER")
    for _, r in cleaners.iterrows():
        cleaner = str(r["FIRST_CLEANER"]).strip()
        if not cleaner or cleaner == "(blank)":
            continue
        if r["REMOVAL_RATE"] >= CLEANER_REMOVAL_THRESHOLD and r["TOTAL"] >= MIN_GROUP_SIZE:
            alerts.append(
                _alert(
                    project_name,
                    "high_cleaner_removal",
                    cleaner,
                    r["REMOVAL_RATE"],
                    CLEANER_REMOVAL_THRESHOLD,
                    f"Cleaner {cleaner} removal {r['REMOVAL_RATE']}% ({r['REMOVED']}/{r['TOTAL']})",
                )
            )

    override_reviewers = analytics.reviewer_override_rate(project_name)
    for _, r in override_reviewers.iterrows():
        if r["OVERRIDE_RATE"] >= REVIEWER_OVERRIDE_THRESHOLD and r["CHANGES"] >= REVIEWER_MIN_CHANGES:
            alerts.append(
                _alert(
                    project_name,
                    "high_reviewer_override",
                    str(r["ACTOR"]),
                    r["OVERRIDE_RATE"],
                    REVIEWER_OVERRIDE_THRESHOLD,
                    f"Reviewer {r['ACTOR']} override {r['OVERRIDE_RATE']}% over {r['CHANGES']} changes",
                )
            )

    execute(
        f"DELETE FROM {REVIEW_CYCLE_SCHEMA}.QUALITY_ALERTS WHERE PROJECT_NAME = %s",
        (project_name,),
    )
    result = pd.DataFrame(alerts)
    if not result.empty:
        append_rows(result, "QUALITY_ALERTS")
    return result


def _alert(
    project_name: str,
    alert_type: str,
    subject: str,
    metric_value: float,
    threshold: float,
    message: str,
) -> dict[str, Any]:
    severity = "high" if metric_value >= threshold * 1.5 else "medium"
    return {
        "PROJECT_NAME": project_name,
        "ALERT_TYPE": alert_type,
        "SUBJECT": subject[:512],
        "METRIC_VALUE": float(metric_value),
        "THRESHOLD": float(threshold),
        "SEVERITY": severity,
        "MESSAGE": message[:4000],
    }


def list_alerts(project_name: str) -> pd.DataFrame:
    return fetch_df_optional(
        f"SELECT * FROM {REVIEW_CYCLE_SCHEMA}.QUALITY_ALERTS WHERE PROJECT_NAME = %s ORDER BY METRIC_VALUE DESC",
        (project_name,),
    )
