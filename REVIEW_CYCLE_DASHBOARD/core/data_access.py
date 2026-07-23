from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA
from core.snowflake_conn import append_dataframe, execute, fetch_df, merge_upsert


def _json_dumps(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "{}"
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def _parse_payload(value: Any) -> dict[str, Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _record_id_from_row(row: pd.Series) -> str:
    for col in ("elvis_id", "id", "ID", "Elvis_id"):
        if col in row.index and pd.notna(row[col]):
            return str(row[col]).strip()
    return ""


def _normalize_usage(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _is_nonempty(value: Any) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip() != ""


# Snowflake typed RECORDS columns -> Elvis_Review payload keys (UI source of truth on read).
TYPED_COLUMN_MAP: dict[str, str] = {
    "FINAL_USAGE": "Final_Usage",
    "FINAL_REVIEWER": "FINAL_REVIEWER",
    "ROUTE_SURVEYED_CODE": "ROUTE_SURVEYEDCode",
    "INTERV_INIT": "INTERV_INIT",
    "ELVIS_STATUS": "ElvisStatus",
    "SUPERVISOR_COMMENT": "SUPERVISOR_COMMENT",
    "FIRST_CLEANER": "1st Cleaner",
    "REASON_FOR_REMOVAL": "REASON FOR REMOVAL",
}

# Elvis_Review payload keys -> alternate keys (MySQL export uses PascalCase, pipeline uses SCREAMING).
PAYLOAD_CANONICAL_ALIASES: dict[str, tuple[str, ...]] = {
    "ROUTE_SURVEYEDCode": ("ROUTE_SURVEYEDCode", "ROUTE_SURVEYED_CODE", "RouteSurveyedCode"),
    "ROUTE_SURVEYED": ("ROUTE_SURVEYED", "RouteSurveyed"),
    "INTERV_INIT": ("INTERV_INIT", "IntervInit", "Interv_Init"),
    "Final_Usage": ("Final_Usage", "FINAL_USAGE"),
    "FINAL_REVIEWER": ("FINAL_REVIEWER", "Final Reviewer"),
    "ElvisStatus": ("ElvisStatus", "ELVIS_STATUS"),
    "1st Cleaner": ("1st Cleaner", "FIRST_CLEANER"),
}


def _find_column(columns: list[str] | pd.Index, *candidates: str) -> str | None:
    lowered = {str(c).lower(): str(c) for c in columns}
    for cand in candidates:
        key = cand.lower()
        if key in lowered:
            return lowered[key]
    for cand in candidates:
        compact = cand.lower().replace("_", "")
        for low, original in lowered.items():
            if low.replace("_", "") == compact:
                return original
    return None


def _payload_get(payload: dict[str, Any], *candidates: str) -> Any:
    """First non-empty payload value across canonical and alias keys."""
    for key in candidates:
        if _is_nonempty(payload.get(key)):
            return payload.get(key)
    norm = {str(k).lower().replace("_", ""): k for k in payload}
    for key in candidates:
        nk = key.lower().replace("_", "")
        orig = norm.get(nk)
        if orig and _is_nonempty(payload.get(orig)):
            return payload.get(orig)
    return None


def _normalize_route_code(value: Any) -> str | None:
    """Route codes may be numeric (42) or alphanumeric (TAM_1_1_00); always store as string."""
    if not _is_nonempty(value):
        return None
    return str(value).strip()


def normalize_payload_aliases(payload: dict[str, Any]) -> dict[str, Any]:
    """Coalesce MySQL/pipeline field names into Elvis_Review payload keys."""
    out = dict(payload)
    for canonical, aliases in PAYLOAD_CANONICAL_ALIASES.items():
        value = _payload_get(out, canonical, *aliases)
        if value is not None:
            out[canonical] = value
    return out


def enrich_payload_from_typed_columns(row: pd.Series | dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """Merge typed Snowflake columns into payload so UI reflects latest DB state."""
    row_dict = row.to_dict() if isinstance(row, pd.Series) else row
    out = normalize_payload_aliases(dict(payload))
    for typed_col, payload_key in TYPED_COLUMN_MAP.items():
        typed_val = row_dict.get(typed_col)
        if _is_nonempty(typed_val):
            out[payload_key] = typed_val
    record_id = row_dict.get("RECORD_ID")
    if _is_nonempty(record_id):
        out.setdefault("id", record_id)
        out.setdefault("elvis_id", record_id)
    return out


def records_to_dataframe(records_df: pd.DataFrame) -> pd.DataFrame:
    if records_df.empty:
        return records_df
    payloads = []
    for _, row in records_df.iterrows():
        payload = enrich_payload_from_typed_columns(row, _parse_payload(row.get("RECORD_PAYLOAD")))
        payloads.append(payload)
    return pd.DataFrame(payloads)


def load_records(
    project_name: str | None = None,
    only_new: bool = False,
    final_usage: str | None = None,
) -> pd.DataFrame:
    from core.streamlit_cache import cached_load_records, cache_version, in_streamlit_runtime

    if in_streamlit_runtime():
        return cached_load_records(cache_version(), project_name, only_new, final_usage)
    return _load_records_uncached(project_name, only_new, final_usage)


def _load_records_uncached(
    project_name: str | None = None,
    only_new: bool = False,
    final_usage: str | None = None,
) -> pd.DataFrame:
    clauses = []
    params: list[Any] = []
    if project_name:
        clauses.append("PROJECT_NAME = %s")
        params.append(project_name)
    if only_new:
        clauses.append("IS_NEW = TRUE")
    if final_usage is not None:
        clauses.append("UPPER(FINAL_USAGE) = %s")
        params.append(final_usage.upper())
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_df(
        f"SELECT * FROM {REVIEW_CYCLE_SCHEMA}.RECORDS {where} "
        "ORDER BY PIPELINE_SORT_ORDER ASC NULLS LAST, INGESTED_AT DESC, RECORD_ID",
        tuple(params),
    )


def load_records_for_projects(
    project_names: list[str] | None = None,
    only_new: bool = False,
    final_usage: str | None = None,
) -> pd.DataFrame:
    """Load records restricted to a set of projects (SQL filter, not pandas)."""
    if not project_names:
        return load_records(only_new=only_new, final_usage=final_usage)
    from core.streamlit_cache import (
        cached_load_records_for_projects,
        cache_version,
        in_streamlit_runtime,
    )

    names_tuple = tuple(sorted(project_names))
    if in_streamlit_runtime():
        return cached_load_records_for_projects(cache_version(), names_tuple, only_new, final_usage)
    return _load_records_for_projects_uncached(project_names, only_new, final_usage)


def _load_records_for_projects_uncached(
    project_names: list[str],
    only_new: bool = False,
    final_usage: str | None = None,
) -> pd.DataFrame:
    clauses = []
    params: list[Any] = []
    if project_names:
        placeholders = ", ".join(["%s"] * len(project_names))
        clauses.append(f"PROJECT_NAME IN ({placeholders})")
        params.extend(project_names)
    if only_new:
        clauses.append("IS_NEW = TRUE")
    if final_usage is not None:
        clauses.append("UPPER(FINAL_USAGE) = %s")
        params.append(final_usage.upper())
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_df(
        f"SELECT * FROM {REVIEW_CYCLE_SCHEMA}.RECORDS {where} "
        "ORDER BY PIPELINE_SORT_ORDER ASC NULLS LAST, INGESTED_AT DESC, RECORD_ID",
        tuple(params),
    )


def load_record(project_name: str, record_id: str) -> pd.DataFrame:
    from core.streamlit_cache import cached_load_record, cache_version, in_streamlit_runtime

    if in_streamlit_runtime():
        return cached_load_record(cache_version(), project_name, record_id)
    return _load_record_uncached(project_name, record_id)


def _load_record_uncached(project_name: str, record_id: str) -> pd.DataFrame:
    return fetch_df(
        f"""
        SELECT * FROM {REVIEW_CYCLE_SCHEMA}.RECORDS
        WHERE PROJECT_NAME = %s AND RECORD_ID = %s
        """,
        (project_name, record_id),
    )


def _is_valid_route_code(value: Any) -> bool:
    if not _is_nonempty(value):
        return False
    text = str(value).strip()
    lowered = text.lower()
    if lowered in {"routesurveyed", "routesurveyedcode", "nan", "none"}:
        return False
    if "select the route" in lowered or "click to select" in lowered:
        return False
    return True


def resolve_elvis_export_path(project_name: str) -> Path | None:
    """Latest staged MySQL Elvis CSV for a project (pipeline workspace)."""
    from core.config import WORKSPACE_DIR
    from core.projects import get_project

    project = get_project(project_name)
    if not project:
        return None
    elvis_table = project.get("ELVIS_TABLE") or ""
    csv_name = f"{elvis_table}.csv" if elvis_table else "elvis_transit_ls6_export_odbc.csv"
    project_dir = WORKSPACE_DIR / project_name
    if not project_dir.is_dir():
        return None
    for day_dir in sorted(project_dir.iterdir(), reverse=True):
        if not day_dir.is_dir():
            continue
        candidate = day_dir / csv_name
        if candidate.exists():
            return candidate
    return None


def _elvis_route_lookup(elvis_csv_path: Path | str) -> tuple[dict[str, str], dict[str, str]]:
    """Build record_id -> route code/name maps from the MySQL Elvis export CSV."""
    path = Path(elvis_csv_path)
    if not path.exists():
        return {}, {}
    elvis = pd.read_csv(path, low_memory=False)
    id_col = _find_column(elvis.columns, "id", "elvis_id")
    code_col = _find_column(elvis.columns, "RouteSurveyedCode", "ROUTE_SURVEYEDCode", "ROUTE_SURVEYED_CODE")
    name_col = _find_column(elvis.columns, "RouteSurveyed", "ROUTE_SURVEYED")
    if not id_col:
        return {}, {}
    codes: dict[str, str] = {}
    names: dict[str, str] = {}
    for _, row in elvis.iterrows():
        rid = str(row.get(id_col) or "").strip()
        if not rid:
            continue
        if code_col and _is_valid_route_code(row.get(code_col)):
            codes[rid] = str(row.get(code_col)).strip()
        if name_col and _is_nonempty(row.get(name_col)):
            name = str(row.get(name_col)).strip()
            if "select the route" not in name.lower():
                names[rid] = name
    return codes, names


def merge_elvis_export_route_fields(df: pd.DataFrame, elvis_csv_path: Path | str) -> pd.DataFrame:
    """Fill empty ROUTE_SURVEYEDCode from MySQL export (Tampa uses RouteSurveyedCode)."""
    if df.empty:
        return df
    codes, names = _elvis_route_lookup(elvis_csv_path)
    if not codes and not names:
        return df
    out = df.copy()
    id_col = _find_column(out.columns, "id", "elvis_id", "ID")
    if not id_col:
        return out
    if "ROUTE_SURVEYEDCode" not in out.columns:
        out["ROUTE_SURVEYEDCode"] = ""
    if "ROUTE_SURVEYED" not in out.columns:
        out["ROUTE_SURVEYED"] = ""

    def _fill_code(row: pd.Series) -> str:
        current = row.get("ROUTE_SURVEYEDCode")
        if _is_nonempty(current):
            return str(current).strip()
        rid = str(row.get(id_col) or "").strip()
        return codes.get(rid, "")

    def _fill_name(row: pd.Series) -> str:
        current = row.get("ROUTE_SURVEYED")
        if _is_nonempty(current):
            return str(current).strip()
        rid = str(row.get(id_col) or "").strip()
        return names.get(rid, "")

    out["ROUTE_SURVEYEDCode"] = out.apply(_fill_code, axis=1)
    out["ROUTE_SURVEYED"] = out.apply(_fill_name, axis=1)
    return out


def hydrate_route_fields_from_elvis_export(project_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing ROUTE_SURVEYEDCode on a payload dataframe from staged Elvis export."""
    path = resolve_elvis_export_path(project_name)
    if path is None:
        return df
    return merge_elvis_export_route_fields(df, path)


def ensure_route_codes_for_project(project_name: str) -> int:
    """Backfill missing route codes in Snowflake from latest Elvis export (idempotent)."""
    path = resolve_elvis_export_path(project_name)
    if path is None:
        return 0
    return backfill_route_surveyed_codes(project_name, path)


def backfill_route_surveyed_codes(project_name: str, elvis_csv_path: Path | str | None = None) -> int:
    """Update existing RECORDS missing route code from the Elvis MySQL export."""
    if elvis_csv_path is None:
        return 0
    codes, names = _elvis_route_lookup(elvis_csv_path)
    if not codes and not names:
        return 0
    records = load_records(project_name)
    if records.empty:
        return 0
    now = datetime.utcnow()
    rows: list[dict[str, Any]] = []
    for _, row in records.iterrows():
        record_id = str(row["RECORD_ID"]).strip()
        payload = enrich_payload_from_typed_columns(row, _parse_payload(row.get("RECORD_PAYLOAD")))
        changed = False
        if record_id in codes and not _is_nonempty(payload.get("ROUTE_SURVEYEDCode")):
            code_val = codes[record_id]
            payload["ROUTE_SURVEYEDCode"] = code_val
            payload["RouteSurveyedCode"] = code_val
            changed = True
        if record_id in names and not _is_nonempty(payload.get("ROUTE_SURVEYED")):
            payload["ROUTE_SURVEYED"] = names[record_id]
            changed = True
        typed_code = row.get("ROUTE_SURVEYED_CODE")
        new_typed = _normalize_route_code(codes.get(record_id) if record_id in codes else typed_code)
        if changed or (record_id in codes and not _is_nonempty(typed_code)):
            rows.append(
                {
                    "PROJECT_NAME": project_name,
                    "RECORD_ID": record_id,
                    "ROUTE_SURVEYED_CODE": new_typed,
                    "RECORD_PAYLOAD": json.dumps(payload, default=str),
                    "UPDATED_AT": now,
                    "UPDATED_BY": "route_backfill",
                }
            )
    if not rows:
        return 0
    merge_upsert(
        pd.DataFrame(rows),
        "RECORDS",
        key_columns=["PROJECT_NAME", "RECORD_ID"],
        update_columns=["ROUTE_SURVEYED_CODE", "RECORD_PAYLOAD", "UPDATED_AT", "UPDATED_BY"],
        variant_columns=["RECORD_PAYLOAD"],
    )
    try:
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
    except Exception:
        pass
    return len(rows)


def load_combined_checks(project_name: str | None = None, flagged_only: bool = False) -> pd.DataFrame:
    from core.streamlit_cache import cached_load_combined_checks, cache_version, in_streamlit_runtime

    if in_streamlit_runtime():
        return cached_load_combined_checks(cache_version(), project_name, flagged_only)
    return _load_combined_checks_uncached(project_name, flagged_only)


def _load_combined_checks_uncached(project_name: str | None = None, flagged_only: bool = False) -> pd.DataFrame:
    clauses = []
    params: list[Any] = []
    if project_name:
        clauses.append("PROJECT_NAME = %s")
        params.append(project_name)
    if flagged_only:
        clauses.append("TWO_X_REVIEW_CHECK = 1")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_df(f"SELECT * FROM {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS {where}", tuple(params))


def load_assignments(
    assigned_to: str | None = None,
    team: str | None = None,
    project_name: str | None = None,
    include_deferred: bool = False,
) -> pd.DataFrame:
    from core.streamlit_cache import cached_load_assignments, cache_version, in_streamlit_runtime

    if in_streamlit_runtime():
        return cached_load_assignments(
            cache_version(), assigned_to, team, project_name, include_deferred
        )
    return _load_assignments_uncached(assigned_to, team, project_name, include_deferred)


def _load_assignments_uncached(
    assigned_to: str | None = None,
    team: str | None = None,
    project_name: str | None = None,
    include_deferred: bool = False,
) -> pd.DataFrame:
    clauses = ["STATUS = 'assigned'"]
    params: list[Any] = []
    if assigned_to:
        clauses.append("ASSIGNED_TO = %s")
        params.append(assigned_to)
    if team:
        clauses.append("TEAM = %s")
        params.append(team)
    if project_name:
        clauses.append("PROJECT_NAME = %s")
        params.append(project_name)
    if not include_deferred:
        clauses.append("(DEFER_UNTIL IS NULL OR DEFER_UNTIL <= CURRENT_TIMESTAMP())")
    where = "WHERE " + " AND ".join(clauses)
    return fetch_df(
        f"""
        SELECT * FROM {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
        {where}
        ORDER BY PRIORITY ASC, ASSIGNED_AT ASC
        """,
        tuple(params),
    )


def load_decision_history(project_name: str, record_id: str) -> pd.DataFrame:
    return fetch_df(
        f"""
        SELECT * FROM {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        WHERE PROJECT_NAME = %s AND RECORD_ID = %s
        ORDER BY CREATED_AT ASC
        """,
        (project_name, record_id),
    )


def _coerce_int(value: Any) -> int:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0
        return int(float(value))
    except (ValueError, TypeError):
        return 0


def _coerce_bool_value(value: Any) -> bool:
    """Safe boolean coercion; NaN/None must not become True."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("true", "1", "yes", "t")


_CHECK_SUM_COLUMNS = (
    "Traditional_Check",
    "OD_Distance_Check",
    "Transfer_Distance_Check",
    "StopListValidation_Check",
    "2X_REVIEW_CHECK",
)


def _compute_sum_all_checks(payload: dict[str, Any]) -> int:
    explicit = _coerce_int(payload.get("SUM_ALL_CHECKS"))
    if explicit > 0:
        return explicit
    return sum(_coerce_int(payload.get(col)) for col in _CHECK_SUM_COLUMNS)


def upsert_records_from_dataframe(
    project_name: str,
    df: pd.DataFrame,
    batch_id: str,
    mark_new: bool = True,
) -> dict[str, int]:
    """Incrementally upsert pipeline records.

    New records are inserted (and snapshotted into ORIGINAL_RECORDS). Existing records
    keep cleaning/review edits, but SUGGESTED_* transfer fields are refreshed each run.
    """
    from core.schema import repair_records_route_surveyed_code
    from pipeline.elvis_review_format import (
        has_transfer_suggestions,
        merge_suggestion_fields,
        merge_supervisor_comment_fields,
    )

    try:
        repair_records_route_surveyed_code()
    except Exception:
        pass

    result = {"inserted": 0, "suggestions_updated": 0, "skipped_existing": 0}
    if df.empty:
        return result
    existing = load_records(project_name)
    existing_ids = set(existing["RECORD_ID"].astype(str)) if not existing.empty else set()
    existing_payloads = {
        str(row["RECORD_ID"]): _parse_payload(row.get("RECORD_PAYLOAD"))
        for _, row in existing.iterrows()
    }
    rows = []
    suggestion_rows = []
    originals = []
    now = datetime.utcnow()
    for sort_idx, (_, row) in enumerate(df.iterrows()):
        record_id = _record_id_from_row(row)
        if not record_id:
            continue
        is_existing = record_id in existing_ids
        pipeline_payload = row.to_dict()
        if is_existing:
            base = existing_payloads.get(record_id, {})
            merged_payload = None
            if not has_transfer_suggestions(base):
                merged_payload = merge_suggestion_fields(base, pipeline_payload)
            comment_payload = merge_supervisor_comment_fields(merged_payload or base, pipeline_payload)
            if comment_payload is not None:
                merged_payload = comment_payload
            if merged_payload is not None:
                supervisor_comment = _payload_get(merged_payload, "SUPERVISOR_COMMENT", "ELVIS_COMMENT")
                row_update: dict[str, Any] = {
                    "PROJECT_NAME": project_name,
                    "RECORD_ID": record_id,
                    "BATCH_ID": batch_id,
                    "RECORD_PAYLOAD": json.dumps(merged_payload, default=str),
                    "UPDATED_AT": now,
                    "UPDATED_BY": "pipeline",
                }
                if supervisor_comment is not None:
                    row_update["SUPERVISOR_COMMENT"] = str(supervisor_comment).strip()
                suggestion_rows.append(row_update)
            else:
                result["skipped_existing"] += 1
            continue
        payload = normalize_payload_aliases(pipeline_payload)
        final_usage = _normalize_usage(_payload_get(payload, "Final_Usage", "FINAL_USAGE"))
        route_code = _normalize_route_code(
            _payload_get(payload, "ROUTE_SURVEYEDCode", "ROUTE_SURVEYED_CODE", "RouteSurveyedCode")
        )
        rows.append(
            {
                "PROJECT_NAME": project_name,
                "RECORD_ID": record_id,
                "BATCH_ID": batch_id,
                "INGESTED_AT": now,
                "IS_NEW": mark_new,
                "PIPELINE_SORT_ORDER": sort_idx,
                "FINAL_USAGE": final_usage,
                "FINAL_REVIEWER": _payload_get(payload, "FINAL_REVIEWER", "Final Reviewer"),
                "ROUTE_SURVEYED_CODE": route_code,
                "INTERV_INIT": _payload_get(payload, "INTERV_INIT", "IntervInit"),
                "ELVIS_STATUS": _payload_get(payload, "ElvisStatus", "ELVIS_STATUS"),
                "SUPERVISOR_COMMENT": payload.get("SUPERVISOR_COMMENT"),
                "FIRST_CLEANER": _payload_get(payload, "1st Cleaner", "FIRST_CLEANER"),
                "REASON_FOR_REMOVAL": payload.get("REASON FOR REMOVAL"),
                "RECORD_PAYLOAD": json.dumps(payload, default=str),
                "UPDATED_AT": now,
                "UPDATED_BY": "pipeline",
            }
        )
        originals.append(
            {
                "PROJECT_NAME": project_name,
                "RECORD_ID": record_id,
                "RECORD_PAYLOAD": json.dumps(payload, default=str),
                "CAPTURED_AT": now,
            }
        )
    inserted = 0
    if rows:
        records_df = pd.DataFrame(rows)
        # insert_only=True: existing records never get a full pipeline payload overwrite.
        inserted = merge_upsert(
            records_df,
            "RECORDS",
            key_columns=["PROJECT_NAME", "RECORD_ID"],
            insert_only=True,
            variant_columns=["RECORD_PAYLOAD"],
        )
        merge_upsert(
            pd.DataFrame(originals),
            "ORIGINAL_RECORDS",
            key_columns=["PROJECT_NAME", "RECORD_ID"],
            insert_only=True,
            variant_columns=["RECORD_PAYLOAD"],
        )
    updated_suggestions = 0
    if suggestion_rows:
        updated_suggestions = merge_upsert(
            pd.DataFrame(suggestion_rows),
            "RECORDS",
            key_columns=["PROJECT_NAME", "RECORD_ID"],
            update_columns=["RECORD_PAYLOAD", "BATCH_ID", "UPDATED_AT", "UPDATED_BY", "SUPERVISOR_COMMENT"],
            variant_columns=["RECORD_PAYLOAD"],
        )
    result["inserted"] = inserted or len(rows)
    result["suggestions_updated"] = updated_suggestions or len(suggestion_rows)
    return result


def backfill_transfer_suggestions(
    project_name: str,
    df: pd.DataFrame,
    batch_id: str,
) -> int:
    """Fill SUGGESTED_* only when a record does not already have transfer suggestions."""
    from pipeline.elvis_review_format import (
        has_transfer_suggestions,
        merge_suggestion_fields,
        merge_supervisor_comment_fields,
    )

    if df.empty:
        return 0
    existing = load_records(project_name)
    if existing.empty:
        return 0
    existing_payloads = {
        str(row["RECORD_ID"]): _parse_payload(row.get("RECORD_PAYLOAD"))
        for _, row in existing.iterrows()
    }
    suggestion_rows = []
    now = datetime.utcnow()
    for _, row in df.iterrows():
        record_id = _record_id_from_row(row)
        if not record_id or record_id not in existing_payloads:
            continue
        base = existing_payloads[record_id]
        # Keep existing SUGGESTED_* values — only fill suggestions for records that never had them.
        if has_transfer_suggestions(base):
            continue
        merged_payload = merge_suggestion_fields(base, row.to_dict())
        comment_payload = merge_supervisor_comment_fields(merged_payload or base, row.to_dict())
        if comment_payload is not None:
            merged_payload = comment_payload
        if merged_payload is None:
            continue
        existing_payloads[record_id] = merged_payload
        supervisor_comment = _payload_get(merged_payload, "SUPERVISOR_COMMENT", "ELVIS_COMMENT")
        row_update: dict[str, Any] = {
            "PROJECT_NAME": project_name,
            "RECORD_ID": record_id,
            "BATCH_ID": batch_id,
            "RECORD_PAYLOAD": json.dumps(merged_payload, default=str),
            "UPDATED_AT": now,
            "UPDATED_BY": "pipeline",
        }
        if supervisor_comment is not None:
            row_update["SUPERVISOR_COMMENT"] = str(supervisor_comment).strip()
        suggestion_rows.append(row_update)
    if not suggestion_rows:
        return 0
    updated = merge_upsert(
        pd.DataFrame(suggestion_rows),
        "RECORDS",
        key_columns=["PROJECT_NAME", "RECORD_ID"],
        update_columns=["RECORD_PAYLOAD", "BATCH_ID", "UPDATED_AT", "UPDATED_BY", "SUPERVISOR_COMMENT"],
        variant_columns=["RECORD_PAYLOAD"],
    )
    return updated or len(suggestion_rows)


def _coerce_str(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text or None


def upsert_combined_checks_from_dataframe(project_name: str, df: pd.DataFrame, batch_id: str) -> int:
    """Incrementally upsert flag results.

    Check values are refreshed on every run, but reviewer decisions
    (ADMIN_APPROVED, 2X_REVIEWED_*) are preserved across re-runs.
    """
    if df.empty:
        return 0
    rows = []
    now = datetime.utcnow()
    for _, row in df.iterrows():
        record_id = _record_id_from_row(row)
        if not record_id:
            continue
        payload = row.to_dict()
        sum_checks = _compute_sum_all_checks(payload)
        rows.append(
            {
                "PROJECT_NAME": project_name,
                "RECORD_ID": record_id,
                "BATCH_ID": batch_id,
                "TRADITIONAL_CHECK": _coerce_int(payload.get("Traditional_Check")),
                "OD_DISTANCE_CHECK": _coerce_int(payload.get("OD_Distance_Check")),
                "TRANSFER_DISTANCE_CHECK": _coerce_int(payload.get("Transfer_Distance_Check")),
                "STOPLISTVALIDATION_CHECK": _coerce_int(payload.get("StopListValidation_Check")),
                "TWO_X_REVIEW_CHECK": _coerce_int(payload.get("2X_REVIEW_CHECK")),
                "SUM_ALL_CHECKS": sum_checks,
                "ADMIN_APPROVED": _coerce_bool_value(payload.get("ADMIN_APPROVED")),
                "TWO_X_REVIEWED_BY": _coerce_str(payload.get("2x_REVIEWED_BY")),
                "TWO_X_REVIEWED_FLAG": _coerce_str(payload.get("2x_REVIEWED_FLAG")),
                "CHECK_PAYLOAD": json.dumps(payload, default=str),
                "UPDATED_AT": now,
            }
        )
    checks_df = pd.DataFrame(rows)
    merge_upsert(
        checks_df,
        "COMBINED_CHECKS",
        key_columns=["PROJECT_NAME", "RECORD_ID"],
        update_columns=[
            "BATCH_ID",
            "TRADITIONAL_CHECK",
            "OD_DISTANCE_CHECK",
            "TRANSFER_DISTANCE_CHECK",
            "STOPLISTVALIDATION_CHECK",
            "TWO_X_REVIEW_CHECK",
            "SUM_ALL_CHECKS",
            "CHECK_PAYLOAD",
            "UPDATED_AT",
        ],
        variant_columns=["CHECK_PAYLOAD"],
    )
    return len(rows)


def update_record_decision(
    project_name: str,
    record_id: str,
    final_usage: str,
    actor: str,
    actor_role: str,
    action: str,
) -> None:
    current = fetch_df(
        f"SELECT * FROM {REVIEW_CYCLE_SCHEMA}.RECORDS WHERE PROJECT_NAME = %s AND RECORD_ID = %s",
        (project_name, record_id),
    )
    if current.empty:
        return
    row = current.iloc[0]
    old_usage = row.get("FINAL_USAGE")
    payload = _parse_payload(row.get("RECORD_PAYLOAD"))
    payload["Final_Usage"] = final_usage
    execute(
        f"""
        UPDATE {REVIEW_CYCLE_SCHEMA}.RECORDS
        SET FINAL_USAGE = %s, RECORD_PAYLOAD = PARSE_JSON(%s), UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, UPDATED_BY = %s, IS_NEW = FALSE
        WHERE PROJECT_NAME = %s AND RECORD_ID = %s
        """,
        (final_usage, json.dumps(payload, default=str), actor, project_name, record_id),
    )
    execute(
        f"""
        INSERT INTO {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        (PROJECT_NAME, RECORD_ID, FIELD_NAME, OLD_VALUE, NEW_VALUE, ACTION, ACTOR, ACTOR_ROLE, CREATED_AT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
        """,
        (
            project_name,
            record_id,
            "Final_Usage",
            old_usage,
            final_usage,
            action,
            actor,
            actor_role,
        ),
    )


def set_admin_approved(project_name: str, record_id: str, actor: str, actor_role: str) -> None:
    execute(
        f"""
        UPDATE {REVIEW_CYCLE_SCHEMA}.COMBINED_CHECKS
        SET ADMIN_APPROVED = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE PROJECT_NAME = %s AND RECORD_ID = %s
        """,
        (project_name, record_id),
    )
    execute(
        f"""
        INSERT INTO {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        (PROJECT_NAME, RECORD_ID, FIELD_NAME, OLD_VALUE, NEW_VALUE, ACTION, ACTOR, ACTOR_ROLE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (project_name, record_id, "ADMIN_APPROVED", "FALSE", "TRUE", "Admin-Approve", actor, actor_role),
    )


def assign_records(
    project_name: str,
    record_ids: list[str],
    assigned_to: str,
    team: str,
    priority: int = 100,
) -> None:
    """Insert assignment rows in batched multi-row INSERTs (few Snowflake round-trips)."""
    ids: list[str] = []
    seen: set[str] = set()
    for record_id in record_ids:
        rid = str(record_id or "").strip()
        if not rid or rid in seen:
            continue
        seen.add(rid)
        ids.append(rid)
    if not ids:
        return

    # Keep each statement modest; one connection commit covers the whole batch via execute().
    chunk_size = 200
    priority_i = int(priority)
    for start in range(0, len(ids), chunk_size):
        chunk = ids[start : start + chunk_size]
        values_sql = ", ".join(
            ["(%s, %s, %s, %s, 'assigned', %s, CURRENT_TIMESTAMP())"] * len(chunk)
        )
        params: list = []
        for rid in chunk:
            params.extend([project_name, rid, assigned_to, team, priority_i])
        execute(
            f"""
            INSERT INTO {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
            (PROJECT_NAME, RECORD_ID, ASSIGNED_TO, TEAM, STATUS, PRIORITY, ASSIGNED_AT)
            VALUES {values_sql}
            """,
            tuple(params),
        )


def release_assignments(project_name: str, assigned_to: str, count: int) -> int:
    df = load_assignments(assigned_to=assigned_to, project_name=project_name)
    if df.empty:
        return 0
    to_release = df.head(count)
    released = 0
    for _, row in to_release.iterrows():
        execute(
            f"""
            UPDATE {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
            SET STATUS = 'released', NOTES = 'Released for higher-priority reassignment'
            WHERE ASSIGNMENT_ID = %s
            """,
            (row["ASSIGNMENT_ID"],),
        )
        released += 1
    return released


def unassign_records(
    project_name: str,
    record_ids: list[str],
    *,
    team: str = "cleaning",
    actor: str | None = None,
) -> int:
    """Release active assignments for the given record IDs (STATUS → released)."""
    unique_ids = _unique_norm_ids(record_ids)
    if not unique_ids:
        return 0
    return _unassign_by_project_records(project_name, unique_ids, team=team, actor=actor)


def unassign_by_assignment_ids(
    assignment_ids: list[int],
    *,
    actor: str | None = None,
    project_name: str | None = None,
    record_ids: list[str] | None = None,
    team: str = "cleaning",
) -> int:
    """Release active assignments by primary key; optionally also by record id as backup."""
    ids = sorted({int(x) for x in assignment_ids if x is not None})
    note = "Unassigned via Cleaning Assignments"
    if actor:
        note = f"{note} by {actor}"

    released = 0
    if ids:
        chunk_size = 200
        for start in range(0, len(ids), chunk_size):
            chunk = ids[start : start + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))
            released += execute(
                f"""
                UPDATE {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
                SET STATUS = 'released', NOTES = %s
                WHERE STATUS = 'assigned'
                  AND ASSIGNMENT_ID IN ({placeholders})
                """,
                (note, *chunk),
            )

    # Belt-and-suspenders: also release by normalized RECORD_ID so display/DB format drift cannot leave rows behind.
    if project_name and record_ids:
        released += _unassign_by_project_records(
            project_name, record_ids, team=team, actor=actor
        )
    return released


def _unique_norm_ids(record_ids: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for rid in record_ids:
        norm = _norm_id_for_unassign(rid)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def _unassign_by_project_records(
    project_name: str,
    record_ids: list[str],
    *,
    team: str,
    actor: str | None,
) -> int:
    unique_ids = _unique_norm_ids(record_ids)
    if not unique_ids:
        return 0
    note = "Unassigned via Cleaning Assignments"
    if actor:
        note = f"{note} by {actor}"

    # Match RECORD_ID whether stored as "1018", "1018.0", or numeric text.
    expanded: list[str] = []
    numeric_ids: list[int] = []
    for rid in unique_ids:
        expanded.append(rid)
        if rid.isdigit():
            expanded.append(f"{rid}.0")
            numeric_ids.append(int(rid))
    expanded = list(dict.fromkeys(expanded))
    exp_ph = ", ".join(["%s"] * len(expanded))
    if numeric_ids:
        num_ph = ", ".join(["%s"] * len(numeric_ids))
        number_clause = f" OR TRY_TO_NUMBER(TRIM(TO_VARCHAR(RECORD_ID))) IN ({num_ph})"
        params = (note, project_name, team, *expanded, *numeric_ids)
    else:
        number_clause = ""
        params = (note, project_name, team, *expanded)
    return execute(
        f"""
        UPDATE {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
        SET STATUS = 'released', NOTES = %s
        WHERE PROJECT_NAME = %s
          AND TEAM = %s
          AND STATUS = 'assigned'
          AND (
                RECORD_ID IN ({exp_ph})
                {number_clause}
          )
        """,
        params,
    )


def count_active_assignments_for_records(
    project_name: str,
    record_ids: list[str],
    *,
    team: str = "cleaning",
) -> int:
    """Fresh (uncached) count of still-active assignment rows for the given record IDs."""
    unique_ids = _unique_norm_ids(record_ids)
    if not unique_ids:
        return 0
    expanded: list[str] = []
    numeric_ids: list[int] = []
    for rid in unique_ids:
        expanded.append(rid)
        if rid.isdigit():
            expanded.append(f"{rid}.0")
            numeric_ids.append(int(rid))
    expanded = list(dict.fromkeys(expanded))
    exp_ph = ", ".join(["%s"] * len(expanded))
    if numeric_ids:
        num_ph = ", ".join(["%s"] * len(numeric_ids))
        number_clause = f" OR TRY_TO_NUMBER(TRIM(TO_VARCHAR(RECORD_ID))) IN ({num_ph})"
        params: tuple = (project_name, team, *expanded, *numeric_ids)
    else:
        number_clause = ""
        params = (project_name, team, *expanded)
    # Bypass Streamlit read-connection cache for verification.
    from core.snowflake_conn import cursor as sf_cursor

    query = f"""
        SELECT COUNT(*) AS N
        FROM {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
        WHERE PROJECT_NAME = %s
          AND TEAM = %s
          AND STATUS = 'assigned'
          AND (
                RECORD_ID IN ({exp_ph})
                {number_clause}
          )
    """
    with sf_cursor(schema=REVIEW_CYCLE_SCHEMA) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def _norm_id_for_unassign(value) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    try:
        # Handles values like 1018.0 coming from pandas floats already stringified oddly.
        as_float = float(text)
        if as_float.is_integer():
            return str(int(as_float))
    except (TypeError, ValueError):
        pass
    return text


def defer_assignment(assignment_id: int, hours: int = 24) -> None:
    execute(
        f"""
        UPDATE {REVIEW_CYCLE_SCHEMA}.ASSIGNMENTS
        SET DEFER_UNTIL = DATEADD(hour, %s, CURRENT_TIMESTAMP()), PRIORITY = PRIORITY + 1000
        WHERE ASSIGNMENT_ID = %s
        """,
        (hours, assignment_id),
    )


def load_reviewer_stats(project_name: str, stat_type: str | None = None) -> pd.DataFrame:
    clauses = ["PROJECT_NAME = %s"]
    params: list[Any] = [project_name]
    if stat_type:
        clauses.append("STAT_TYPE = %s")
        params.append(stat_type)
    where = " AND ".join(clauses)
    return fetch_df(
        f"SELECT * FROM {REVIEW_CYCLE_SCHEMA}.REVIEWER_STATS WHERE {where} ORDER BY STAT_TYPE, STAT_KEY, METRIC_NAME",
        tuple(params),
    )


def store_reviewer_stats(project_name: str, batch_id: str, stats_by_sheet: dict[str, pd.DataFrame]) -> None:
    rows = []
    for stat_type, df in stats_by_sheet.items():
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            stat_key = " | ".join(str(row.iloc[i]) for i in range(min(2, len(row.index))))
            for col in df.columns:
                value = row[col]
                metric_text = None
                metric_value = None
                if isinstance(value, (int, float)) and pd.notna(value):
                    metric_value = float(value)
                else:
                    metric_text = str(value)
                rows.append(
                    {
                        "PROJECT_NAME": project_name,
                        "STAT_TYPE": stat_type,
                        "STAT_KEY": stat_key,
                        "METRIC_NAME": str(col),
                        "METRIC_VALUE": metric_value,
                        "METRIC_TEXT": metric_text,
                        "BATCH_ID": batch_id,
                    }
                )
    if not rows:
        return
    execute(
        f"DELETE FROM {REVIEW_CYCLE_SCHEMA}.REVIEWER_STATS WHERE PROJECT_NAME = %s",
        (project_name,),
    )
    stats_df = pd.DataFrame(rows)
    # One row per metric cell; reviewer_stats xlsx can exceed Snowflake's 200k expression limit.
    append_dataframe(stats_df, "REVIEWER_STATS", chunk_size=15000)


def build_elvis_review_export(project_name: str) -> pd.DataFrame:
    records = load_records(project_name)
    return records_to_elvis_review(records)


def _merge_checks_into_dataframe(base: pd.DataFrame, checks: pd.DataFrame) -> pd.DataFrame:
    if checks.empty:
        return base
    check_cols = checks[
        [
            "RECORD_ID",
            "TRADITIONAL_CHECK",
            "OD_DISTANCE_CHECK",
            "TRANSFER_DISTANCE_CHECK",
            "STOPLISTVALIDATION_CHECK",
            "TWO_X_REVIEW_CHECK",
            "SUM_ALL_CHECKS",
            "ADMIN_APPROVED",
            "TWO_X_REVIEWED_BY",
            "TWO_X_REVIEWED_FLAG",
        ]
    ].copy()
    check_cols = check_cols.rename(
        columns={
            "RECORD_ID": "id",
            "TRADITIONAL_CHECK": "Traditional_Check",
            "OD_DISTANCE_CHECK": "OD_Distance_Check",
            "TRANSFER_DISTANCE_CHECK": "Transfer_Distance_Check",
            "STOPLISTVALIDATION_CHECK": "StopListValidation_Check",
            "TWO_X_REVIEW_CHECK": "2X_REVIEW_CHECK",
            "ADMIN_APPROVED": "ADMIN_APPROVED",
            "TWO_X_REVIEWED_BY": "2x_REVIEWED_BY",
            "TWO_X_REVIEWED_FLAG": "2x_REVIEWED_FLAG",
        }
    )
    id_col = "elvis_id" if "elvis_id" in base.columns else "id"
    left = base.copy()
    right = check_cols.copy()
    left[id_col] = left[id_col].fillna("").astype(str).str.strip()
    right["id"] = right["id"].fillna("").astype(str).str.strip()
    merged = left.merge(right, left_on=id_col, right_on="id", how="left", suffixes=("", "_chk"))
    for col in (
        "Traditional_Check",
        "OD_Distance_Check",
        "Transfer_Distance_Check",
        "StopListValidation_Check",
        "2X_REVIEW_CHECK",
        "ADMIN_APPROVED",
        "2x_REVIEWED_BY",
        "2x_REVIEWED_FLAG",
    ):
        chk_col = f"{col}_chk"
        if chk_col in merged.columns:
            if col in merged.columns:
                merged[col] = merged[chk_col].combine_first(merged[col])
            else:
                merged[col] = merged[chk_col]
            merged = merged.drop(columns=[chk_col])
    return merged


def records_to_elvis_review(records: pd.DataFrame) -> pd.DataFrame:
    """Shape record rows into the KingElvis Elvis_Review sheet layout (53 columns)."""
    from core.streamlit_cache import (
        cached_records_to_elvis_review,
        cache_version,
        in_streamlit_runtime,
        records_cache_key,
    )

    if in_streamlit_runtime() and not records.empty:
        key = records_cache_key(records)
        return cached_records_to_elvis_review(cache_version(), key, records)
    return _records_to_elvis_review_uncached(records)


def _records_to_elvis_review_uncached(records: pd.DataFrame) -> pd.DataFrame:
    from pipeline.elvis_review_format import shape_to_elvis_review

    if records.empty:
        return pd.DataFrame()
    projects = records["PROJECT_NAME"].unique()
    all_checks = load_combined_checks()
    if not all_checks.empty and len(projects):
        all_checks = all_checks[all_checks["PROJECT_NAME"].isin(projects)]
    parts: list[pd.DataFrame] = []
    for project in projects:
        proj_records = records[records["PROJECT_NAME"] == project]
        proj_ids = set(proj_records["RECORD_ID"].astype(str))
        proj_base = records_to_dataframe(proj_records)
        proj_base = hydrate_route_fields_from_elvis_export(str(project), proj_base)
        checks = (
            all_checks[all_checks["PROJECT_NAME"] == project]
            if not all_checks.empty
            else pd.DataFrame()
        )
        if not checks.empty:
            checks = checks[checks["RECORD_ID"].astype(str).isin(proj_ids)]
        parts.append(_merge_checks_into_dataframe(proj_base, checks))
    merged = pd.concat(parts, ignore_index=True) if parts else records_to_dataframe(records)
    shaped = shape_to_elvis_review(merged)
    try:
        from pipeline.scripts.sort_improved_auto_approval_output import sort_dataframe as sort_elvis_review_rows

        return sort_elvis_review_rows(shaped)
    except SystemExit:
        return shaped


def build_combined_checks_export(project_name: str) -> pd.DataFrame:
    records = load_records(project_name)
    checks = load_combined_checks(project_name)
    return _merge_checks_into_dataframe(records_to_dataframe(records), checks)
