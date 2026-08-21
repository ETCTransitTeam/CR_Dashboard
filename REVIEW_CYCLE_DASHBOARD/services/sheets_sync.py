"""Import Elvis Review / Combined Checks decision fields from an uploaded workbook into Snowflake."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA
from core.data_access import _parse_payload, load_combined_checks, load_records
from core.snowflake_conn import executemany, merge_upsert
from services import history as history_svc
from services.history import FIELD_TO_COLUMN

DEFAULT_WORKSHEET = "Elvis_Review"

SHEETS_SYNC_ACTION = "Sheets-Sync"

ELVIS_IMPORT_FIELDS: tuple[str, ...] = (
    "Final_Usage",
    "FINAL_REVIEWER",
    "REASON FOR REMOVAL",
    "REASON FOR REMOVAL [Other]",
    "POSSIBLE ERRORS",
)
CHECK_IMPORT_FIELDS: tuple[str, ...] = (
    "ADMIN_APPROVED",
    "2x_REVIEWED_BY",
    "2x_REVIEWED_FLAG",
)

_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "Final_Usage": ("Final_Usage", "FINAL_USAGE"),
    "FINAL_REVIEWER": ("FINAL_REVIEWER", "Final Reviewer"),
    "REASON FOR REMOVAL": ("REASON FOR REMOVAL", "REASON_FOR_REMOVAL"),
    "REASON FOR REMOVAL [Other]": ("REASON FOR REMOVAL [Other]",),
    "POSSIBLE ERRORS": ("POSSIBLE ERRORS",),
    "ADMIN_APPROVED": ("ADMIN_APPROVED",),
    "2x_REVIEWED_BY": ("2x_REVIEWED_BY", "2X_REVIEWED_BY"),
    "2x_REVIEWED_FLAG": ("2x_REVIEWED_FLAG", "2X_REVIEWED_FLAG"),
}


@dataclass
class SheetsImportResult:
    rows_read: int = 0
    records_updated: int = 0
    fields_changed: int = 0
    unmatched_rows: int = 0
    skipped_empty: int = 0


def _cell_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "<na>"}:
        return ""
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        return text[:-2]
    return text


def _norm_record_id(value: Any) -> str:
    return _cell_text(value)


def _id_aliases(record_id: str) -> list[str]:
    rid = _norm_record_id(record_id)
    if not rid:
        return []
    aliases = [rid]
    if rid.isdigit():
        aliases.append(f"{rid}.0")
    return aliases


def _existing_id_map(records: pd.DataFrame) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if records.empty or "RECORD_ID" not in records.columns:
        return mapping
    for raw in records["RECORD_ID"].tolist():
        persisted = _norm_record_id(raw)
        if not persisted:
            continue
        for alias in _id_aliases(persisted):
            mapping.setdefault(alias, persisted)
    return mapping


def _column_lookup(columns: list[str] | pd.Index) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for col in columns:
        name = str(col)
        lookup.setdefault(name, name)
        lookup.setdefault(name.lower(), name)
    return lookup


def _sheet_column(lookup: dict[str, str], field: str) -> str | None:
    for alias in _FIELD_ALIASES.get(field, (field,)):
        if alias in lookup:
            return lookup[alias]
        lowered = alias.lower()
        if lowered in lookup:
            return lookup[lowered]
    return None


def _normalize_final_usage(value: Any) -> str:
    text = _cell_text(value)
    lowered = text.lower()
    if lowered == "use":
        return "Use"
    if lowered == "remove":
        return "Remove"
    return text


def _sheet_record_id(row: pd.Series) -> str:
    for col in ("elvis_id", "id", "ID", "Elvis_id", "RECORD_ID"):
        if col not in row.index:
            continue
        rid = _norm_record_id(row[col])
        if rid:
            return rid
    return ""


def _elvis_updates_from_row(row: pd.Series, lookup: dict[str, str]) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    for field in ELVIS_IMPORT_FIELDS:
        col = _sheet_column(lookup, field)
        if col is None:
            continue
        raw = row[col]
        updates[field] = _normalize_final_usage(raw) if field == "Final_Usage" else _cell_text(raw)
    return updates


def _check_updates_from_row(row: pd.Series, lookup: dict[str, str]) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    for field in CHECK_IMPORT_FIELDS:
        col = _sheet_column(lookup, field)
        if col is None:
            continue
        raw = row[col]
        if field == "ADMIN_APPROVED":
            updates[field] = history_svc.coerce_bool(raw)
        else:
            updates[field] = _cell_text(raw)
    return updates


def read_uploaded_workbook(uploaded, worksheet: str = DEFAULT_WORKSHEET) -> pd.DataFrame:
    """Read the Elvis Review tab from an uploaded .xlsx (SharePoint/Excel download)."""
    name = (worksheet or DEFAULT_WORKSHEET).strip() or DEFAULT_WORKSHEET
    xl = pd.ExcelFile(uploaded, engine="openpyxl")
    names = list(xl.sheet_names)
    if name in names:
        return xl.parse(name)
    lowered = {str(tab).lower(): tab for tab in names}
    if name.lower() in lowered:
        return xl.parse(lowered[name.lower()])
    raise RuntimeError(
        f"Worksheet {name!r} not found. Available tabs: {', '.join(names) or '(none)'}"
    )


def _index_by_record_id(df: pd.DataFrame) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}
    if df.empty or "RECORD_ID" not in df.columns:
        return out
    for _, row in df.iterrows():
        rid = _norm_record_id(row.get("RECORD_ID"))
        if rid:
            out.setdefault(rid, row)
    return out


def _history_row(
    project_name: str,
    record_id: str,
    field: str,
    old_value: Any,
    new_value: Any,
    actor: str,
    actor_role: str,
) -> tuple[Any, ...]:
    return (
        project_name,
        record_id,
        field,
        history_svc._norm(old_value)[:4000],
        history_svc._norm(new_value)[:4000],
        SHEETS_SYNC_ACTION,
        actor,
        actor_role,
    )


def _collect_record_diff(
    rec: pd.Series,
    elvis_updates: dict[str, Any],
    project_name: str,
    persisted_id: str,
    actor: str,
) -> tuple[dict[str, Any] | None, list[tuple[str, Any, Any]]]:
    payload = _parse_payload(rec.get("RECORD_PAYLOAD"))
    changes: list[tuple[str, Any, Any]] = []
    column_updates: dict[str, Any] = {}
    new_payload = dict(payload)
    for field, new_value in elvis_updates.items():
        old_value = payload.get(field)
        if history_svc._norm(old_value) == history_svc._norm(new_value):
            continue
        new_payload[field] = new_value
        changes.append((field, old_value, new_value))
        col = FIELD_TO_COLUMN.get(field)
        if col:
            column_updates[col] = new_value
    if not changes:
        return None, []

    def _typed(value: Any) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return value

    update_row = {
        "PROJECT_NAME": project_name,
        "RECORD_ID": persisted_id,
        "RECORD_PAYLOAD": json.dumps(new_payload, default=str),
        "UPDATED_BY": actor,
        "UPDATED_AT": datetime.now(),
        "IS_NEW": False,
        "FINAL_USAGE": _typed(rec.get("FINAL_USAGE")),
        "FINAL_REVIEWER": _typed(rec.get("FINAL_REVIEWER")),
        "REASON_FOR_REMOVAL": _typed(rec.get("REASON_FOR_REMOVAL")),
    }
    update_row.update(column_updates)
    return update_row, changes


def _collect_check_diff(
    crow: pd.Series,
    check_updates: dict[str, Any],
    project_name: str,
    persisted_id: str,
) -> tuple[dict[str, Any] | None, list[tuple[str, Any, Any]]]:
    field_map = {
        "ADMIN_APPROVED": "ADMIN_APPROVED",
        "2x_REVIEWED_BY": "TWO_X_REVIEWED_BY",
        "2x_REVIEWED_FLAG": "TWO_X_REVIEWED_FLAG",
    }
    changes: list[tuple[str, Any, Any]] = []
    def _opt_text(value: Any) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip()
        return text or None

    merged = {
        "ADMIN_APPROVED": history_svc.coerce_bool(crow.get("ADMIN_APPROVED")),
        "TWO_X_REVIEWED_BY": _opt_text(crow.get("TWO_X_REVIEWED_BY")),
        "TWO_X_REVIEWED_FLAG": _opt_text(crow.get("TWO_X_REVIEWED_FLAG")),
    }
    for ui_field, db_col in field_map.items():
        if ui_field not in check_updates:
            continue
        new_val = check_updates[ui_field]
        if ui_field == "ADMIN_APPROVED":
            new_val = bool(new_val)
        elif ui_field in ("2x_REVIEWED_BY", "2x_REVIEWED_FLAG"):
            new_val = str(new_val or "").strip() or None
        old_val = crow.get(db_col)
        if history_svc._norm(old_val) == history_svc._norm(new_val):
            continue
        merged[db_col] = new_val
        changes.append((ui_field, old_val, new_val))
    if not changes:
        return None, []
    update_row = {
        "PROJECT_NAME": project_name,
        "RECORD_ID": persisted_id,
        "UPDATED_AT": datetime.now(),
        **merged,
    }
    return update_row, changes


def _write_history(rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    executemany(
        f"""
        INSERT INTO {REVIEW_CYCLE_SCHEMA}.DECISION_HISTORY
        (PROJECT_NAME, RECORD_ID, FIELD_NAME, OLD_VALUE, NEW_VALUE, ACTION, ACTOR, ACTOR_ROLE, CREATED_AT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
        """,
        rows,
        chunk_size=2000,
    )


def import_sheet_into_snowflake(
    project_name: str,
    actor: str,
    actor_role: str,
    sheet: pd.DataFrame,
) -> SheetsImportResult:
    """Write matching workbook decision fields into existing Snowflake rows."""
    result = SheetsImportResult(rows_read=int(len(sheet)))
    if sheet.empty:
        return result

    records = load_records(project_name)
    checks = load_combined_checks(project_name)
    existing = _existing_id_map(records)
    records_by_id = _index_by_record_id(records)
    checks_by_id = _index_by_record_id(checks)
    lookup = _column_lookup(sheet.columns)
    seen_ids: set[str] = set()

    record_rows: list[dict[str, Any]] = []
    check_rows: list[dict[str, Any]] = []
    history_rows: list[tuple[Any, ...]] = []
    touched: set[str] = set()

    for _, row in sheet.iterrows():
        sheet_id = _sheet_record_id(row)
        if not sheet_id:
            result.skipped_empty += 1
            continue
        persisted_id = existing.get(sheet_id) or existing.get(_norm_record_id(sheet_id))
        if not persisted_id:
            result.unmatched_rows += 1
            continue
        if persisted_id in seen_ids:
            continue
        seen_ids.add(persisted_id)

        rec = records_by_id.get(persisted_id)
        if rec is None:
            result.unmatched_rows += 1
            continue

        field_count = 0
        elvis_updates = _elvis_updates_from_row(row, lookup)
        if elvis_updates:
            rec_row, rec_changes = _collect_record_diff(
                rec, elvis_updates, project_name, persisted_id, actor
            )
            if rec_row:
                record_rows.append(rec_row)
                history_rows.extend(
                    _history_row(project_name, persisted_id, field, old, new, actor, actor_role)
                    for field, old, new in rec_changes
                )
                field_count += len(rec_changes)

        check_updates = _check_updates_from_row(row, lookup)
        crow = checks_by_id.get(persisted_id)
        if check_updates and crow is not None:
            chk_row, chk_changes = _collect_check_diff(
                crow, check_updates, project_name, persisted_id
            )
            if chk_row:
                check_rows.append(chk_row)
                history_rows.extend(
                    _history_row(project_name, persisted_id, field, old, new, actor, actor_role)
                    for field, old, new in chk_changes
                )
                field_count += len(chk_changes)

        if field_count:
            touched.add(persisted_id)
            result.fields_changed += field_count

    result.records_updated = len(touched)

    if record_rows:
        merge_upsert(
            pd.DataFrame(record_rows),
            "RECORDS",
            key_columns=["PROJECT_NAME", "RECORD_ID"],
            update_columns=[
                "RECORD_PAYLOAD",
                "FINAL_USAGE",
                "FINAL_REVIEWER",
                "REASON_FOR_REMOVAL",
                "UPDATED_BY",
                "UPDATED_AT",
                "IS_NEW",
            ],
            variant_columns=["RECORD_PAYLOAD"],
        )
    if check_rows:
        merge_upsert(
            pd.DataFrame(check_rows),
            "COMBINED_CHECKS",
            key_columns=["PROJECT_NAME", "RECORD_ID"],
            update_columns=[
                "ADMIN_APPROVED",
                "TWO_X_REVIEWED_BY",
                "TWO_X_REVIEWED_FLAG",
                "UPDATED_AT",
            ],
        )
    _write_history(history_rows)

    try:
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
    except Exception:
        pass
    return result


def format_import_result(result: SheetsImportResult) -> str:
    parts = [
        f"Updated {result.records_updated} record(s)",
        f"{result.fields_changed} field(s) changed",
    ]
    if result.unmatched_rows:
        parts.append(f"{result.unmatched_rows} sheet row(s) had no matching Snowflake record")
    if result.skipped_empty:
        parts.append(f"{result.skipped_empty} empty-id row(s) skipped")
    return "; ".join(parts) + "."
