"""Field-team Remove/Delete and Supervisor Remark sheets (matches Excel export layout)."""

from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from core.data_access import records_to_dataframe, records_to_elvis_review, resolve_elvis_export_path
from services import history as history_svc

REMOVE_DELETE_COLUMNS = [
    "ID",
    "ROUTE_SURVEYED",
    "ROUTE_SURVEYEDCode",
    "INTERV_INIT",
    "FINAL_REVIEWER",
    "FINAL_USAGE",
    "REASON FOR REMOVAL",
    "REASON FOR REMOVAL [Other]",
    "ELVIS_STATUS",
    "FIELD MANAGER NAME",
    "UPDATED STATUS NEEDED",
    "COMMENTS",
]

SUPERVISOR_REMARK_COLUMNS = [
    "ID",
    "FINAL_REVIEWER",
    "FINAL_USAGE",
    "ElvisRemark",
    "COMMENTS",
]

REMOVE_DELETE_EDITABLE = frozenset({"FIELD MANAGER NAME", "UPDATED STATUS NEEDED", "COMMENTS"})
SUPERVISOR_REMARK_EDITABLE = frozenset({"ElvisRemark", "COMMENTS"})

_REMARK_JUNK = r"^ElvisRemark\s+Elvis Remark:"


def _norm(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _norm_lower(value: Any) -> str:
    return _norm(value).lower()


def _record_id_column(df: pd.DataFrame) -> str | None:
    for col in ("elvis_id", "id", "ID"):
        if col in df.columns:
            return col
    return None


def _valid_survey_mask(review: pd.DataFrame) -> pd.Series:
    interv = review["INTERV_INIT"].fillna("").astype(str) if "INTERV_INIT" in review.columns else pd.Series("", index=review.index)
    have5_col = next(
        (c for c in ("HAVE_5_MIN_FOR_SURVECode", "HAVE_5_MIN_FOR_SURVE") if c in review.columns),
        None,
    )
    have5 = review[have5_col].fillna("").astype(str) if have5_col else pd.Series("", index=review.index)
    return (interv != "999") & (have5 == "1")


def _payload_by_id(records: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if records.empty:
        return {}
    payloads = records_to_dataframe(records)
    ids = records["RECORD_ID"].astype(str).tolist()
    return {rid: payloads.iloc[i].to_dict() for i, rid in enumerate(ids) if i < len(payloads)}


def _elvis_remark_lookup(project_name: str) -> dict[str, str]:
    path = resolve_elvis_export_path(project_name)
    if not path or not path.exists():
        return {}
    elvis = pd.read_csv(path, low_memory=False)
    id_col = next((c for c in elvis.columns if c.lower() in ("id", "elvis_id")), None)
    remark_col = next((c for c in elvis.columns if c.lower() == "elvisremark"), None)
    if not id_col or not remark_col:
        return {}
    out: dict[str, str] = {}
    for _, row in elvis.iterrows():
        rid = _norm(row.get(id_col))
        remark = _norm(row.get(remark_col))
        if rid and remark and not remark.lower().startswith("elvisremark  elvis remark:"):
            out[rid] = remark
    return out


def _field_value(payload: dict[str, Any], key: str) -> str:
    return _norm(payload.get(key))


def build_remove_or_delete_sheet(
    records: pd.DataFrame,
    elvis_review: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Records with Final_Usage = remove or ElvisStatus = delete (valid 5-min surveys)."""
    empty = pd.DataFrame(columns=REMOVE_DELETE_COLUMNS)
    if records.empty:
        return empty

    review = elvis_review if elvis_review is not None else records_to_elvis_review(records)
    if review.empty:
        return empty

    id_col = _record_id_column(review)
    if not id_col:
        return empty

    usage = review["Final_Usage"].fillna("").astype(str).str.strip().str.lower() if "Final_Usage" in review.columns else pd.Series("", index=review.index)
    status = review["ElvisStatus"].fillna("").astype(str).str.strip().str.lower() if "ElvisStatus" in review.columns else pd.Series("", index=review.index)
    valid = _valid_survey_mask(review)
    mask = ((usage == "remove") | (status == "delete")) & valid
    subset = review[mask].copy()
    if subset.empty:
        return empty

    payloads = _payload_by_id(records.loc[records["RECORD_ID"].astype(str).isin(subset[id_col].astype(str))])
    rows: list[dict[str, Any]] = []
    for _, row in subset.iterrows():
        rid = _norm(row.get(id_col))
        payload = payloads.get(rid, {})
        rows.append(
            {
                "ID": rid,
                "ROUTE_SURVEYED": _norm(row.get("ROUTE_SURVEYED")),
                "ROUTE_SURVEYEDCode": _norm(row.get("ROUTE_SURVEYEDCode")),
                "INTERV_INIT": _norm(row.get("INTERV_INIT")),
                "FINAL_REVIEWER": _norm(row.get("FINAL_REVIEWER")),
                "FINAL_USAGE": _norm_lower(row.get("Final_Usage")),
                "REASON FOR REMOVAL": _norm(row.get("REASON FOR REMOVAL")),
                "REASON FOR REMOVAL [Other]": _norm(row.get("REASON FOR REMOVAL [Other]")),
                "ELVIS_STATUS": _norm_lower(row.get("ElvisStatus")),
                "FIELD MANAGER NAME": _field_value(payload, "FIELD MANAGER NAME"),
                "UPDATED STATUS NEEDED": _field_value(payload, "UPDATED STATUS NEEDED"),
                "COMMENTS": _field_value(payload, "COMMENTS"),
            }
        )
    return pd.DataFrame(rows, columns=REMOVE_DELETE_COLUMNS)


def build_supervisor_remark_sheet(
    records: pd.DataFrame,
    project_name: str,
    elvis_review: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Use records with a non-empty Elvis remark (excludes remove/delete rows)."""
    empty = pd.DataFrame(columns=SUPERVISOR_REMARK_COLUMNS)
    if records.empty:
        return empty

    review = elvis_review if elvis_review is not None else records_to_elvis_review(records)
    if review.empty:
        return empty

    id_col = _record_id_column(review)
    if not id_col:
        return empty

    remove_ids = set(build_remove_or_delete_sheet(records, elvis_review=review)["ID"].astype(str))
    elvis_remarks = _elvis_remark_lookup(project_name)
    valid = _valid_survey_mask(review)
    usage = review["Final_Usage"].fillna("").astype(str).str.strip().str.lower() if "Final_Usage" in review.columns else pd.Series("", index=review.index)

    payloads = _payload_by_id(records)
    rows: list[dict[str, Any]] = []
    for _, row in review.iterrows():
        rid = _norm(row.get(id_col))
        if not rid or rid in remove_ids or _norm_lower(row.get("Final_Usage")) != "use":
            continue
        if not valid.loc[row.name]:
            continue
        payload = payloads.get(rid, {})
        remark = elvis_remarks.get(rid) or _field_value(payload, "ELVIS_COMMENT") or _field_value(payload, "ElvisRemark")
        if not remark:
            continue
        rows.append(
            {
                "ID": rid,
                "FINAL_REVIEWER": _norm(row.get("FINAL_REVIEWER")),
                "FINAL_USAGE": _norm_lower(row.get("Final_Usage")),
                "ElvisRemark": remark,
                "COMMENTS": _field_value(payload, "SUPERVISOR_REMARK_COMMENTS") or _field_value(payload, "COMMENTS"),
            }
        )
    return pd.DataFrame(rows, columns=SUPERVISOR_REMARK_COLUMNS)


def _persist_sheet_edits(
    before: pd.DataFrame,
    after: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
    editable_fields: Iterable[str],
    *,
    action: str,
    remark_field: str | None = None,
    comments_payload_key: str = "COMMENTS",
) -> int:
    if before.empty or records.empty:
        return 0
    editable = set(editable_fields)
    project_by_id = records.set_index(records["RECORD_ID"].astype(str))["PROJECT_NAME"].to_dict()
    actor = user.get("name") or user.get("EMAIL")
    role = user.get("ROLE") or user.get("role")
    saved = 0

    before = before.reset_index(drop=True)
    after = after.reset_index(drop=True)
    for i in range(len(before)):
        record_id = _norm(before.iloc[i].get("ID"))
        if not record_id:
            continue
        project = project_by_id.get(record_id)
        if not project:
            continue
        updates: dict[str, Any] = {}
        for field in editable:
            if field not in before.columns or field not in after.columns:
                continue
            old_val = before.iloc[i][field]
            new_val = after.iloc[i][field]
            if _norm(old_val) == _norm(new_val):
                continue
            if field == "ElvisRemark" and remark_field:
                updates[remark_field] = new_val
            elif field == "COMMENTS":
                updates[comments_payload_key] = new_val
            else:
                updates[field] = new_val
        if updates:
            saved += history_svc.apply_record_update(
                project,
                record_id,
                updates,
                actor,
                role,
                action=action,
                editable_only=False,
            )
    return saved


def persist_remove_delete_edits(
    before: pd.DataFrame,
    after: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
) -> int:
    return _persist_sheet_edits(
        before,
        after,
        records,
        user,
        REMOVE_DELETE_EDITABLE,
        action="Field Team - Remove/Delete",
        comments_payload_key="COMMENTS",
    )


def persist_supervisor_remark_edits(
    before: pd.DataFrame,
    after: pd.DataFrame,
    records: pd.DataFrame,
    user: dict,
) -> int:
    return _persist_sheet_edits(
        before,
        after,
        records,
        user,
        SUPERVISOR_REMARK_EDITABLE,
        action="Field Team - Supervisor Remark",
        remark_field="ELVIS_COMMENT",
        comments_payload_key="SUPERVISOR_REMARK_COMMENTS",
    )


def field_team_workbook_bytes(remove_df: pd.DataFrame, supervisor_df: pd.DataFrame) -> bytes:
    import io

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        remove_df.to_excel(writer, sheet_name="Remove_or_Delete", index=False)
        supervisor_df.to_excel(writer, sheet_name="Supervisor_Remark", index=False)
    buffer.seek(0)
    return buffer.read()
