from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import BUCKET_NAME
from core.data_access import (
    backfill_route_surveyed_codes,
    backfill_transfer_suggestions,
    build_combined_checks_export,
    build_elvis_review_export,
    merge_elvis_export_route_fields,
    store_reviewer_stats,
    upsert_combined_checks_from_dataframe,
    upsert_records_from_dataframe,
)
from core.projects import get_project, upsert_sync_state
from core.s3_utils import export_kingelvis_workbook, storage_mode_label
from pipeline.runner import PipelineContext, build_context, cleanup_workspace, run_full_pipeline


def ingest_pipeline_outputs(project_name: str, outputs: dict[str, Any]) -> dict[str, int]:
    ctx: PipelineContext = outputs["context"]
    counts = {"records_inserted": 0, "suggestions_updated": 0, "combined_checks": 0, "stats_sheets": 0}

    sorted_csv = outputs.get("auto", {}).get("sorted_elvis_review_csv")
    elvis_csv = ctx.workspace / ctx.elvis_csv_name
    if sorted_csv and Path(sorted_csv).exists():
        df = pd.read_csv(sorted_csv, low_memory=False)
        if elvis_csv.exists():
            df = merge_elvis_export_route_fields(df, elvis_csv)
        record_counts = upsert_records_from_dataframe(project_name, df, ctx.batch_id, mark_new=True)
        counts["records_inserted"] = record_counts["inserted"]
        counts["suggestions_updated"] = record_counts["suggestions_updated"]
        counts["skipped_existing"] = record_counts.get("skipped_existing", 0)
    if elvis_csv.exists() and outputs.get("phase") not in ("stats",):
        counts["route_codes_backfilled"] = backfill_route_surveyed_codes(project_name, elvis_csv)

    auto_csv = outputs.get("auto", {}).get("auto_approval_csv")
    if auto_csv and Path(auto_csv).exists():
        auto_df = pd.read_csv(auto_csv, low_memory=False)
        extra = backfill_transfer_suggestions(project_name, auto_df, ctx.batch_id)
        counts["suggestions_updated"] = max(counts["suggestions_updated"], extra)

    combined_auto = outputs.get("auto", {}).get("combined_auto_csv")
    if combined_auto and Path(combined_auto).exists():
        checks_df = pd.read_csv(combined_auto, low_memory=False)
        counts["combined_checks"] = upsert_combined_checks_from_dataframe(project_name, checks_df, ctx.batch_id)

    combined_flags = outputs.get("flags", {}).get("combined_flags_csv")
    if combined_flags and Path(combined_flags).exists():
        checks_df = pd.read_csv(combined_flags, low_memory=False)
        counts["combined_checks"] = upsert_combined_checks_from_dataframe(project_name, checks_df, ctx.batch_id)

    stats_xlsx = outputs.get("stats", {}).get("reviewer_stats_xlsx")
    if not stats_xlsx or not Path(stats_xlsx).exists():
        stats_xlsx = outputs.get("flags", {}).get("reviewer_stats_xlsx")
    if stats_xlsx and Path(stats_xlsx).exists():
        xls = pd.ExcelFile(stats_xlsx)
        stats_by_sheet = {sheet: pd.read_excel(stats_xlsx, sheet_name=sheet) for sheet in xls.sheet_names}
        store_reviewer_stats(project_name, ctx.batch_id, stats_by_sheet)
        counts["stats_sheets"] = len(stats_by_sheet)

    demo_csv = outputs.get("demographics", {}).get("demographics_csv")
    if demo_csv and Path(demo_csv).exists():
        from services import quality

        demo_df = quality.ingest_demographics_from_csv(project_name, demo_csv)
        counts["demographic_checks"] = len(demo_df)

    upsert_sync_state(
        project_name,
        LAST_PULL_TS=datetime.utcnow(),
        LAST_PIPELINE_STATUS="success",
        LAST_PIPELINE_MESSAGE=(
            f"inserted={counts['records_inserted']} "
            f"skipped={counts.get('skipped_existing', 0)} "
            f"suggestions_updated={counts['suggestions_updated']} "
            f"checks={counts['combined_checks']}"
        ),
    )
    return counts


def format_ingest_counts(counts: dict[str, int]) -> str:
    parts = []
    if counts.get("records_inserted"):
        parts.append(f"{counts['records_inserted']} new record(s)")
    if counts.get("skipped_existing"):
        parts.append(f"{counts['skipped_existing']} existing record(s) preserved (edits kept)")
    if counts.get("suggestions_updated"):
        parts.append(f"{counts['suggestions_updated']} suggestion update(s)")
    if counts.get("combined_checks"):
        parts.append(f"{counts['combined_checks']} check row(s)")
    if counts.get("stats_sheets"):
        parts.append(f"{counts['stats_sheets']} stats sheet(s)")
    if counts.get("demographic_checks"):
        parts.append(f"{counts['demographic_checks']} demographic check(s)")
    if counts.get("route_codes_backfilled"):
        parts.append(f"{counts['route_codes_backfilled']} route code(s) backfilled")
    return ", ".join(parts) if parts else "no changes (all records already present)"


def export_kingelvis(project_name: str) -> str:
    project = get_project(project_name)
    if not project:
        raise ValueError(f"Unknown project: {project_name}")
    key = project.get("KINGELVIS_FILE_NAME")
    if not key:
        raise ValueError(f"No KINGELVIS_FILE_NAME configured for {project_name}")
    elvis_df = build_elvis_review_export(project_name)
    combined_df = build_combined_checks_export(project_name)
    location = export_kingelvis_workbook(elvis_df, combined_df, BUCKET_NAME or "", key)
    upsert_sync_state(project_name, LAST_KINGELVIS_EXPORT_TS=datetime.utcnow())
    return location


def export_kingelvis_to_s3(project_name: str) -> str:
    """Backward-compatible alias."""
    return export_kingelvis(project_name)


def sync_project(project_name: str | None, phase: str = "auto", progress=None) -> dict[str, Any]:
    if not project_name:
        raise ValueError("project_name is required")
    outputs: dict[str, Any] | None = None
    try:
        outputs = run_full_pipeline(project_name, phase=phase, progress=progress)
        if progress is not None and hasattr(progress, "set_total") and hasattr(progress, "update"):
            final_step = int(getattr(progress, "total", 1)) + 1
            progress.set_total(final_step)
            progress.update(final_step, "Saving pipeline results to Snowflake...")
        counts = ingest_pipeline_outputs(project_name, outputs)
        return {"outputs": outputs, "counts": counts}
    finally:
        if outputs:
            ctx = outputs.get("context")
            if isinstance(ctx, PipelineContext):
                cleanup_workspace(ctx)


def sync_and_export(
    project_name: str,
    phase: str = "auto",
    export: bool = True,
    progress=None,
) -> dict[str, Any]:
    result = sync_project(project_name, phase=phase, progress=progress)
    if export:
        if progress is not None and hasattr(progress, "set_total") and hasattr(progress, "update"):
            final_step = int(getattr(progress, "total", 1)) + 1
            progress.set_total(final_step)
            progress.update(final_step, "Building and uploading the KingElvis workbook...")
        result["kingelvis_location"] = export_kingelvis(project_name)
    try:
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
    except Exception:
        pass
    return result
