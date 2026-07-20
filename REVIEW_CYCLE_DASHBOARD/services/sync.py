"""Synchronization service.

Separates two concerns that used to be tangled together:

1. Detection  - cheaply compare the OD Collection dashboard's last_survey_date
   (Option A: Snowflake timestamp tracking) against what this app has already seen.
2. Execution  - explicitly run the HRTVA pipeline + ingest to (re)generate records.

A headless ``morning_refresh`` entrypoint is provided for Option B (scheduled
refresh via Snowflake Task or cron):  ``python -m services.sync``.
The Streamlit UI provides Option C (manual refresh button).

This never modifies the OD Collection dashboard; it only reads its timestamp.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from core.projects import (
    get_od_last_sync,
    get_project,
    get_sync_state,
    list_projects,
    upsert_sync_state,
)


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if ts is None or pd.isna(ts):
        return None
    return ts.to_pydatetime()


def od_sync_status(project_name: str) -> dict[str, Any]:
    """Report whether the OD dashboard has synced newer data than we've consumed."""
    od_sync = get_od_last_sync(project_name)
    state = get_sync_state(project_name) or {}
    last_seen = _to_datetime(state.get("LAST_OD_SYNC_SEEN"))

    if od_sync is None:
        project = get_project(project_name)
        if not project or not project.get("BASE_SCHEMA"):
            return {
                "available": False,
                "message": "OD sync watch unavailable (no BASE_SCHEMA for this project in APP_CONFIG).",
                "od_sync": None,
            }
        return {
            "available": False,
            "message": "Waiting on the first OD Collection sync for this project.",
            "od_sync": None,
        }

    od_dt = _to_datetime(od_sync)
    if last_seen is None or (od_dt is not None and od_dt > last_seen):
        return {
            "available": True,
            "message": f"New OD Collection sync detected at {od_dt}.",
            "od_sync": od_dt,
        }
    return {
        "available": False,
        "message": f"Already in sync with OD Collection (seen {last_seen}).",
        "od_sync": od_dt,
    }


def mark_od_sync_seen(project_name: str, od_sync: datetime | None = None) -> None:
    upsert_sync_state(project_name, LAST_OD_SYNC_SEEN=od_sync or datetime.utcnow())


def run_incremental_pull(project_name: str, export: bool = False) -> dict[str, Any]:
    """Explicitly run the auto-approval pipeline and ingest new Elvis records.

    Imported lazily to avoid pulling heavy pipeline deps for pure detection calls.
    """
    from pipeline.ingest import sync_and_export

    result = sync_and_export(project_name, phase="auto", export=export)
    status = od_sync_status(project_name)
    if status.get("od_sync"):
        mark_od_sync_seen(project_name, status["od_sync"])
    return result


def morning_refresh(
    project_names: list[str] | None = None,
    force: bool = False,
    progress=None,
) -> list[dict[str, Any]]:
    """Scheduled hook: pull every project whose OD data changed since last seen.

    Returns a per-project summary. Designed to be safe to run unattended.
    """
    if project_names is None:
        projects = list_projects()
        project_names = projects["PROJECT_NAME"].tolist() if not projects.empty else []

    summary: list[dict[str, Any]] = []
    total = max(len(project_names), 1)
    for index, project in enumerate(project_names, start=1):
        if progress:
            progress(index, total, f"Checking and refreshing {project} ({index}/{len(project_names)})...")
        status = od_sync_status(project)
        if not force and not status["available"]:
            summary.append({"project": project, "action": "skipped", "reason": status["message"]})
            continue
        try:
            result = run_incremental_pull(project, export=False)
            summary.append({"project": project, "action": "pulled", "counts": result.get("counts")})
        except Exception as exc:  # keep going for remaining projects
            upsert_sync_state(
                project,
                LAST_PIPELINE_STATUS="error",
                LAST_PIPELINE_MESSAGE=str(exc)[:4000],
            )
            summary.append({"project": project, "action": "error", "error": str(exc)})
    return summary


if __name__ == "__main__":
    import json
    import sys

    force_flag = "--force" in sys.argv
    names = [a for a in sys.argv[1:] if not a.startswith("--")] or None
    print(json.dumps(morning_refresh(names, force=force_flag), default=str, indent=2))
