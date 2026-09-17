#!/usr/bin/env python3
"""
Headless morning OD Collection sync — same work as the dashboard Sync button,
run project-by-project without Streamlit UI.

Usage (from repo root, with conda/venv activated):
  python scripts/morning_od_sync.py --dry-run
  python scripts/morning_od_sync.py --project Oahu_Honolulu
  python scripts/morning_od_sync.py
  python scripts/morning_od_sync.py --include-hidden
  python scripts/morning_od_sync.py --force   # ignore morning_sync_enabled=off
"""

from __future__ import annotations

import argparse
import atexit
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
os.chdir(REPO_ROOT)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TZ = ZoneInfo("America/Chicago")

# Projects hidden from the main frontend; skipped unless --include-hidden.
HIDDEN_PROJECTS = frozenset({
    "LACMTA_FEEDER",
    "ACTRANSIT",
    "SALEM",
    "PARKCITY",
})


class _HeadlessStreamlit:
    """Minimal stand-in so fetch_and_process_data can run outside `streamlit run`."""

    def __init__(self) -> None:
        self.session_state: dict = {}

    def error(self, *args, **kwargs) -> None:
        print("[st.error]", *args, flush=True)

    def warning(self, *args, **kwargs) -> None:
        print("[st.warning]", *args, flush=True)

    def info(self, *args, **kwargs) -> None:
        print("[st.info]", *args, flush=True)

    def success(self, *args, **kwargs) -> None:
        print("[st.success]", *args, flush=True)

    def write(self, *args, **kwargs) -> None:
        print("[st.write]", *args, flush=True)


def _log(msg: str) -> None:
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{now}] {msg}", flush=True)


def _patch_streamlit_for_headless() -> None:
    """Replace Streamlit UI calls used by the sync pipeline with print stubs."""
    import automated_refresh_flow_new as refresh

    shim = _HeadlessStreamlit()
    refresh.st = shim
    try:
        import automated_sync_flow_utils as sync_utils

        sync_utils.st = shim
    except Exception:
        pass
    try:
        import utils

        utils.st = shim
    except Exception:
        pass


def _project_list(include_hidden: bool) -> list[tuple[str, str]]:
    from automated_refresh_flow_new import PROJECTS, refresh_projects

    refresh_projects()
    rows: list[tuple[str, str]] = []
    for name, cfg in sorted(PROJECTS.items(), key=lambda item: item[0].lower()):
        if not include_hidden and name in HIDDEN_PROJECTS:
            continue
        schema = (cfg or {}).get("schema") or name
        rows.append((name, str(schema)))
    return rows


def sync_one(project: str, schema: str) -> str:
    from automated_refresh_flow_new import fetch_and_process_data

    _log(f"START sync: {project} → schema {schema}")
    started = time.time()
    result = fetch_and_process_data(project, schema)
    elapsed = time.time() - started
    label = result if isinstance(result, str) else "OK"
    _log(f"DONE  sync: {project} ({label}) in {elapsed:.1f}s")
    return label


def main() -> int:
    parser = argparse.ArgumentParser(description="Morning OD Collection sync (all projects)")
    parser.add_argument("--project", help="Sync only this project name")
    parser.add_argument("--dry-run", action="store_true", help="List projects only; do not sync")
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Also sync frontend-hidden projects (LACMTA_FEEDER, ACTRANSIT, …)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even when morning auto-sync is disabled in Sync History settings",
    )
    args = parser.parse_args()

    _log("Morning OD sync starting")
    _patch_streamlit_for_headless()

    projects = _project_list(include_hidden=args.include_hidden)
    if args.project:
        match = [(n, s) for n, s in projects if n.lower() == args.project.lower()]
        if not match:
            from automated_refresh_flow_new import PROJECTS, refresh_projects

            refresh_projects()
            key = next((k for k in PROJECTS if k.lower() == args.project.lower()), None)
            if not key:
                _log(f"ERROR: project {args.project!r} not found in active PROJECT_CONFIGS")
                return 1
            projects = [(key, str(PROJECTS[key].get("schema") or key))]
        else:
            projects = match

    _log(f"Projects queued: {len(projects)}")
    for name, schema in projects:
        _log(f"  - {name} ({schema})")

    if args.dry_run:
        _log("Dry run only — exiting")
        return 0

    from od_sync_history import (
        finish_project_row,
        finish_run,
        is_morning_sync_enabled,
        send_run_alert_digest,
        start_project_row,
        start_run,
    )
    from od_sync_lock import release_sync_lock, sync_lock_holder, try_acquire_sync_lock

    trigger = "manual_cli" if args.project else "morning"

    if not args.force and not is_morning_sync_enabled():
        run_id = start_run(
            trigger=trigger,
            project_total=0,
            actor="morning_od_sync",
            notes="Morning sync disabled by admin",
            status="skipped",
        )
        finish_run(run_id, status="skipped", project_ok=0, project_failed=0)
        _log("SKIP: morning auto-sync is disabled (Sync History). Use --force to override.")
        return 0

    try:
        lock_fd = try_acquire_sync_lock("morning_od_sync")
    except BlockingIOError:
        _log(
            "SKIP: another OD sync is already running "
            f"(holder={sync_lock_holder()!r}). Exiting without changes."
        )
        return 0
    atexit.register(release_sync_lock, lock_fd)

    run_id = start_run(
        trigger=trigger,
        project_total=len(projects),
        actor="morning_od_sync",
    )
    ok = 0
    failed: list[tuple[str, str]] = []
    alert_failures: list[dict] = []
    alert_drops: list[dict] = []

    for name, schema in projects:
        start_project_row(run_id, name, schema)
        t0 = time.time()
        try:
            label = sync_one(name, schema)
            meta = finish_project_row(
                run_id,
                name,
                status="success",
                started_monotonic=t0,
                result_label=label,
                schema_name=schema,
            )
            ok += 1
            if meta.get("alert_drop"):
                alert_drops.append(meta)
                _log(
                    f"DROP alert: {name} drop={meta.get('drop_pct'):.2f}% "
                    f"(threshold {meta.get('threshold')}%)"
                )
        except Exception as exc:
            failed.append((name, str(exc)))
            detail = traceback.format_exc()
            meta = finish_project_row(
                run_id,
                name,
                status="failed",
                started_monotonic=t0,
                error_message=str(exc),
                error_detail=detail,
                schema_name=schema,
                collect_counts=False,
            )
            alert_failures.append(meta)
            _log(f"FAIL  sync: {name}: {exc}")
            traceback.print_exc()

    if failed and ok:
        status = "partial"
    elif failed:
        status = "failed"
    else:
        status = "success"
    finish_run(
        run_id,
        status=status,
        project_ok=ok,
        project_failed=len(failed),
    )

    try:
        send_run_alert_digest(
            run_id=run_id,
            trigger=trigger,
            failures=alert_failures,
            drops=alert_drops,
        )
    except Exception as exc:
        _log(f"Alert digest skipped: {exc}")

    try:
        from authentication.auth import clear_landing_stats_cache, clear_projects_cache

        clear_projects_cache()
        clear_landing_stats_cache()
        _log("Cleared OD projects/landing caches")
    except Exception as exc:
        _log(f"Cache clear skipped: {exc}")

    _log(f"Finished. ok={ok} failed={len(failed)} total={len(projects)} run_id={run_id}")
    for name, err in failed:
        _log(f"  FAILED {name}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
