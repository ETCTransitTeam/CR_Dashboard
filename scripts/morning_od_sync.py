#!/usr/bin/env python3
"""
Headless morning OD Collection sync — same work as the dashboard Sync button,
run project-by-project without Streamlit UI.

Usage (from repo root, with conda/venv activated):
  python scripts/morning_od_sync.py --dry-run
  python scripts/morning_od_sync.py --project Oahu_Honolulu
  python scripts/morning_od_sync.py
  python scripts/morning_od_sync.py --include-hidden
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
    # Some helpers imported star-style may still reference utils/streamlit; patch common modules.
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
    args = parser.parse_args()

    _log("Morning OD sync starting")
    _patch_streamlit_for_headless()

    projects = _project_list(include_hidden=args.include_hidden)
    if args.project:
        match = [(n, s) for n, s in projects if n.lower() == args.project.lower()]
        if not match:
            # Allow syncing a hidden project by explicit name
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

    from od_sync_lock import release_sync_lock, sync_lock_holder, try_acquire_sync_lock

    try:
        lock_fd = try_acquire_sync_lock("morning_od_sync")
    except BlockingIOError:
        _log(
            "SKIP: another OD sync is already running "
            f"(holder={sync_lock_holder()!r}). Exiting without changes."
        )
        return 0
    atexit.register(release_sync_lock, lock_fd)

    ok = 0
    failed: list[tuple[str, str]] = []
    for name, schema in projects:
        try:
            sync_one(name, schema)
            ok += 1
        except Exception as exc:
            failed.append((name, str(exc)))
            _log(f"FAIL  sync: {name}: {exc}")
            traceback.print_exc()

    try:
        from authentication.auth import clear_landing_stats_cache, clear_projects_cache

        clear_projects_cache()
        clear_landing_stats_cache()
        _log("Cleared OD projects/landing caches")
    except Exception as exc:
        _log(f"Cache clear skipped: {exc}")

    _log(f"Finished. ok={ok} failed={len(failed)} total={len(projects)}")
    for name, err in failed:
        _log(f"  FAILED {name}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
