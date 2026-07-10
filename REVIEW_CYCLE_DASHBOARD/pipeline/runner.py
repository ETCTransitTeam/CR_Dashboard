from __future__ import annotations

import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import PIPELINE_DIR, ROOT_DIR, SCRIPTS_DIR, WORKSPACE_DIR, s3_enabled
from core.projects import get_project
from core.s3_utils import _local_kingelvis_path
from pipeline.header_mapping import MAPPING_FILENAME, write_header_mapping_xlsx


@dataclass
class PipelineContext:
    project_name: str
    pipeline_project_code: str
    workspace: Path
    today_date: str
    batch_id: str
    elvis_csv_name: str
    kingelvis_auto_csv: str
    kingelvis_xlsx: str
    elvis_table_csv: str
    main_table_csv: str | None
    details_file: str
    mapping_file: str = MAPPING_FILENAME
    elvis_database: str | None = None
    elvis_table: str | None = None
    main_database: str | None = None
    main_table: str | None = None


def cleanup_workspace(ctx: PipelineContext) -> None:
    """Remove ephemeral pipeline run directory after outputs are ingested."""
    try:
        if ctx.workspace.exists():
            shutil.rmtree(ctx.workspace, ignore_errors=True)
    except Exception:
        pass


def build_context(project_name: str | None) -> PipelineContext:
    if not project_name:
        raise ValueError(
            "No project selected. Open Sync & Admin, choose a project, "
            "or click 'Refresh projects from APP_CONFIG' if the list is empty."
        )
    project = get_project(project_name)
    if not project:
        raise ValueError(f"Project {project_name} not found in REVIEW_CYCLE.PROJECTS")
    today = "".join(str(date.today()).split("-"))
    pipeline_code = project.get("PIPELINE_PROJECT_CODE") or project_name.upper().replace(" ", "_")
    elvis_table = project.get("ELVIS_TABLE") or ""
    elvis_csv = f"{elvis_table}.csv" if elvis_table else f"elvis_transit_ls6_export_odbc.csv"
    main_table = project.get("MAIN_TABLE")
    main_csv = f"{main_table}.csv" if main_table else None
    kingelvis_xlsx = Path(project.get("KINGELVIS_FILE_NAME") or f"{project_name}_KINGElvis.xlsx").name
    kingelvis_auto_csv = f"{pipeline_code}_KINGElvis_auto_approval_{today}.csv"
    WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix=f"{project_name}_{today}_", dir=WORKSPACE_DIR))
    return PipelineContext(
        project_name=project_name,
        pipeline_project_code=pipeline_code,
        workspace=workspace,
        today_date=today,
        batch_id=f"{pipeline_code}_{today}",
        elvis_csv_name=elvis_csv,
        kingelvis_auto_csv=kingelvis_auto_csv,
        kingelvis_xlsx=kingelvis_xlsx,
        elvis_table_csv=elvis_csv,
        main_table_csv=main_csv,
        details_file=project.get("DETAILS_FILE_NAME") or "details_od_excel.xlsx",
        elvis_database=project.get("ELVIS_DATABASE"),
        elvis_table=project.get("ELVIS_TABLE"),
        main_database=project.get("MAIN_DATABASE"),
        main_table=project.get("MAIN_TABLE"),
    )


def _patch_script_content(source: Path, replacements: dict[str, str], literal_replacements: dict[str, str] | None = None) -> str:
    text = source.read_text(encoding="utf-8")
    for key, value in replacements.items():
        text = re.sub(rf"(?m)^({re.escape(key)}\s*=\s*).*$", rf"\1{repr(value)}", text, count=1)
    for old, new in (literal_replacements or {}).items():
        text = text.replace(old, new)
    return text


def _resolve_main_table_csv(ctx: PipelineContext) -> str | None:
    if ctx.main_table_csv:
        return ctx.main_table_csv
    match = re.match(r"elvis_(transit_ls6_\d+_export_odbc)\.csv", ctx.elvis_csv_name)
    if match:
        return f"{match.group(1)}.csv"
    return None


def _apply_project_file_paths(text: str, ctx: PipelineContext) -> str:
    """Replace hardcoded sample Elvis/details paths from HRTVA scripts with this project's files."""
    text = re.sub(
        r"(['\"])elvis_transit_ls6_\d+_export_odbc\.csv\1",
        rf"\1{ctx.elvis_csv_name}\1",
        text,
    )
    main_csv = _resolve_main_table_csv(ctx)
    if main_csv:
        text = re.sub(
            r"(['\"])transit_ls6_\d+_export_odbc\.csv\1",
            rf"\1{main_csv}\1",
            text,
        )
    text = re.sub(
        r"(['\"])details_[A-Za-z0-9_\-]+od_excel(?:_\d+)?\.xlsx\1",
        rf"\1{ctx.details_file}\1",
        text,
    )
    return text


def _prepare_script(
    ctx: PipelineContext,
    script_name: str,
    replacements: dict[str, str],
    literal_replacements: dict[str, str] | None = None,
) -> Path:
    scripts_dir = ctx.workspace / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    destination = scripts_dir / script_name
    text = _patch_script_content(SCRIPTS_DIR / script_name, replacements, literal_replacements)
    text = _apply_project_file_paths(text, ctx)
    text = _sanitize_script_console_unicode(text)
    destination.write_text(text, encoding="utf-8")
    return destination


def _subprocess_env() -> dict[str, str]:
    """Force UTF-8 in child Python processes (avoids cp1252 UnicodeEncodeError on Windows)."""
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


def _sanitize_script_console_unicode(text: str) -> str:
    """Replace symbols that break Windows cp1252 consoles when scripts print debug output."""
    replacements = {
        "\u2192": "->",
        "\u2190": "<-",
        "\u2713": "[ok]",
        "\u2717": "[x]",
        "\u2705": "[ok]",
        "\u274c": "[x]",
        "\U0001f6a9": "[flag]",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def _run_script(script_path: Path, cwd: Path, extra_args: list[str] | None = None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(script_path)]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_subprocess_env(),
        check=False,
    )


def _stage_script_support_files(ctx: PipelineContext) -> None:
    """Copy helper modules next to patched scripts so optional imports resolve."""
    scripts_dir = ctx.workspace / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    for helper in ("database.py", "constants.py", "suggest_transfer_routes.py"):
        source = SCRIPTS_DIR / helper
        if source.exists():
            shutil.copy2(source, scripts_dir / helper)


def _fetch_mysql_csv(database: str, table: str, output_path: Path) -> None:
    from core.config import env

    host = env("SQL_HOST")
    user = env("SQL_USER")
    password = env("SQL_PASSWORD")
    if not all([host, user, password]):
        raise RuntimeError("MySQL credentials missing in .env")
    sys.path.insert(0, str(SCRIPTS_DIR))
    from database import DatabaseConnector

    connector = DatabaseConnector(host, database, user, password)
    connector.connect()
    try:
        cur = connector.connection.cursor()
        cur.execute(f"SELECT * FROM {table}")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)
    finally:
        connector.disconnect()
    df.to_csv(output_path, index=False)


def _input_search_dirs() -> list[Path]:
    from core.config import PIPELINE_INPUTS_DIR

    return [
        PIPELINE_DIR / "shared_inputs",
        ROOT_DIR.parent / "HRTVA_AUTOMATION",
        PIPELINE_INPUTS_DIR,
    ]


def _list_available_inputs() -> list[str]:
    found: list[str] = []
    for directory in _input_search_dirs():
        if directory.is_dir():
            for path in sorted(directory.glob("*.xlsx")):
                found.append(f"{path.name}  ({directory})")
    return found


def _resolve_input_source(filename: str) -> Path | None:
    """Find pipeline input by exact name, case-insensitive match, or details-file pattern."""
    name = Path(filename).name
    stem = Path(name).stem.lower()

    for directory in _input_search_dirs():
        if not directory.is_dir():
            continue
        exact = directory / name
        if exact.exists():
            return exact
        for path in directory.glob("*.xlsx"):
            if path.name.lower() == name.lower():
                return path

    if stem.startswith("details_"):
        prefix = stem.rsplit("_", 1)[0] if "_" in stem else stem
        for directory in _input_search_dirs():
            if not directory.is_dir():
                continue
            matches = sorted(directory.glob(f"{prefix}*.xlsx"), key=lambda p: p.name.lower())
            if len(matches) == 1:
                return matches[0]
            for path in matches:
                if path.stem.lower().startswith(prefix):
                    return path

    return None


def _candidate_input_paths(filename: str) -> list[Path]:
    name = Path(filename).name
    return [directory / name for directory in _input_search_dirs()]


def _stage_required_file(ctx: PipelineContext, filename: str) -> Path:
    """Copy or download a required pipeline input into the run workspace."""
    dest = ctx.workspace / Path(filename).name
    if dest.exists():
        return dest

    resolved = _resolve_input_source(filename)
    if resolved is not None:
        shutil.copy2(resolved, dest)
        return dest

    if s3_enabled():
        from core.config import BUCKET_NAME
        from core.s3_utils import download_pipeline_input_from_s3

        try:
            s3_key = download_pipeline_input_from_s3(BUCKET_NAME, filename, dest)
            return dest
        except Exception as exc:
            s3_error = str(exc).strip()
    else:
        s3_error = None

    searched = "\n  - ".join(str(p) for p in _candidate_input_paths(filename))
    available = _list_available_inputs()
    available_hint = (
        "\n\nFiles found in input folders:\n  - " + "\n  - ".join(available)
        if available
        else ""
    )
    near_match_hint = ""
    configured = Path(filename).name.lower()
    for line in available:
        on_disk = line.split("  (")[0].strip().lower()
        if on_disk.startswith("details_") and configured.startswith("details_"):
            if on_disk.rsplit("_", 1)[0] == configured.rsplit("_", 1)[0] and on_disk != configured:
                near_match_hint = (
                    f"\n\nLikely filename mismatch: config expects `{Path(filename).name}` "
                    f"but you have `{line.split('  (')[0].strip()}` in shared_inputs. "
                    "The runner will use the on-disk file if the prefix matches."
                )
                break

    s3_hint = ""
    if s3_enabled():
        s3_hint = f"\n  - s3://{BUCKET_NAME}/{Path(filename).name}"
        if s3_error:
            s3_hint += f" (S3 lookup failed: {s3_error})"
        else:
            s3_hint += " (S3 configured but file not found in bucket)"
    else:
        s3_hint = "\n  - Or configure S3 in .env (bucket_name, aws keys) to auto-download from the OD bucket"
    raise FileNotFoundError(
        f"Required pipeline file not found: {filename}\n"
        f"Place it in one of these locations:\n  - {searched}"
        f"{s3_hint}"
        f"{available_hint}"
        f"{near_match_hint}\n"
        f"Then re-run the pipeline."
    )


def _stage_header_mapping(ctx: PipelineContext) -> Path:
    """Build mapping xlsx from automated_sync_flow_constants_maps.KCATA_HEADER_MAPPING."""
    dest = ctx.workspace / ctx.mapping_file
    if not dest.exists():
        write_header_mapping_xlsx(dest)
    return dest


def stage_inputs(ctx: PipelineContext) -> None:
    if ctx.elvis_database and ctx.elvis_table:
        _fetch_mysql_csv(ctx.elvis_database, ctx.elvis_table, ctx.workspace / ctx.elvis_csv_name)
    if ctx.main_database and ctx.main_table and ctx.main_table_csv:
        _fetch_mysql_csv(ctx.main_database, ctx.main_table, ctx.workspace / ctx.main_table_csv)

    _stage_header_mapping(ctx)
    _stage_required_file(ctx, ctx.details_file)

    local_kingelvis = _local_kingelvis_path(ctx.kingelvis_xlsx)
    if local_kingelvis.exists():
        shutil.copy2(local_kingelvis, ctx.workspace / ctx.kingelvis_xlsx)


def _run_improved_auto_approval(
    ctx: PipelineContext,
    *,
    suggestion_ids_file: Path | None = None,
) -> Path:
    """Run improved_auto_approval.py and return the auto-approval CSV path."""
    cwd = ctx.workspace
    ia_script = _prepare_script(
        ctx,
        "improved_auto_approval.py",
        {
            "DEFAULT_INPUT": ctx.elvis_csv_name,
            "DEFAULT_NAME_KE": ctx.pipeline_project_code,
            "DEFAULT_MAPPING_FILE": ctx.mapping_file,
            "DEFAULT_DETAILS_FILE": ctx.details_file,
            "DEFAULT_SELECT_QUERY": f"SELECT * FROM {ctx.elvis_table or 'elvis_export'}",
            "DEFAULT_DB_NAME": ctx.elvis_database or "transit-ls6",
        },
    )
    extra_args = [
        ctx.elvis_csv_name,
        "-o",
        ctx.kingelvis_auto_csv,
        "--mapping",
        ctx.mapping_file,
        "--details",
        ctx.details_file,
        "--stops",
        ctx.details_file,
    ]
    if suggestion_ids_file is not None and suggestion_ids_file.exists():
        extra_args.extend(["--suggestion-ids-file", suggestion_ids_file.name])
    result = _run_script(ia_script, cwd, extra_args=extra_args)
    if result.returncode != 0:
        raise RuntimeError(f"improved_auto_approval failed: {result.stderr or result.stdout}")
    return cwd / ctx.kingelvis_auto_csv


def _write_new_suggestion_ids_file(ctx: PipelineContext) -> Path:
    """Write ids present in the Elvis export but not yet in RECORDS (suggestions only for these)."""
    from core.data_access import load_records

    dest = ctx.workspace / f"new_suggestion_ids_{ctx.today_date}.txt"
    elvis_path = ctx.workspace / ctx.elvis_csv_name
    if not elvis_path.exists():
        dest.write_text("", encoding="utf-8")
        return dest

    header = pd.read_csv(elvis_path, nrows=0)
    id_cols = [c for c in header.columns if str(c).lower() in {"id", "elvis_id"}]
    if not id_cols:
        dest.write_text("", encoding="utf-8")
        return dest
    elvis_df = pd.read_csv(elvis_path, low_memory=False, usecols=id_cols)
    id_col = id_cols[0]

    export_ids = {str(v).strip() for v in elvis_df[id_col].dropna().tolist() if str(v).strip()}
    existing = load_records(ctx.project_name)
    existing_ids = (
        set(existing["RECORD_ID"].astype(str).str.strip())
        if not existing.empty and "RECORD_ID" in existing.columns
        else set()
    )
    new_ids = sorted(export_ids - existing_ids)
    dest.write_text("\n".join(new_ids) + ("\n" if new_ids else ""), encoding="utf-8")
    return dest


def _shape_and_sort_elvis_review(source_csv: Path, sorted_csv: Path, kingelvis_path: Path) -> Path:
    """Shape + sort a pipeline CSV into Elvis_Review layout and write CSV/XLSX."""
    from pipeline.elvis_review_format import shape_to_elvis_review
    from pipeline.scripts.sort_improved_auto_approval_output import sort_dataframe as sort_elvis_review_rows

    sorted_full = pd.read_csv(source_csv, low_memory=False)
    elvis_review_df = shape_to_elvis_review(sorted_full)
    try:
        elvis_review_df = sort_elvis_review_rows(elvis_review_df)
    except SystemExit as exc:
        raise RuntimeError(
            "sort_improved_auto_approval_output failed to resolve Final_Usage / FINAL_REVIEWER columns"
        ) from exc
    elvis_review_df.to_csv(sorted_csv, index=False)
    with pd.ExcelWriter(kingelvis_path, engine="openpyxl", mode="w") as writer:
        elvis_review_df.to_excel(writer, sheet_name="Elvis_Review", index=False)
    return sorted_csv


def run_elvis_review_pipeline(ctx: PipelineContext, progress=None) -> dict[str, Path]:
    """Elvis Review only: auto-approval + sort/shape. Skips flag scripts for other pages."""
    from pipeline.progress import PipelineProgress

    prog = progress or PipelineProgress()
    prog.set_total(3)
    prog.update(1, "Gathering the latest survey exports…")
    stage_inputs(ctx)
    _stage_script_support_files(ctx)
    cwd = ctx.workspace
    suggestion_ids = _write_new_suggestion_ids_file(ctx)
    prog.update(2, "Reviewing new trips and transfer ideas…")
    auto_csv = _run_improved_auto_approval(ctx, suggestion_ids_file=suggestion_ids)
    sorted_csv = cwd / f"sorted_{ctx.pipeline_project_code}_elvis_review_{ctx.today_date}.csv"
    kingelvis_path = cwd / ctx.kingelvis_xlsx
    prog.update(3, "Polishing your Elvis Review queue…")
    _shape_and_sort_elvis_review(auto_csv, sorted_csv, kingelvis_path)
    return {
        "auto_approval_csv": auto_csv,
        "sorted_elvis_review_csv": sorted_csv,
        "kingelvis_xlsx": kingelvis_path,
    }


def run_auto_approval_pipeline(ctx: PipelineContext, progress=None) -> dict[str, Path]:
    """Full weekly auto path: Elvis Review + distance/transfer flag scripts + combine."""
    from pipeline.progress import PipelineProgress

    prog = progress or PipelineProgress()
    steps = [
        "Gathering the latest survey exports…",
        "Reviewing new trips and transfer ideas…",
        "Checking how far origins and destinations sit…",
        "Looking over transfer patterns…",
        "Measuring transfer walk distances…",
        "Confirming boarding and alighting stops…",
        "Weaving everything into one review set…",
        "Polishing your Elvis Review queue…",
    ]
    prog.set_total(len(steps))

    prog.update(1, steps[0])
    stage_inputs(ctx)
    _stage_script_support_files(ctx)
    cwd = ctx.workspace
    common = {
        "project_name": ctx.pipeline_project_code,
        "file_name": ctx.kingelvis_auto_csv,
        "file_path": ctx.details_file,
        "today_date": ctx.today_date,
    }

    suggestion_ids = _write_new_suggestion_ids_file(ctx)
    prog.update(2, steps[1])
    _run_improved_auto_approval(ctx, suggestion_ids_file=suggestion_ids)

    auto_scripts = [
        ("od_distance_checks_auto_approval.py", steps[2]),
        ("traditional_transfer_checks_auto_approval.py", steps[3]),
        ("transfer_distance_flags_auto_approval.py", steps[4]),
        ("directional_stops_flags_auto_approval.py", steps[5]),
    ]
    for idx, (script, label) in enumerate(auto_scripts, start=3):
        prog.update(idx, label)
        patched = _prepare_script(ctx, script, common)
        result = _run_script(patched, cwd)
        if result.returncode != 0:
            raise RuntimeError(f"{script} failed: {result.stderr or result.stdout}")

    prog.update(7, steps[6])
    combining = _prepare_script(ctx, "combining_distance_flags_auto_approval.py", common)
    result = _run_script(combining, cwd)
    if result.returncode != 0:
        raise RuntimeError(f"combining_distance_flags_auto_approval failed: {result.stderr or result.stdout}")

    combined_csv = cwd / f"reviewtool_{ctx.today_date}_{ctx.pipeline_project_code}_combinedflags_auto_approved.csv"
    sorted_csv = cwd / f"sorted_reviewtool_{ctx.today_date}_{ctx.pipeline_project_code}_combinedflags_auto_approved.csv"
    sort_script = _prepare_script(
        ctx,
        "sort_improved_auto_approval_output.py",
        {"DEFAULT_INPUT_PATH": combined_csv.name, "DEFAULT_EXCEL_SHEET": "Elvis_Review"},
    )
    prog.update(8, steps[7])
    sort_result = _run_script(
        sort_script,
        cwd,
        extra_args=["-i", combined_csv.name, "-o", sorted_csv.name],
    )
    if sort_result.returncode != 0:
        detail = (sort_result.stderr or sort_result.stdout or "").strip()
        print(
            f"sort_improved_auto_approval_output subprocess failed (exit {sort_result.returncode}); "
            f"will sort in-process. {detail[-500:]}",
            file=sys.stderr,
        )

    source_csv = sorted_csv if sort_result.returncode == 0 and sorted_csv.exists() else combined_csv
    kingelvis_path = cwd / ctx.kingelvis_xlsx
    _shape_and_sort_elvis_review(source_csv, sorted_csv, kingelvis_path)

    return {
        "auto_approval_csv": cwd / ctx.kingelvis_auto_csv,
        "combined_auto_csv": combined_csv,
        "sorted_elvis_review_csv": sorted_csv,
        "kingelvis_xlsx": kingelvis_path,
    }


def run_post_cleaning_pipeline(ctx: PipelineContext) -> dict[str, Path]:
    stage_inputs(ctx)
    _stage_script_support_files(ctx)
    cwd = ctx.workspace
    _ensure_kingelvis_workbook(ctx)

    common = {
        "project_name": ctx.pipeline_project_code,
        "file_name": ctx.kingelvis_xlsx,
        "file_path": ctx.details_file,
        "today_date": ctx.today_date,
    }
    for script in [
        "od_distance_checks.py",
        "traditional_transfer_checks.py",
        "transfer_distance_flags.py",
        "directional_stops_flags.py",
    ]:
        patched = _prepare_script(ctx, script, common)
        result = _run_script(patched, cwd)
        if result.returncode != 0:
            raise RuntimeError(f"{script} failed: {result.stderr or result.stdout}")

    combining = _prepare_script(ctx, "combining_distance_flags.py", common)
    result = _run_script(combining, cwd)
    if result.returncode != 0:
        raise RuntimeError(f"combining_distance_flags failed: {result.stderr or result.stdout}")

    _run_reviewer_stats_script(ctx)

    removed_script = _prepare_script(
        ctx,
        "Removed_ids_field_team.py",
        {"project_name": ctx.pipeline_project_code},
        literal_replacements={
            "elvis_transit_ls6_733524_export_odbc.csv": ctx.elvis_csv_name,
            "LACMTA_FEEDER_2025_KINGElvis.xlsx": ctx.kingelvis_xlsx,
        },
    )
    result = _run_script(removed_script, cwd)
    if result.returncode != 0:
        raise RuntimeError(f"Removed_ids_field_team failed: {result.stderr or result.stdout}")

    return {
        "combined_flags_csv": cwd / f"reviewtool_{ctx.today_date}_{ctx.pipeline_project_code}_combinedflags.csv",
        "reviewer_stats_xlsx": cwd / f"reviewtool_{ctx.today_date}_{ctx.pipeline_project_code}_reviewerstats.xlsx",
        "removed_ids_xlsx": cwd / f"{ctx.pipeline_project_code}_Removed_or_Deleted_Records_by_{ctx.today_date}.xlsx",
    }


def _enrich_kingelvis_completed(ctx: PipelineContext, kingelvis_path: Path) -> None:
    """Ensure Elvis_Review has Completed (reviewer_stats_kcata expects it)."""
    df = pd.read_excel(kingelvis_path, sheet_name="Elvis_Review")
    if "Completed" in df.columns and df["Completed"].notna().any():
        return
    for col in ("DATE_SUBMITTED", "Date_submitted", "DATE", "Elvis_Date"):
        if col in df.columns and df[col].notna().any():
            df["Completed"] = df[col]
            df.to_excel(kingelvis_path, sheet_name="Elvis_Review", index=False)
            return
    elvis_path = ctx.workspace / ctx.elvis_csv_name
    if not elvis_path.exists():
        return
    elvis = pd.read_csv(elvis_path, low_memory=False)
    date_col = next(
        (c for c in elvis.columns if c.lower() in ("date_submitted", "date_started", "completed")),
        None,
    )
    if not date_col or "id" not in elvis.columns or "id" not in df.columns:
        return
    dates = elvis[["id", date_col]].drop_duplicates(subset=["id"], keep="first")
    dates["id"] = dates["id"].astype(str).str.strip()
    df["id"] = df["id"].astype(str).str.strip()
    merged = df.merge(dates.rename(columns={date_col: "Completed_elvis"}), on="id", how="left")
    if "Completed" in merged.columns:
        merged["Completed"] = merged["Completed"].combine_first(merged["Completed_elvis"])
    else:
        merged["Completed"] = merged["Completed_elvis"]
    merged = merged.drop(columns=["Completed_elvis"])
    merged.to_excel(kingelvis_path, sheet_name="Elvis_Review", index=False)


def _ensure_kingelvis_workbook(ctx: PipelineContext) -> Path:
    kingelvis_path = ctx.workspace / ctx.kingelvis_xlsx
    if not kingelvis_path.exists():
        from core.data_access import build_elvis_review_export

        export_df = build_elvis_review_export(ctx.project_name)
        export_df.to_excel(kingelvis_path, sheet_name="Elvis_Review", index=False)
    _enrich_kingelvis_completed(ctx, kingelvis_path)
    return kingelvis_path


def _extract_script_failure(detail: str) -> str:
    """Pull the most useful exception line from a script traceback."""
    if not detail:
        return "unknown error"
    for line in reversed(detail.splitlines()):
        text = line.strip()
        if not text:
            continue
        if text.startswith(("File ", "Traceback", "The above exception", "^", "~")):
            continue
        if "Error" in text or "Exception" in text:
            return text[:300]
    return detail.splitlines()[-1].strip()[:300]


def _run_reviewer_stats_script(ctx: PipelineContext) -> None:
    """Run reviewer_stats_kcata.py only (patched copy from pipeline/scripts)."""
    cwd = ctx.workspace
    stats_script = _prepare_script(
        ctx,
        "reviewer_stats_kcata.py",
        {
            "project_name": ctx.pipeline_project_code,
            "file_name": ctx.kingelvis_xlsx,
        },
        literal_replacements={
            "elvis_transit_ls6_733524_export_odbc.csv": ctx.elvis_csv_name,
        },
    )
    main_csv = _resolve_main_table_csv(ctx)
    if main_csv:
        stats_script_text = stats_script.read_text(encoding="utf-8")
        # Only patch active (uncommented) assignments — commented examples come first.
        stats_script_text = re.sub(
            r"(?m)^baby_df\s*=\s*pd\.read_csv\(['\"][^'\"]+['\"]\)",
            f"baby_df=pd.read_csv('{main_csv}')",
            stats_script_text,
            count=1,
        )
        stats_script_text = re.sub(
            r"(?m)^main_df\s*=\s*pd\.read_csv\(['\"][^'\"]+['\"]\)",
            f"main_df=pd.read_csv('{main_csv}')",
            stats_script_text,
            count=1,
        )
        stats_script.write_text(stats_script_text, encoding="utf-8")
    result = _run_script(stats_script, cwd)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"Reviewer stats failed: {_extract_script_failure(detail)}")

def run_reviewer_stats_pipeline(ctx: PipelineContext) -> dict[str, Path]:
    """Stage inputs and run reviewer_stats_kcata.py without the flag scripts."""
    stage_inputs(ctx)
    _stage_script_support_files(ctx)
    _ensure_kingelvis_workbook(ctx)
    _run_reviewer_stats_script(ctx)
    return {
        "reviewer_stats_xlsx": ctx.workspace
        / f"reviewtool_{ctx.today_date}_{ctx.pipeline_project_code}_reviewerstats.xlsx",
    }


def _demographics_output_prefix(ctx: PipelineContext) -> str:
    """First token of KingElvis filename (matches od_demographics_checks file_first_name)."""
    stem = Path(ctx.kingelvis_xlsx).stem
    if stem.split("_")[0].isdigit():
        return stem.split("_")[0] + "_" + stem.split("_")[1]
    return stem.split("_")[0]


def _resolve_csv_input(filename: str) -> Path | None:
    """Find a pipeline CSV input by exact name in shared input folders."""
    name = Path(filename).name
    for directory in _input_search_dirs():
        if not directory.is_dir():
            continue
        exact = directory / name
        if exact.exists():
            return exact
        for path in directory.glob("*.csv"):
            if path.name.lower() == name.lower():
                return path
    return None


def _stage_elvis_export_for_demographics(ctx: PipelineContext) -> Path:
    """Stage full Elvis ODBC export (demographic source fields) for od_demographics_checks."""
    dest = ctx.workspace / ctx.elvis_csv_name
    if dest.exists():
        return dest

    from core.data_access import resolve_elvis_export_path

    staged = resolve_elvis_export_path(ctx.project_name)
    if staged is not None and staged.exists():
        shutil.copy2(staged, dest)
        return dest

    if ctx.elvis_database and ctx.elvis_table:
        _fetch_mysql_csv(ctx.elvis_database, ctx.elvis_table, dest)
        return dest

    resolved = _resolve_csv_input(ctx.elvis_csv_name)
    if resolved is not None:
        shutil.copy2(resolved, dest)
        return dest

    if ctx.elvis_table:
        table_token = ctx.elvis_table.lower()
        for directory in _input_search_dirs():
            if not directory.is_dir():
                continue
            for path in sorted(directory.glob("elvis*.csv"), key=lambda p: p.name.lower()):
                if table_token in path.stem.lower() or "export_odbc" in path.stem.lower():
                    shutil.copy2(path, dest)
                    return dest

    searched = "\n  - ".join(str(d / ctx.elvis_csv_name) for d in _input_search_dirs())
    raise FileNotFoundError(
        f"Elvis export CSV not found for demographics ({ctx.elvis_csv_name}).\n"
        f"Run Sync & Admin pipeline once for {ctx.project_name}, or place the export in:\n  - {searched}\n"
        "Demographics checks need the full ODBC export (StudentStatusCode, YourAge, etc.), "
        "not just the Elvis Review grid columns."
    )


def _stage_kingelvis_from_review_db(ctx: PipelineContext) -> Path:
    """Build KingElvis Elvis_Review sheet from dashboard records (Final_Usage, reviewers, etc.)."""
    from core.data_access import build_elvis_review_export

    kingelvis_path = ctx.workspace / ctx.kingelvis_xlsx
    export_df = build_elvis_review_export(ctx.project_name)
    if export_df.empty:
        raise ValueError(
            f"No Elvis Review records in the database for {ctx.project_name}. "
            "Run Sync & Admin or ingest records before Demographic Review."
        )
    export_df.to_excel(kingelvis_path, sheet_name="Elvis_Review", index=False)
    return kingelvis_path


def run_demographics_pipeline(ctx: PipelineContext) -> dict[str, Path]:
    """Run HRTVA od_demographics_checks.py using Elvis Review (DB) + Elvis ODBC export."""
    ctx.workspace.mkdir(parents=True, exist_ok=True)
    _stage_elvis_export_for_demographics(ctx)
    _stage_kingelvis_from_review_db(ctx)
    _stage_script_support_files(ctx)
    cwd = ctx.workspace
    kingelvis_path = cwd / ctx.kingelvis_xlsx

    prefix = _demographics_output_prefix(ctx)
    demo_csv = cwd / f"{prefix}_DemoGraphic_Checks(01).csv"

    patched = _prepare_script(
        ctx,
        "od_demographics_checks.py",
        {
            "file_name": ctx.kingelvis_xlsx,
        },
        literal_replacements={
            "elvislacmta2023obweekday_export_odbc(new2).csv": ctx.elvis_csv_name,
            "LACMTA_KINGElvis (3).xlsx": ctx.kingelvis_xlsx,
        },
    )
    result = _run_script(patched, cwd)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"od_demographics_checks failed (exit {result.returncode}).\n{detail[-2000:]}"
        )
    if not demo_csv.exists():
        matches = list(cwd.glob("*_DemoGraphic_Checks(01).csv"))
        if matches:
            demo_csv = matches[0]
        else:
            raise FileNotFoundError(
                f"Demographics output not found (expected {demo_csv.name}). "
                f"stdout: {result.stdout[-500:] if result.stdout else ''}"
            )
    return {"demographics_csv": demo_csv}


def run_full_pipeline(project_name: str, phase: str = "auto", progress=None) -> dict[str, Any]:
    ctx = build_context(project_name)
    outputs: dict[str, Any] = {"context": ctx, "phase": phase}
    if phase in ("elvis",):
        # Elvis Review page only — skip flag scripts that feed Combined Checks / other pages.
        outputs["auto"] = run_elvis_review_pipeline(ctx, progress=progress)
    if phase in ("auto", "full"):
        outputs["auto"] = run_auto_approval_pipeline(ctx, progress=progress)
    if phase in ("flags", "full"):
        outputs["flags"] = run_post_cleaning_pipeline(ctx)
    if phase in ("stats",):
        outputs["stats"] = run_reviewer_stats_pipeline(ctx)
    if phase in ("demographics", "full"):
        outputs["demographics"] = run_demographics_pipeline(ctx)
    return outputs
