"""Per-user expansion runs. The calculation modules are called unchanged."""

from __future__ import annotations

import copy
import io
import json
import os
import zipfile
from pathlib import Path
import tempfile

from expansion import adjustments, engine, exports

ROOT = Path(__file__).resolve().parent
MAX_UPLOAD_BYTES = 40 * 1024 * 1024


def load_legacy() -> tuple[dict | None, str | None]:
    """Optional reviewed-adjustments JSON. Absent file means the option stays off."""
    path = (os.environ.get("ETC_REVIEWED_ADJUSTMENTS") or "").strip()
    if not path:
        return None, None
    file = Path(path)
    if not file.is_file():
        return None, "Reviewed adjustments are configured, but the file could not be read."
    try:
        payload = json.loads(file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, "Reviewed adjustments are configured, but the file could not be read."
    if not isinstance(payload, dict):
        return None, "Reviewed adjustments are configured, but the file could not be read."
    return payload, None


def default_workbook() -> Path | None:
    path = Path((os.environ.get("ETC_DEFAULT_WORKBOOK") or "").strip())
    if str(path) and path.is_file():
        return path
    return None


def run_workbook(
    data: bytes,
    name: str,
    rate: float,
    auto: bool,
    use_legacy: bool,
    legacy: dict | None,
) -> dict:
    suffix = Path(name).suffix.lower()
    if suffix != ".xlsx":
        raise ValueError("Use an .xlsx workbook with O2O, OD, APC, and SAMPLING sheets.")
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValueError("Input file is larger than 40 MB.")
    rate = float(rate)
    with tempfile.TemporaryDirectory(prefix="expansion-input-") as tmp:
        path = Path(tmp) / f"workbook{suffix}"
        path.write_bytes(data)
        tables = engine.read_workbook(path)
    raw = engine.expand(
        tables,
        rate,
        project_rate_override=True,
        allow_incomplete_apc=True,
    )
    applied = legacy if use_legacy else None
    result = adjustments.finalize(copy.deepcopy(raw), tables, applied, bool(auto))
    return {
        "tables": tables,
        "raw": raw,
        "result": result,
        "manual": [],
        "existing": bool(use_legacy and applied is not None),
        "auto": bool(auto),
        "legacy": applied if use_legacy else None,
        "input_name": name,
        "input_size": len(data),
        "rate": rate,
        "rev": 1,
    }


def apply_rules(state: dict, rules: list) -> dict:
    result = adjustments.finalize(
        copy.deepcopy(state["raw"]),
        state["tables"],
        state.get("legacy") if state.get("existing") else None,
        bool(state.get("auto")),
        rules,
    )
    updated = dict(state)
    updated["manual"] = rules
    updated["result"] = result
    updated["rev"] = int(state.get("rev") or 0) + 1
    return updated


def adjust(state: dict, payload: dict) -> dict:
    rules = copy.deepcopy(state.get("manual") or [])
    if payload.get("clear"):
        rules = []
    else:
        members = payload.get("members") or []
        if not members:
            raise ValueError("Select cells first.")
        rule = {"members": members}
        if "weight" in payload:
            weight = float(payload["weight"])
            if not 0 <= weight < float("inf"):
                raise ValueError("Weight must be finite and nonnegative.")
            rule["weight"] = weight
        rules.append(rule)
    return apply_rules(state, rules)


def status_message(result: dict) -> str:
    info = result["info"]
    if info.get("totals_are_partial"):
        lead = (
            f"Partial results: {info.get('missing_apc_values', 0)} APC inputs missing; "
            "affected ridership is unavailable. "
        )
    else:
        lead = "Complete. "
    message = f"{lead}{info.get('issue_rows', 0)} check rows to review."
    if info.get("legacy_loaded_but_inputs_changed"):
        message += " Inputs changed, so historical adjustments were skipped."
    return message


def _method_text() -> str:
    return (ROOT / "METHOD.md").read_text(encoding="utf-8")


def export_fix(state: dict) -> tuple[bytes, str]:
    with tempfile.TemporaryDirectory(prefix="expansion-fix-") as tmp:
        output = exports.export_fix_workbook(state["result"], tmp)
        return output.read_bytes(), output.name


def export_bundle(state: dict) -> tuple[bytes, str]:
    with tempfile.TemporaryDirectory(prefix="expansion-export-") as tmp:
        stamp = exports.export_stamp()
        folder = exports.export_run(state["result"], tmp, stamp=stamp)
        rules = {
            "input_fingerprint": adjustments.fingerprint(state["tables"]),
            "manual_rules": state.get("manual") or [],
        }
        (folder / exports.stamped_filename("Reviewed session rules.json", stamp)).write_text(
            json.dumps(rules, indent=2),
            encoding="utf-8",
        )
        (folder / exports.stamped_filename("Method.txt", stamp)).write_text(
            _method_text(),
            encoding="utf-8",
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in folder.iterdir():
                if path.name.startswith(".") or path.name.endswith(".inspect.ndjson"):
                    continue
                archive.write(path, path.name)
        filename = exports.stamped_filename("ETC Institute expansion results.zip", stamp)
        return buf.getvalue(), filename
