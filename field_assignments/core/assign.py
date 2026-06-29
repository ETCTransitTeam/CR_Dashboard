from __future__ import annotations

from io import BytesIO
from pathlib import Path

from openpyxl import load_workbook

from field_assignments.core.constants import (
    COL_ASN,
    COL_BLOCK,
    COL_END_LOCATION,
    COL_END_TIME,
    COL_ROUTE,
    COL_START_LOCATION,
    COL_START_TIME,
)
from field_assignments.core.time_utils import is_blank, normalize_cell, time_in_range
from field_assignments.core.workbook import workbook_options


def row_matches_base_rules(
    sheet,
    row_number: int,
    rules: dict[str, str],
    block: str | None = None,
    *,
    require_route: bool = True,
) -> bool:
    if not is_blank(sheet.cell(row=row_number, column=COL_ASN).value):
        return False
    required_block = block or rules["block"]
    if required_block and normalize_cell(sheet.cell(row=row_number, column=COL_BLOCK).value) != required_block:
        return False
    if require_route and normalize_cell(sheet.cell(row=row_number, column=COL_ROUTE).value) != rules["route"]:
        return False
    return True


def row_matches_segment_row(
    sheet,
    row_number: int,
    rules: dict[str, str],
    block: str,
) -> bool:
    """Match blank Asn# rows on the same block within an assignment segment."""
    include_interlined = str(rules.get("include_interlined", "false")).lower() in {"1", "true", "yes", "on"}
    return row_matches_base_rules(
        sheet,
        row_number,
        rules,
        block=block,
        require_route=not include_interlined,
    )


def row_matches_start_rules(sheet, row_number: int, rules: dict[str, str]) -> bool:
    if not row_matches_base_rules(sheet, row_number, rules):
        return False
    if normalize_cell(sheet.cell(row=row_number, column=COL_START_LOCATION).value) != rules["start_location"]:
        return False
    tolerance = int(rules.get("tolerance", "0") or 0)
    if not time_in_range(
        sheet.cell(row=row_number, column=COL_START_TIME).value,
        rules["start_from"],
        rules["start_to"],
        tolerance,
    ):
        return False
    return True


def row_matches_end_rules(sheet, row_number: int, rules: dict[str, str], block: str) -> bool:
    if not row_matches_base_rules(sheet, row_number, rules, block=block):
        return False
    if normalize_cell(sheet.cell(row=row_number, column=COL_END_LOCATION).value) != rules["end_location"]:
        return False
    tolerance = int(rules.get("tolerance", "0") or 0)
    if not time_in_range(
        sheet.cell(row=row_number, column=COL_END_TIME).value,
        rules["shift_from"],
        rules["shift_to"],
        tolerance,
    ):
        return False
    return True


def find_assignment_segment(sheet, rules: dict[str, str]) -> tuple[int, int, str]:
    start_rows = (
        range(sheet.max_row, 1, -1)
        if rules["scan_order"] == "bottom_to_top"
        else range(2, sheet.max_row + 1)
    )
    for start_row in start_rows:
        if not row_matches_start_rules(sheet, start_row, rules):
            continue
        block = normalize_cell(sheet.cell(row=start_row, column=COL_BLOCK).value)
        for end_row in range(start_row, sheet.max_row + 1):
            if row_matches_end_rules(sheet, end_row, rules, block):
                return start_row, end_row, block
    raise ValueError(
        "No blank assignment segment matched those rules. "
        "Start Location and Start Time find the first row; End Location and Shift range find the final row."
    )


def fill_assignment_numbers(
    source: bytes,
    sheet_name: str | None,
    rules_list: list[dict[str, str]],
) -> tuple[bytes, list[dict[str, object]]]:
    workbook = load_workbook(BytesIO(source))
    sheet = workbook[sheet_name] if sheet_name else workbook.active
    options = workbook_options(source, sheet.title)
    next_assignment = int(options["next_assignment"])
    results: list[dict[str, object]] = []

    for index, rules in enumerate(rules_list, start=1):
        try:
            start_row, end_row, segment_block = find_assignment_segment(sheet, rules)
        except ValueError as exc:
            raise ValueError(f"Assignment {index}: {exc}") from exc
        matching_rows: list[int] = []
        for row_number in range(start_row, end_row + 1):
            if row_matches_segment_row(sheet, row_number, rules, segment_block):
                sheet.cell(row=row_number, column=COL_ASN).value = next_assignment
                matching_rows.append(row_number)

        if not matching_rows:
            raise ValueError(
                f"Assignment {index}: a segment was found, but no blank Asn# rows were available to fill."
            )

        matching_rows.sort()
        results.append(
            {
                "guideline": index,
                "assignment": next_assignment,
                "rows": matching_rows,
                "count": len(matching_rows),
            }
        )
        next_assignment += 1

    out = BytesIO()
    workbook.save(out)
    return out.getvalue(), results
