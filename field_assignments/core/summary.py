from __future__ import annotations

import pandas as pd
from openpyxl import load_workbook
from io import BytesIO

from field_assignments.core.constants import COL_ASN, COL_ROUTE
from field_assignments.core.time_utils import is_blank, normalize_cell
from field_assignments.core.workbook import read_headers, validate_headers


def _coverage_color(pct: float) -> str:
    if pct < 20:
        return "#ffcdd2"
    if pct < 40:
        return "#fff9c4"
    if pct < 70:
        return "#c8e6c9"
    return "#66bb6a"


def route_coverage_summary(source: bytes, sheet_name: str | None = None) -> pd.DataFrame:
    workbook = load_workbook(BytesIO(source), data_only=True)
    sheet = workbook[sheet_name] if sheet_name else workbook.active
    headers = read_headers(sheet)
    validate_headers(headers)

    totals: dict[str, int] = {}
    assigned: dict[str, int] = {}

    for row_number in range(2, sheet.max_row + 1):
        route = normalize_cell(sheet.cell(row=row_number, column=COL_ROUTE).value)
        if not route:
            continue
        totals[route] = totals.get(route, 0) + 1
        if not is_blank(sheet.cell(row=row_number, column=COL_ASN).value):
            assigned[route] = assigned.get(route, 0) + 1

    rows = []
    for route in sorted(totals.keys(), key=lambda value: (not value.isdigit(), value.lower())):
        total = totals[route]
        done = assigned.get(route, 0)
        pct = round((done / total) * 100, 1) if total else 0.0
        rows.append(
            {
                "Route": route,
                "Total Runs": total,
                "Assigned Runs": done,
                "% Covered": pct,
            }
        )

    return pd.DataFrame(rows)


def style_coverage_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def _row_style(row):
        pct = row["% Covered"]
        color = _coverage_color(float(pct))
        return [f"background-color: {color}"] * len(row)

    return df.style.apply(_row_style, axis=1).format({"% Covered": "{:.1f}%"})
