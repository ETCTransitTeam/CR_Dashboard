from __future__ import annotations

import zipfile
from io import BytesIO

import pytest
from openpyxl import load_workbook

from field_assignments.core.assign import fill_assignment_numbers
from field_assignments.core.export_docs import export_reports_zip
from field_assignments.core.workbook import build_header_template, read_headers
from field_assignments.core.constants import EXPECTED_COLUMNS
from field_assignments.tests.conftest import CLIENT_REFS_TEMPLATE, SURVEY_TEMPLATE, _read_asn_values


def test_build_header_template_matches_expected_columns():
    data = build_header_template()
    workbook = load_workbook(BytesIO(data), data_only=True)
    sheet = workbook.active
    assert read_headers(sheet) == EXPECTED_COLUMNS


def test_fill_assignment_without_interline_only_fills_selected_route(blank_interline_workbook):
    rules = [
        {
            "block": "10193",
            "route": "19",
            "start_location": "Marion Transit Center",
            "end_location": "Marion Transit Center",
            "start_from": "05:00",
            "start_to": "06:00",
            "shift_from": "07:00",
            "shift_to": "08:00",
            "scan_order": "top_to_bottom",
            "tolerance": "30",
            "include_interlined": "false",
        }
    ]
    updated_bytes, results = fill_assignment_numbers(blank_interline_workbook, None, rules)
    asn_values = _read_asn_values(updated_bytes)
    assert len(results) == 1
    assert results[0]["assignment"] == 1
    assert asn_values == ["1", "", "1"]
    assert results[0]["count"] == 2


def test_fill_assignment_with_interline_fills_same_block_rows(blank_interline_workbook):
    rules = [
        {
            "block": "10193",
            "route": "19",
            "start_location": "Marion Transit Center",
            "end_location": "Marion Transit Center",
            "start_from": "05:00",
            "start_to": "06:00",
            "shift_from": "07:00",
            "shift_to": "08:00",
            "scan_order": "top_to_bottom",
            "tolerance": "30",
            "include_interlined": "true",
        }
    ]
    updated_bytes, results = fill_assignment_numbers(blank_interline_workbook, None, rules)
    asn_values = _read_asn_values(updated_bytes)
    assert asn_values == ["1", "1", "1"]
    assert results[0]["count"] == 3


def test_export_assignments_11_and_12(interline_workbook):
    zip_bytes, summaries = export_reports_zip(interline_workbook)
    assert zip_bytes[:2] == b"PK"
    assignments = {item["Assignment"] for item in summaries}
    assert assignments == {"11", "12"}

    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        names = archive.namelist()
        assert any("Assignment-11" in name for name in names)
        assert any("Assignment-12" in name for name in names)

    zip_12, summaries_12 = export_reports_zip(interline_workbook, wanted={"12"})
    assert len(summaries_12) == 1
    assert summaries_12[0]["Assignment"] == "12"
    assert zip_12[:2] == b"PK"


@pytest.mark.skipif(not CLIENT_REFS_TEMPLATE.exists(), reason="RunCut template not present")
def test_client_refs_template_exports_when_assignments_exist():
    data = CLIENT_REFS_TEMPLATE.read_bytes()
    zip_bytes, summaries = export_reports_zip(data)
    assert zip_bytes[:2] == b"PK"
    assert len(summaries) >= 1


@pytest.mark.skipif(not SURVEY_TEMPLATE.exists(), reason="Survey Assignments template not present")
def test_survey_assignments_template_workbook_options():
    from field_assignments.core.workbook import workbook_options

    data = SURVEY_TEMPLATE.read_bytes()
    options = workbook_options(data)
    assert options["max_assignment"] >= 0
    assert isinstance(options["routes"], list)
