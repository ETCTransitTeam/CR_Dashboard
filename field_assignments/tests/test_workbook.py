from __future__ import annotations

from pathlib import Path

import pytest

from field_assignments.core.export_docs import export_reports_zip, read_assignments
from field_assignments.core.workbook import build_header_template, workbook_options
from field_assignments.core.constants import EXPECTED_COLUMNS
from field_assignments.tests.conftest import CLIENT_REFS_TEMPLATE, SURVEY_TEMPLATE

TEMPLATE = CLIENT_REFS_TEMPLATE


@pytest.mark.skipif(not TEMPLATE.exists(), reason="RunCut template not present")
def test_workbook_options_normalizes_block_header():
    data = TEMPLATE.read_bytes()
    options = workbook_options(data)
    assert options["max_assignment"] >= 0
    assert isinstance(options["routes"], list)


@pytest.mark.skipif(not TEMPLATE.exists(), reason="RunCut template not present")
def test_read_assignments_and_export_zip():
    data = TEMPLATE.read_bytes()
    headers, assignments = read_assignments(data)
    assert headers[0] == "Asn#"
    if assignments:
        first_key = next(iter(assignments))
        zip_bytes, summaries = export_reports_zip(data, wanted={first_key})
        assert zip_bytes[:2] == b"PK"
        assert len(summaries) == 1


def test_header_template_download_bytes():
    data = build_header_template()
    assert data[:2] == b"PK"
    options = workbook_options(data)
    assert options["blank_rows"] == 0
    assert options["next_assignment"] == 1


@pytest.mark.skipif(not SURVEY_TEMPLATE.exists(), reason="Survey Assignments template not present")
def test_survey_template_header_validation():
    data = SURVEY_TEMPLATE.read_bytes()
    headers, assignments = read_assignments(data)
    assert headers == EXPECTED_COLUMNS
    assert isinstance(assignments, dict)
