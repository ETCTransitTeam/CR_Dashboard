from __future__ import annotations

from pathlib import Path

import pytest

from field_assignments.core.export_docs import export_reports_zip, read_assignments
from field_assignments.core.workbook import workbook_options

TEMPLATE = Path(__file__).resolve().parents[2] / "_client_refs" / "Runcut Template.xlsx"


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
