from __future__ import annotations

import pytest

from field_assignments.core.summary import start_location_routes_summary
from field_assignments.tests.conftest import CLIENT_REFS_TEMPLATE, start_location_workbook


def test_start_location_routes_summary_percentages(start_location_workbook):
    df = start_location_routes_summary(start_location_workbook)
    assert list(df.columns) == ["Start Location", "# of Routes", "% of Routes", "Routes"]

    university = df[df["Start Location"] == "University Area Transit Center"].iloc[0]
    marion = df[df["Start Location"] == "Marion Transit Center"].iloc[0]
    netpark = df[df["Start Location"] == "Netpark Transfer Center"].iloc[0]

    assert university["# of Routes"] == 2
    assert marion["# of Routes"] == 2
    assert netpark["# of Routes"] == 1
    assert university["% of Routes"] == pytest.approx(40.0)
    assert marion["% of Routes"] == pytest.approx(40.0)
    assert netpark["% of Routes"] == pytest.approx(20.0)
    assert university["Routes"] == "1, 5"


@pytest.mark.skipif(not CLIENT_REFS_TEMPLATE.exists(), reason="RunCut template not present")
def test_start_location_routes_summary_on_client_template():
    data = CLIENT_REFS_TEMPLATE.read_bytes()
    df = start_location_routes_summary(data)
    assert not df.empty
    assert (df["# of Routes"] > 0).all()
    assert (df["Routes"].astype(str).str.len() > 0).all()
