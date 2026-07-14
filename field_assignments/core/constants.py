from __future__ import annotations

EXPECTED_COLUMNS = [
    "Asn#",
    "Block",
    "Route",
    "Direction",
    "Start Time",
    "Start Location",
    "End Location",
    "End Time",
]

COL_ASN = 1
COL_BLOCK = 2
COL_ROUTE = 3
COL_DIRECTION = 4
COL_START_TIME = 5
COL_START_LOCATION = 6
COL_END_LOCATION = 7
COL_END_TIME = 8

ANY_ROUTE = "(Any route)"
ANY_LOCATION = "(Any location)"
ROUTE_ONLY_MAX_GAP_MINUTES = 60

COLUMN_WIDTHS = [0.55, 0.65, 0.65, 0.85, 0.85, 2.55, 2.55, 0.85]

S3_PREFIX = "field-assignments"
