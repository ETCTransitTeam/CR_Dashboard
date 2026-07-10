"""Header mapping for RCD pipeline scripts (ls6 -> ls2 column names).

Uses KCATA_HEADER_MAPPING from the OD Collection repo instead of
request_20250708_ls6tols2-headers.xlsx on disk or S3.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from core.config import REPO_ROOT

# Filename expected by legacy pipeline scripts in pipeline/scripts/.
MAPPING_FILENAME = "request_20250708_ls6tols2-headers.xlsx"
MAPPING_SHEET = "Example"


def header_mapping_dict() -> dict[str, str]:
    root = str(REPO_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    from automated_sync_flow_constants_maps import KCATA_HEADER_MAPPING

    return dict(KCATA_HEADER_MAPPING)


def write_header_mapping_xlsx(destination: Path) -> Path:
    """Write the standard mapping workbook that pipeline scripts read via pandas."""
    mapping = header_mapping_dict()
    df = pd.DataFrame(
        {
            "Headers-ls6": list(mapping.keys()),
            "FormattedHeader-ls2": list(mapping.values()),
        }
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(destination, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=MAPPING_SHEET, index=False)
    return destination
