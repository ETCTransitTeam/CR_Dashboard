from __future__ import annotations

import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_DIR = ROOT_DIR / "pipeline"
SCRIPTS_DIR = PIPELINE_DIR / "scripts"

load_dotenv(REPO_ROOT / ".env")


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or default


_workspace_override = env("RCD_WORKSPACE_DIR")
WORKSPACE_DIR = Path(_workspace_override) if _workspace_override else Path(tempfile.gettempdir()) / "rcd_workspace"

REVIEW_CYCLE_SCHEMA = (env("REVIEW_CYCLE_SCHEMA", "REVIEW_CYCLE") or "REVIEW_CYCLE").strip()
APP_CONFIG_SCHEMA = (env("APP_CONFIG_SCHEMA", "APP_CONFIG") or "APP_CONFIG").strip()
BUCKET_NAME = env("bucket_name")
LOCAL_DATA_DIR = ROOT_DIR / (env("LOCAL_DATA_DIR", "local_data") or "local_data")
KINGELVIS_LOCAL_DIR = LOCAL_DATA_DIR / "kingelvis"
PIPELINE_INPUTS_DIR = LOCAL_DATA_DIR / "pipeline_inputs"


def s3_enabled() -> bool:
    """S3 is used only when bucket and AWS credentials are configured."""
    return bool(BUCKET_NAME and env("aws_access_key_id") and env("aws_secret_access_key"))


def ensure_local_dirs() -> None:
    KINGELVIS_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    PIPELINE_INPUTS_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)


def fq_table(table: str, schema: str | None = None) -> str:
    """Fully qualified Snowflake table: DATABASE.SCHEMA.TABLE."""
    database = env("SNOWFLAKE_DATABASE")
    if not database:
        raise RuntimeError("SNOWFLAKE_DATABASE is not set in .env")
    return f"{database}.{schema or REVIEW_CYCLE_SCHEMA}.{table}"
