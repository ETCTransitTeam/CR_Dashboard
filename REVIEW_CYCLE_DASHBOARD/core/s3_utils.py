from __future__ import annotations

import io
from pathlib import Path

import pandas as pd

from core.config import BUCKET_NAME, KINGELVIS_LOCAL_DIR, PIPELINE_INPUTS_DIR, ensure_local_dirs, s3_enabled


def _s3_client():
    import boto3
    import os

    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )


def read_excel_from_s3(bucket: str, key: str, sheet_name: str) -> pd.DataFrame:
    response = _s3_client().get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()
    return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)


def read_excel_storage(key: str, sheet_name: str, bucket: str | None = None) -> pd.DataFrame:
    if s3_enabled():
        return read_excel_from_s3(bucket or BUCKET_NAME, key, sheet_name)
    local_path = _local_kingelvis_path(key)
    if not local_path.exists():
        raise FileNotFoundError(
            f"KingElvis file not found locally: {local_path}. "
            "Export from Sync & Admin or place the workbook in local_data/kingelvis/."
        )
    return pd.read_excel(local_path, sheet_name=sheet_name)


def upload_excel_to_s3(bucket: str, key: str, excel_bytes: bytes) -> None:
    _s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=excel_bytes,
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def download_file_from_s3(bucket: str, key: str, destination: Path) -> None:
    response = _s3_client().get_object(Bucket=bucket, Key=key)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(response["Body"].read())


def _list_s3_keys(bucket: str) -> list[str]:
    """List object keys in a bucket (flat list)."""
    client = _s3_client()
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            if key and not key.endswith("/"):
                keys.append(key)
    return keys


def find_s3_key(bucket: str, filename: str) -> str | None:
    """Resolve an S3 object key for a pipeline input filename.

    Tries exact key, basename match (any folder), then prefix rules for details_/request_ files.
    """
    name = Path(filename).name
    name_lower = name.lower()
    stem_lower = Path(name).stem.lower()

    try:
        _s3_client().head_object(Bucket=bucket, Key=name)
        return name
    except Exception:
        pass

    try:
        keys = _list_s3_keys(bucket)
    except Exception:
        return None

    for key in keys:
        if Path(key).name.lower() == name_lower:
            return key

    if stem_lower.startswith("details_"):
        prefix = stem_lower.rsplit("_", 1)[0] if "_" in stem_lower else stem_lower
        candidates = sorted(
            k for k in keys if Path(k).name.lower().startswith(prefix) and k.lower().endswith(".xlsx")
        )
        if len(candidates) == 1:
            return candidates[0]
        for key in candidates:
            if Path(key).stem.lower().startswith(prefix):
                return key

    if "ls6tols2" in name_lower or stem_lower.startswith("request_"):
        for key in keys:
            kl = key.lower()
            if "ls6tols2" in kl and kl.endswith(".xlsx"):
                return key
        for key in keys:
            kl = Path(key).name.lower()
            if kl.startswith("request_") and "header" in kl:
                return key

    return None


def download_pipeline_input_from_s3(bucket: str, filename: str, destination: Path) -> str:
    """Download a pipeline input by filename, resolving the S3 key when needed."""
    key = find_s3_key(bucket, filename)
    if not key:
        raise FileNotFoundError(
            f"No S3 object found for {filename!r} in bucket {bucket!r}. "
            "Upload the file to the bucket root (same name as DETAILS/KingElvis files) "
            "or place it in pipeline/shared_inputs/."
        )
    download_file_from_s3(bucket, key, destination)
    return key


def local_pipeline_input_path(filename: str) -> Path:
    ensure_local_dirs()
    return PIPELINE_INPUTS_DIR / Path(filename).name


def _local_kingelvis_path(key: str) -> Path:
    ensure_local_dirs()
    filename = Path(key).name if key else "KINGElvis.xlsx"
    return KINGELVIS_LOCAL_DIR / filename


def save_excel_local(key: str, excel_bytes: bytes) -> Path:
    path = _local_kingelvis_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(excel_bytes)
    return path


def dataframe_to_excel_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    buffer.seek(0)
    return buffer.read()


def export_kingelvis_workbook(
    elvis_review_df: pd.DataFrame,
    combined_checks_df: pd.DataFrame,
    bucket: str,
    key: str,
) -> str:
    payload = dataframe_to_excel_bytes(
        {
            "Elvis_Review": elvis_review_df,
            "COMBINED_CHECKS": combined_checks_df,
        }
    )
    if s3_enabled():
        upload_excel_to_s3(bucket, key, payload)
        return f"s3://{bucket}/{key}"
    local_path = save_excel_local(key, payload)
    return str(local_path)


def storage_mode_label() -> str:
    return "S3" if s3_enabled() else "local files (local_data/kingelvis/)"
