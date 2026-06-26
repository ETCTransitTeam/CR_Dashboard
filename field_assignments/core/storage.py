from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from io import BytesIO

import boto3

from field_assignments.core.constants import S3_PREFIX
from field_assignments.core.workbook import workbook_options


def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )


def _bucket_name() -> str | None:
    return os.getenv("bucket_name")


def _label_slug(label: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in (label or "general").strip())
    return cleaned or "general"


def save_workbook_version(
    workbook_bytes: bytes,
    *,
    label: str,
    original_filename: str,
    uploaded_by: str,
    sheet_name: str | None = None,
) -> dict[str, str] | None:
    bucket = _bucket_name()
    if not bucket:
        return None

    options = workbook_options(workbook_bytes, sheet_name)
    max_asn = str(options.get("max_assignment", 0))
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = _label_slug(label)
    base_key = f"{S3_PREFIX}/{slug}/{timestamp}_max{max_asn}"
    xlsx_key = f"{base_key}.xlsx"
    meta_key = f"{base_key}.json"

    meta = {
        "uploaded_at": timestamp,
        "max_asn": max_asn,
        "original_filename": original_filename,
        "uploaded_by": uploaded_by,
        "label": label,
        "sheet": options.get("sheet"),
        "xlsx_key": xlsx_key,
    }

    client = _s3_client()
    client.put_object(Bucket=bucket, Key=xlsx_key, Body=workbook_bytes)
    client.put_object(Bucket=bucket, Key=meta_key, Body=json.dumps(meta).encode("utf-8"))
    return meta


def list_workbook_versions(label: str, limit: int = 20) -> list[dict[str, str]]:
    bucket = _bucket_name()
    if not bucket:
        return []

    prefix = f"{S3_PREFIX}/{_label_slug(label)}/"
    client = _s3_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents") or []
    json_keys = sorted(
        [item["Key"] for item in contents if str(item["Key"]).endswith(".json")],
        reverse=True,
    )[:limit]

    versions: list[dict[str, str]] = []
    for key in json_keys:
        try:
            body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            versions.append(json.loads(body.decode("utf-8")))
        except Exception:
            continue
    return versions


def load_workbook_version(xlsx_key: str) -> bytes | None:
    bucket = _bucket_name()
    if not bucket or not xlsx_key:
        return None
    try:
        return _s3_client().get_object(Bucket=bucket, Key=xlsx_key)["Body"].read()
    except Exception:
        return None
