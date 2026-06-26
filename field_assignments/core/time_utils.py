from __future__ import annotations

import re
from datetime import datetime, time, timedelta


def normalize_header(value: object) -> str:
    return str(value or "").strip()


def normalize_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def is_blank(value: object) -> bool:
    return normalize_cell(value) == ""


def normalize_assignment(value: object) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    return re.sub(r"\.0$", "", text)


def parse_time(value: object) -> time | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.time().replace(second=0, microsecond=0)
    if isinstance(value, time):
        return value.replace(second=0, microsecond=0)
    if isinstance(value, (int, float)):
        total_seconds = round(float(value) * 24 * 60 * 60)
        total_seconds %= 24 * 60 * 60
        return (datetime.min + timedelta(seconds=total_seconds)).time().replace(second=0)

    text = str(value).strip()
    for fmt in ("%I:%M %p", "%I:%M:%S %p", "%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time().replace(second=0, microsecond=0)
        except ValueError:
            pass
    return None


def time_to_minutes(value: object) -> int | None:
    parsed = parse_time(value)
    if parsed is None:
        return None
    return parsed.hour * 60 + parsed.minute


def format_time(value: object) -> str:
    parsed = parse_time(value)
    if parsed is None:
        return str(value or "")
    return parsed.strftime("%I:%M %p").lstrip("0")


def subtract_minutes(value: object, minutes: int) -> str:
    parsed = parse_time(value)
    if parsed is None:
        return ""
    shifted = datetime.combine(datetime.today(), parsed) - timedelta(minutes=minutes)
    return shifted.time().strftime("%I:%M %p").lstrip("0")


def display_value(header: str, value: object) -> str:
    if "Time" in header:
        return format_time(value)
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def time_in_range(value: object, start: str, end: str, tolerance_minutes: int = 0) -> bool:
    if not start and not end:
        return True
    value_minutes = time_to_minutes(value)
    if value_minutes is None:
        return False
    start_minutes = time_to_minutes(start) if start else None
    end_minutes = time_to_minutes(end) if end else None
    if start_minutes is None:
        return value_minutes <= (end_minutes + tolerance_minutes) % (24 * 60)
    if end_minutes is None:
        return value_minutes >= (start_minutes - tolerance_minutes) % (24 * 60)
    start_minutes = (start_minutes - tolerance_minutes) % (24 * 60)
    end_minutes = (end_minutes + tolerance_minutes) % (24 * 60)
    if start_minutes <= end_minutes:
        return start_minutes <= value_minutes <= end_minutes
    return value_minutes >= start_minutes or value_minutes <= end_minutes


def option_values(values: list[object]) -> list[str]:
    clean = {normalize_cell(value) for value in values if normalize_cell(value)}
    return sorted(clean, key=lambda value: (not value.isdigit(), value.lower()))


def parse_assignment_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    parts = {item.strip() for item in value.split(",") if item.strip()}
    return parts or None
