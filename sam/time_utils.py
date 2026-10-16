from __future__ import annotations

from datetime import datetime, time, timedelta

import pandas as pd


def clean_token(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def time_to_minutes(value: object) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return int(value.hour) * 60 + int(value.minute)
    if isinstance(value, datetime):
        return value.hour * 60 + value.minute
    if isinstance(value, time):
        return value.hour * 60 + value.minute
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        total_seconds = round(float(value) * 24 * 60 * 60) % (24 * 60 * 60)
        return total_seconds // 60
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%I:%M %p", "%I:%M:%S %p", "%H:%M", "%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.hour * 60 + parsed.minute
        except ValueError:
            continue
    return None


def format_time(value: object) -> str:
    minutes = time_to_minutes(value)
    if minutes is None:
        return ""
    hour24, minute = divmod(minutes, 60)
    hour12 = hour24 % 12 or 12
    suffix = "AM" if hour24 < 12 else "PM"
    return f"{hour12}:{minute:02d} {suffix}"


def format_hours(hours: float | None) -> str:
    if hours is None:
        return ""
    total = int(round(hours * 60))
    whole, minute = divmod(total, 60)
    if minute == 0:
        return f"{whole} hr"
    return f"{whole} hr {minute} min"


def duration_hours(start: object, end: object) -> float | None:
    start_m = time_to_minutes(start)
    end_m = time_to_minutes(end)
    if start_m is None or end_m is None:
        return None
    span = end_m - start_m
    if span < 0:
        span += 24 * 60
    return span / 60.0


def subtract_minutes(value: object, minutes: int) -> str:
    current = time_to_minutes(value)
    if current is None:
        return ""
    shifted = (current - minutes) % (24 * 60)
    hour24, minute = divmod(shifted, 60)
    dummy = datetime(2000, 1, 1, hour24, minute)
    return format_time(dummy)


def clock_input_to_minutes(value: time | None) -> int | None:
    if value is None:
        return None
    return value.hour * 60 + value.minute
