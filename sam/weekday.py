from __future__ import annotations

import re
from datetime import datetime
from io import BytesIO

import pandas as pd

from sam.time_utils import clean_token, time_to_minutes

RAW_REQUIRED = {
    "block",
    "route",
    "start",
    "end",
    "trip start place description",
    "trip end place description",
    "type",
}
WEEKDAY_REQUIRED = {
    "asn#",
    "block",
    "route",
    "direction",
    "start time",
    "start location",
    "end location",
    "end time",
}
KEEP_COLUMNS = [
    "Asn#",
    "Block",
    "Route",
    "Direction",
    "Start Time",
    "Start Location",
    "End Location",
    "End Time",
    "Type",
    "Garage",
    "Veh1",
    "Duty",
]


WEEKDAY_WITHOUT_ASN = WEEKDAY_REQUIRED - {"asn#"}


def _norm(name: object) -> str:
    return " ".join(str(name).replace("\n", " ").strip().lower().split())


def _names(columns) -> set[str]:
    return {_norm(column) for column in columns}


def classify_columns(columns) -> str | None:
    names = _names(columns)
    if RAW_REQUIRED <= names:
        return "raw"
    if WEEKDAY_WITHOUT_ASN <= names:
        return "weekday"
    return None


def _lookup(columns) -> dict[str, str]:
    return {_norm(column): column for column in columns}


def _series(frame: pd.DataFrame, *aliases: str) -> pd.Series:
    lookup = _lookup(frame.columns)
    for alias in aliases:
        source = lookup.get(alias)
        if source is not None:
            return frame[source].map(clean_token)
    return pd.Series([""] * len(frame), index=frame.index)


def _clock_value(value: object) -> object:
    minutes = time_to_minutes(value)
    if minutes is None:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return pd.NaT
        minutes = int(parsed.hour) * 60 + int(parsed.minute)
    hour, minute = divmod(minutes, 60)
    return datetime(2000, 1, 1, hour, minute)


def _clock(frame: pd.DataFrame, *aliases: str) -> pd.Series:
    lookup = _lookup(frame.columns)
    for alias in aliases:
        source = lookup.get(alias)
        if source is not None:
            return pd.to_datetime(frame[source].map(_clock_value), errors="coerce")
    return pd.Series(pd.NaT, index=frame.index)


def _with_ids(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.loc[:, KEEP_COLUMNS].copy()
    frame.insert(0, "_id", range(len(frame)))
    frame["Asn#"] = frame["Asn#"].map(clean_token)
    return frame.reset_index(drop=True)


def frame_from_raw(frame: pd.DataFrame) -> pd.DataFrame:
    built = pd.DataFrame(
        {
            "Asn#": "",
            "Block": _series(frame, "block"),
            "Route": _series(frame, "route"),
            "Direction": _series(frame, "direction"),
            "Start Time": _clock(frame, "start"),
            "Start Location": _series(frame, "trip start place description"),
            "End Location": _series(frame, "trip end place description"),
            "End Time": _clock(frame, "end"),
            "Type": _series(frame, "type"),
            "Garage": _series(frame, "garage"),
            "Veh1": _series(frame, "veh1"),
            "Duty": _series(frame, "duty"),
        }
    )
    built = built.loc[built["Block"].ne("")].copy()
    return _with_ids(built)


def frame_from_weekday(frame: pd.DataFrame) -> pd.DataFrame:
    built = pd.DataFrame(
        {
            "Asn#": _series(frame, "asn#"),
            "Block": _series(frame, "block"),
            "Route": _series(frame, "route"),
            "Direction": _series(frame, "direction"),
            "Start Time": _clock(frame, "start time"),
            "Start Location": _series(frame, "start location"),
            "End Location": _series(frame, "end location"),
            "End Time": _clock(frame, "end time"),
            "Type": _series(frame, "type"),
            "Garage": _series(frame, "garage"),
            "Veh1": _series(frame, "veh1"),
            "Duty": _series(frame, "duty"),
        }
    )
    built = built.loc[built["Block"].ne("")].copy()
    return _with_ids(built)


def inspect_workbook(data: bytes, on_sheet=None) -> list[dict[str, object]]:
    book = pd.ExcelFile(BytesIO(data))
    options: list[dict[str, object]] = []
    total = len(book.sheet_names)
    for index, name in enumerate(book.sheet_names):
        if on_sheet is not None:
            on_sheet(index, total, name)
        header = pd.read_excel(book, sheet_name=name, nrows=0)
        kind = classify_columns(header.columns)
        if kind is None:
            continue
        counted = pd.read_excel(book, sheet_name=name, usecols=[0])
        assigned = 0
        if kind == "weekday":
            lookup = _lookup(header.columns)
            asn_col = lookup.get("asn#")
            if asn_col is not None:
                asn = pd.read_excel(book, sheet_name=name, usecols=[asn_col])
                assigned = int(asn.iloc[:, 0].map(clean_token).ne("").sum())
        options.append(
            {
                "name": name,
                "kind": kind,
                "rows": int(len(counted)),
                "assigned": assigned,
            }
        )
    return options


def load_sheet(data: bytes, sheet_name: str, kind: str) -> pd.DataFrame:
    frame = pd.read_excel(BytesIO(data), sheet_name=sheet_name, dtype=object)
    frame.columns = [str(column).strip() for column in frame.columns]
    if kind == "raw":
        return frame_from_raw(frame)
    if kind == "weekday":
        return frame_from_weekday(frame)
    raise ValueError(f"Sheet {sheet_name} is not a Raw Weekday or Weekday sheet.")


def sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Asn#": "",
                "BLOCK": 1,
                "ROUTE": 90,
                "DIRECTION": "TO NASHVILLE",
                "Start Time": "5:40 AM",
                "Start Location": "LEBANON STATION",
                "End Time": "6:35 AM",
                "End Location": "RIVERFRONT STATION",
            },
            {
                "Asn#": "",
                "BLOCK": 1,
                "ROUTE": 90,
                "DIRECTION": "FROM NASHVILLE",
                "Start Time": "6:53 AM",
                "Start Location": "RIVERFRONT STATION",
                "End Time": "7:30 AM",
                "End Location": "MT. JULIET STATION",
            },
        ]
    )


def sample_workbook_bytes() -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sample_frame().to_excel(writer, index=False, sheet_name="Weekday")
    return buffer.getvalue()


def auto_choice(options: list[dict[str, object]]) -> dict[str, object] | None:
    """Prefer a sheet that is clearly the raw weekday extract."""
    named_raw = [
        item
        for item in options
        if item["kind"] == "raw" and _is_raw_weekday_name(str(item["name"]))
    ]
    if len(named_raw) == 1:
        return named_raw[0]
    if len(options) == 1:
        return options[0]
    raw_sheets = [item for item in options if item["kind"] == "raw"]
    weekday_sheets = [item for item in options if item["kind"] == "weekday"]
    assigned = sum(int(item["assigned"]) for item in weekday_sheets)
    if len(raw_sheets) == 1 and assigned == 0:
        return raw_sheets[0]
    return None


def _is_raw_weekday_name(name: str) -> bool:
    text = name.casefold()
    return "raw" in text and "weekday" in text


def has_type_column(frame: pd.DataFrame) -> bool:
    return bool(frame["Type"].map(clean_token).ne("").any())


def next_assignment_number(frame: pd.DataFrame) -> int:
    highest = 0
    for value in frame["Asn#"].tolist():
        text = clean_token(value)
        if text.isdigit():
            highest = max(highest, int(text))
    return highest + 1


def default_hub(frame: pd.DataFrame, preferred: str) -> str:
    if preferred:
        for value in frame["Start Location"].map(clean_token):
            if value.casefold() == preferred.casefold():
                return value
    regular = frame.loc[frame["Type"].map(lambda value: clean_token(value).casefold()).eq("regular"), "Start Location"]
    pool = regular if not regular.empty else frame["Start Location"]
    counts = pool.map(clean_token)
    counts = counts[counts.ne("")]
    if counts.empty:
        return ""
    return str(counts.value_counts().index[0])


_BAY_SUFFIX = re.compile(r"(?i)(?:\s*[-–—]\s*|\s+)bay\s*#?\s*(\d+)\s*$")


def split_bay_hubs(names: list[object] | pd.Series) -> list[dict[str, object]]:
    """Stations whose trips are split into more than one bay name.

    Names are not changed. The caller only uses this to warn that Bay 18 and
    Bay 24 will be treated as different places.
    """
    stations: dict[str, str] = {}
    bays_by_station: dict[str, dict[int, str]] = {}
    for raw in names:
        text = clean_token(raw)
        match = _BAY_SUFFIX.search(text)
        if not match or match.start() == 0:
            continue
        station = text[: match.start()].strip(" -–—")
        if len(station) < 2:
            continue
        key = station.casefold()
        stations.setdefault(key, station)
        bays_by_station.setdefault(key, {})[int(match.group(1))] = text
    hubs: list[dict[str, object]] = []
    for key, bays in bays_by_station.items():
        if len(bays) < 2:
            continue
        numbers = sorted(bays)
        hubs.append(
            {
                "station": stations[key],
                "bays": numbers,
                "names": [bays[number] for number in numbers],
            }
        )
    hubs.sort(key=lambda item: (-len(item["bays"]), str(item["station"]).casefold()))
    return hubs


def block_sort_key(value: str) -> tuple:
    text = clean_token(value)
    if text.isdigit():
        return (0, int(text))
    return (1, text.casefold())


def open_regular_count(frame: pd.DataFrame) -> int:
    blank = frame["Asn#"].map(clean_token).eq("")
    kind = frame["Type"].map(lambda value: clean_token(value).casefold())
    regular_or_blank = kind.eq("regular") | kind.eq("")
    timed = frame["Start Time"].map(time_to_minutes).notna()
    return int((blank & regular_or_blank & timed).sum())
