"""Sequence / directional logic for Rail CR station rows.

Mirrors the regular CR rule: traveling decreasing stop sequence flips
_00 <-> _01. Terminals (last stop in a direction) can only count in the
opposite direction, so I-485 stays inbound and UNC Charlotte stays outbound.
"""
from __future__ import annotations

import pandas as pd

DIRECTION_TOKENS = frozenset({"00", "01", "02", "03"})


def extract_direction_token(value):
    """Return 00/01/02/03 from a route or station id, if present."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    parts = [p for p in str(value).strip().split("_") if p != ""]
    if len(parts) >= 2 and parts[-2] in DIRECTION_TOKENS:
        return parts[-2]
    if parts and parts[-1] in DIRECTION_TOKENS:
        return parts[-1]
    for part in parts:
        if part in DIRECTION_TOKENS:
            return part
    return None


def flip_direction_token(token):
    if token == "00":
        return "01"
    if token == "01":
        return "00"
    return token


def replace_direction_token(value, new_dir):
    """Rewrite the direction token in a route or station id."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    if not new_dir:
        return value
    parts = str(value).strip().split("_")
    if len(parts) >= 2 and parts[-2] in DIRECTION_TOKENS:
        parts[-2] = new_dir
        return "_".join(parts)
    if parts and parts[-1] in DIRECTION_TOKENS:
        parts[-1] = new_dir
        return "_".join(parts)
    return value


def _numeric_seq(df, column):
    if column not in df.columns:
        return None
    return pd.to_numeric(df[column], errors="coerce")


def _max_seq_by_route(df):
    """Highest on/off sequence seen per directional route (for last-stop flip)."""
    if "ROUTE_SURVEYEDCode" not in df.columns:
        return {}
    frames = []
    for col in ("STOP_ON_SEQ", "STOP_OFF_SEQ"):
        if col not in df.columns:
            continue
        part = df[["ROUTE_SURVEYEDCode"]].copy()
        part["_seq"] = pd.to_numeric(df[col], errors="coerce")
        frames.append(part)
    if not frames:
        return {}
    stacked = pd.concat(frames, ignore_index=True)
    stacked = stacked.dropna(subset=["_seq"])
    if stacked.empty:
        return {}
    return stacked.groupby("ROUTE_SURVEYEDCode")["_seq"].max().to_dict()


def infer_rail_flip(row, max_seq_by_route=None):
    """True when this boarding is traveling the opposite CR direction."""
    on_seq = pd.to_numeric(row.get("STOP_ON_SEQ"), errors="coerce")
    off_seq = pd.to_numeric(row.get("STOP_OFF_SEQ"), errors="coerce")
    if pd.notna(on_seq) and pd.notna(off_seq) and off_seq < on_seq:
        return True
    if pd.notna(on_seq) and pd.isna(off_seq) and max_seq_by_route:
        route = row.get("ROUTE_SURVEYEDCode")
        max_seq = max_seq_by_route.get(route)
        if pd.notna(max_seq) and float(on_seq) == float(max_seq) and float(max_seq) > 1:
            return True
    return False


def apply_rail_cr_directional_logic(survey_df):
    """Assign each rail survey to the inbound/outbound implied by stop sequence.

    - If alighting sequence < boarding sequence, flip _00 <-> _01 (regular CR).
    - If boarding is the last stop on that directional route and there is no
      alighting sequence, flip (terminal stations).
    - Keep station identity; only the direction token is rewritten so Collect
      lands on the matching Rail CR row.
    """
    if survey_df is None or not isinstance(survey_df, pd.DataFrame) or survey_df.empty:
        return survey_df

    out = survey_df.copy()
    max_seq_by_route = _max_seq_by_route(out)
    new_routes = []
    new_stations = []

    for _, row in out.iterrows():
        route = row.get("ROUTE_SURVEYEDCode")
        station = row.get("STATION_ID") if "STATION_ID" in out.columns else None
        route_dir = extract_direction_token(route)
        station_dir = extract_direction_token(station)

        if infer_rail_flip(row, max_seq_by_route) and route_dir:
            new_dir = flip_direction_token(route_dir)
            route = replace_direction_token(route, new_dir)
            station = replace_direction_token(station, new_dir)
        elif route_dir and station_dir and route_dir != station_dir:
            # Regular CR already flipped the route; keep the station on that side.
            station = replace_direction_token(station, route_dir)

        new_routes.append(route)
        new_stations.append(station)

    if "ROUTE_SURVEYEDCode" in out.columns:
        out["ROUTE_SURVEYEDCode"] = new_routes
    if "STATION_ID" in out.columns:
        out["STATION_ID"] = new_stations
        out["STATION_ID_SPLITTED"] = [
            str(val).split("_")[-1] if pd.notna(val) else val for val in new_stations
        ]
    return out
