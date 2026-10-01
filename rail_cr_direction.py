"""Sequence / directional logic for Rail CR station rows.

Mirrors the regular CR rule: traveling decreasing stop sequence flips
_00 <-> _01. Terminus ends (last station in a CR direction, e.g. I-485 on
outbound) can only count on the opposite direction (I-485 inbound).
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


def _normalize_station_key(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _station_suffix(station_id):
    if station_id is None or (isinstance(station_id, float) and pd.isna(station_id)):
        return ""
    return str(station_id).strip().split("_")[-1]


def _stop_sequence_by_id(stops_df):
    """Highest travel sequence for each stop id. Empty when stops are unavailable."""
    if stops_df is None or not isinstance(stops_df, pd.DataFrame) or stops_df.empty:
        return {}
    if "ETC_STOP_ID" not in stops_df.columns or "seq_fixed" not in stops_df.columns:
        return {}
    sequences = pd.to_numeric(stops_df["seq_fixed"], errors="coerce")
    by_id = {}
    for stop_id, stop_seq in zip(stops_df["ETC_STOP_ID"], sequences):
        if pd.isna(stop_id) or pd.isna(stop_seq):
            continue
        stop_id = str(stop_id).strip()
        if not stop_id:
            continue
        stop_seq = float(stop_seq)
        previous = by_id.get(stop_id)
        if previous is None or stop_seq > previous:
            by_id[stop_id] = stop_seq
    return by_id


def rail_terminus_ends_from_cr(*cr_dfs, stops_df=None):
    """Travel end of each CR direction.

    The end is the stop with the highest sequence on that direction. On CATS,
    inbound ends at UNC Charlotte and outbound ends at I-485. The CR sheet lists
    both directions in the same geographic order, so the last SORT row is not
    the outbound end. Without stop sequences, the last SORT row is used.
    """
    ends = {}
    stop_sequences = _stop_sequence_by_id(stops_df)
    for cr in cr_dfs:
        if cr is None or not isinstance(cr, pd.DataFrame) or cr.empty:
            continue
        station_col = next(
            (c for c in ("STATION_ID", "LS_NAME_CODE") if c in cr.columns),
            None,
        )
        if station_col is None:
            continue
        work = cr[[station_col]].copy()
        if "LS_NAME_CODE" in cr.columns:
            work["LS_NAME_CODE"] = cr["LS_NAME_CODE"]
        if "STATION_NAME" in cr.columns:
            work["STATION_NAME"] = cr["STATION_NAME"]
        if "SORT" in cr.columns:
            work["SORT"] = cr["SORT"]
        work["_dir"] = work[station_col].map(extract_direction_token)
        if "LS_NAME_CODE" in work.columns:
            work["_dir"] = work["_dir"].where(
                work["_dir"].notna(), work["LS_NAME_CODE"].map(extract_direction_token)
            )
        work = work[work["_dir"].notna()].copy()
        if work.empty:
            continue
        if "SORT" in work.columns:
            work["_sort"] = pd.to_numeric(work["SORT"], errors="coerce")
            work = work.sort_values(["_dir", "_sort"], kind="mergesort", na_position="last")
        for direction, group in work.groupby("_dir", sort=False):
            chosen = group.iloc[-1]
            if stop_sequences:
                best_seq = None
                for _, station_row in group.iterrows():
                    stop_seq = stop_sequences.get(str(station_row[station_col]).strip())
                    if stop_seq is None:
                        continue
                    if best_seq is None or stop_seq > best_seq:
                        best_seq = stop_seq
                        chosen = station_row
            keys = ends.setdefault(str(direction), set())
            keys.add(_station_suffix(chosen[station_col]))
            if "STATION_NAME" in chosen.index:
                name_key = _normalize_station_key(chosen.get("STATION_NAME"))
                if name_key:
                    keys.add(name_key)
    ends = {d: {k for k in keys if k} for d, keys in ends.items()}
    return ends


def infer_rail_flip(row, max_seq_by_route=None, terminus_ends=None):
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
    if terminus_ends:
        route_dir = extract_direction_token(row.get("ROUTE_SURVEYEDCode"))
        ends = terminus_ends.get(route_dir) or set()
        if ends:
            suffix = _station_suffix(row.get("STATION_ID"))
            name_key = _normalize_station_key(row.get("STATION_NAME"))
            if suffix in ends or (name_key and name_key in ends):
                return True
    return False


def apply_rail_cr_directional_logic(survey_df, *cr_dfs, stops_df=None):
    """Assign each rail survey to the inbound/outbound implied by stop sequence.

    - If alighting sequence < boarding sequence, flip _00 <-> _01 (regular CR).
    - If boarding is the last stop on that directional route and there is no
      alighting sequence, flip (terminal stations).
    - If boarding is the travel end of that direction (I-485 outbound,
      UNC Charlotte inbound), flip even when sequence looks valid.
      Outbound boardings at UNC Charlotte stay outbound.
    - Keep station identity; only the direction token is rewritten so Collect
      lands on the matching Rail CR row.
    """
    if survey_df is None or not isinstance(survey_df, pd.DataFrame) or survey_df.empty:
        return survey_df

    out = survey_df.copy()
    max_seq_by_route = _max_seq_by_route(out)
    terminus_ends = rail_terminus_ends_from_cr(*cr_dfs, stops_df=stops_df)
    new_routes = []
    new_stations = []

    for _, row in out.iterrows():
        route = row.get("ROUTE_SURVEYEDCode")
        station = row.get("STATION_ID") if "STATION_ID" in out.columns else None
        route_dir = extract_direction_token(route)
        station_dir = extract_direction_token(station)

        if infer_rail_flip(row, max_seq_by_route, terminus_ends) and route_dir:
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
