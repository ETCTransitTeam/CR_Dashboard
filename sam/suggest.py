from __future__ import annotations

import pandas as pd

from sam.time_utils import clean_token, duration_hours, format_hours, format_time, time_to_minutes

ANY_PLACE = "(Any place)"
ALL_ROUTES = "All routes"
GARAGE_TYPES = {"pull-out", "pull-in", "deadhead"}


def same_place(left: object, right: object) -> bool:
    return clean_token(left).casefold() == clean_token(right).casefold()


def trip_type(value: object) -> str:
    return clean_token(value).casefold()


def is_garage(value: object) -> bool:
    return trip_type(value) in GARAGE_TYPES


def _row(frame: pd.DataFrame, row_id: int) -> pd.Series:
    return frame.loc[frame["_id"] == row_id].iloc[0]


def ordered_ids(frame: pd.DataFrame, row_ids: list[int]) -> list[int]:
    if not row_ids:
        return []
    subset = frame.loc[frame["_id"].isin(row_ids), ["_id", "Start Time"]].copy()
    subset["_minutes"] = subset["Start Time"].map(time_to_minutes)
    subset = subset.sort_values(["_minutes", "_id"], na_position="last")
    return [int(value) for value in subset["_id"].tolist()]


def assignable_rows(frame: pd.DataFrame, include_garage: bool) -> pd.DataFrame:
    kind = frame["Type"].map(trip_type)
    keep_type = kind.eq("") | kind.eq("regular")
    if include_garage:
        keep_type = keep_type | kind.isin(GARAGE_TYPES)
    mask = (
        frame["Asn#"].map(clean_token).eq("")
        & keep_type
        & frame["Start Time"].map(time_to_minutes).notna()
        & frame["End Time"].map(time_to_minutes).notna()
    )
    return frame.loc[mask]


def first_open_block(
    frame: pd.DataFrame,
    blocks: list[str],
    include_garage: bool,
    hub: str,
    min_hours: float = 0,
    max_hours: float = 0,
) -> str:
    """First block that can hold a full shift. Falls back to the first block with any open trip."""
    if not blocks:
        return ""
    rows = assignable_rows(frame, include_garage)
    if rows.empty:
        return blocks[0]
    work = rows.copy()
    work["_block"] = work["Block"].map(clean_token)
    work["_start"] = work["Start Time"].map(time_to_minutes)
    grouped = {
        str(block): [int(value) for value in group.sort_values(["_start", "_id"])["_id"].tolist()]
        for block, group in work.groupby("_block", sort=False)
    }
    fallback = ""
    for block in blocks:
        ordered = grouped.get(block) or []
        start_id = find_start_id(frame, ordered, hub, None)
        if start_id is None:
            continue
        if not fallback:
            fallback = block
        if min_hours <= 0:
            return block
        chosen = cut_full_shift(frame, ordered, start_id, hub, min_hours, max_hours, None)
        hours = span_hours(frame, chosen)
        if hours is None or hours + 1e-9 < min_hours:
            continue
        if max_hours > 0 and hours - 1e-9 > max_hours:
            continue
        target = clean_token(_row(frame, chosen[0])["Start Location"]) if hub == ANY_PLACE else hub
        if same_place(_row(frame, chosen[-1])["End Location"], target):
            return block
    return fallback or blocks[0]


def open_counts(frame: pd.DataFrame, include_garage: bool) -> dict[str, int]:
    subset = assignable_rows(frame, include_garage)
    if subset.empty:
        return {}
    counts = subset["Block"].map(clean_token).value_counts()
    return {str(block): int(count) for block, count in counts.items()}


def eligible_ids(frame: pd.DataFrame, block: str, include_garage: bool) -> list[int]:
    subset = assignable_rows(frame, include_garage)
    subset = subset.loc[subset["Block"].map(clean_token).eq(clean_token(block))]
    return ordered_ids(frame, [int(value) for value in subset["_id"].tolist()])


def _gap_minutes(after_minutes: int, start_minutes: int) -> int:
    gap = start_minutes - after_minutes
    if gap < 0:
        gap += 24 * 60
    return gap


def find_start_id(
    frame: pd.DataFrame,
    ordered: list[int],
    hub: str,
    start_after: int | None,
) -> int | None:
    for row_id in ordered:
        row = _row(frame, row_id)
        start_minutes = time_to_minutes(row["Start Time"])
        if start_minutes is None:
            continue
        if start_after is not None and start_minutes < start_after:
            continue
        if hub != ANY_PLACE and not same_place(row["Start Location"], hub):
            continue
        return row_id
    return None


def find_connect_id(
    frame: pd.DataFrame,
    ordered: list[int],
    place: str,
    after_minutes: int,
    min_gap: int,
    max_gap: int,
) -> int | None:
    for row_id in ordered:
        row = _row(frame, row_id)
        if not same_place(row["Start Location"], place):
            continue
        start_minutes = time_to_minutes(row["Start Time"])
        if start_minutes is None:
            continue
        gap = _gap_minutes(after_minutes, start_minutes)
        if min_gap <= gap <= max_gap:
            return row_id
    return None


def connecting_blocks(
    frame: pd.DataFrame,
    place: str,
    after_minutes: int,
    include_garage: bool,
    min_gap: int,
    max_gap: int,
) -> list[dict[str, object]]:
    subset = assignable_rows(frame, include_garage)
    if subset.empty:
        return []
    work = subset.copy()
    work["_block"] = work["Block"].map(clean_token)
    work["_start"] = work["Start Time"].map(time_to_minutes)
    work = work.sort_values(["_start", "_id"])
    wanted = clean_token(place).casefold()
    found: list[dict[str, object]] = []
    seen: set[str] = set()
    for record in work.to_dict("records"):
        block = str(record["_block"])
        if not block or block in seen:
            continue
        if clean_token(record["Start Location"]).casefold() != wanted:
            continue
        start_minutes = record["_start"]
        if start_minutes is None or pd.isna(start_minutes):
            continue
        gap = _gap_minutes(after_minutes, int(start_minutes))
        if not (min_gap <= gap <= max_gap):
            continue
        seen.add(block)
        found.append(
            {
                "block": block,
                "row_id": int(record["_id"]),
                "route": clean_token(record["Route"]),
                "start": record["Start Time"],
                "gap": gap,
            }
        )
    found.sort(key=lambda item: time_to_minutes(item["start"]) or 0)
    return found


def _end_allowed(end_value: object, end_by: int | None) -> bool:
    if end_by is None:
        return True
    end_minutes = time_to_minutes(end_value)
    if end_minutes is None:
        return False
    return end_minutes <= end_by


def cut_full_shift(
    frame: pd.DataFrame,
    ordered: list[int],
    start_id: int,
    hub: str,
    min_hours: float,
    max_hours: float,
    end_by: int | None,
) -> list[int]:
    start_pos = ordered.index(start_id)
    running: list[int] = []
    candidates: list[list[int]] = []
    first_start = _row(frame, start_id)["Start Time"]
    target_place = clean_token(_row(frame, start_id)["Start Location"]) if hub == ANY_PLACE else hub

    for row_id in ordered[start_pos:]:
        row = _row(frame, row_id)
        if not _end_allowed(row["End Time"], end_by):
            break
        hours = duration_hours(first_start, row["End Time"])
        if running and hours is not None and max_hours > 0 and hours - 1e-9 > max_hours:
            break
        running.append(row_id)
        if hours is None:
            continue
        if hours + 1e-9 >= min_hours and same_place(row["End Location"], target_place):
            candidates.append(list(running))

    if candidates:
        target = (min_hours + max_hours) / 2 if max_hours > 0 else min_hours
        return min(
            candidates,
            key=lambda ids: abs((duration_hours(first_start, _row(frame, ids[-1])["End Time"]) or 0) - target),
        )
    return running


def _batch_cut(
    ordered: list[dict[str, object]],
    hub: str,
    route: str,
    min_hours: float,
    max_hours: float,
    start_after: int | None,
    end_by: int | None,
) -> list[int] | None:
    target = "" if hub == ANY_PLACE else clean_token(hub).casefold()
    wanted = "" if not route or route == ALL_ROUTES else clean_token(route).casefold()
    for start_index, first in enumerate(ordered):
        start_minutes = int(first["start"])
        if start_after is not None and start_minutes < start_after:
            continue
        if wanted and clean_token(first["route"]).casefold() != wanted:
            continue
        end_place = clean_token(first["start_place"]).casefold() if not target else target
        if target and clean_token(first["start_place"]).casefold() != target:
            continue
        running: list[int] = []
        candidates: list[list[int]] = []
        for trip in ordered[start_index:]:
            if wanted and clean_token(trip["route"]).casefold() != wanted:
                break
            end_minutes = int(trip["end"])
            if end_by is not None and end_minutes > end_by:
                break
            hours = (end_minutes - start_minutes) / 60
            if hours < 0:
                hours += 24
            if running and max_hours > 0 and hours - 1e-9 > max_hours:
                break
            running.append(int(trip["id"]))
            if hours + 1e-9 >= min_hours and clean_token(trip["end_place"]).casefold() == end_place:
                candidates.append(list(running))
        if not candidates:
            continue
        goal = (min_hours + max_hours) / 2 if max_hours > 0 else min_hours
        ends = {int(trip["id"]): int(trip["end"]) for trip in ordered[start_index:]}

        def distance(ids: list[int]) -> float:
            hours = (ends[ids[-1]] - start_minutes) % (24 * 60) / 60
            return abs(hours - goal)

        return min(candidates, key=distance)
    return None


def plan_batch(
    frame: pd.DataFrame,
    hub: str,
    route: str,
    min_hours: float,
    max_hours: float,
    include_garage: bool,
    start_after: int | None,
    end_by: int | None,
    limit: int = 300,
) -> list[dict[str, object]]:
    """Cut as many same-block shifts as possible. Each one starts and ends at the hub.

    A chosen route never skips the other trips on that block. The shift stops
    when the bus changes route, so the person stays on one continuous piece.
    """
    rows = assignable_rows(frame, include_garage)
    if rows.empty:
        return []
    trips: list[dict[str, object]] = []
    for record in rows.to_dict("records"):
        start_minutes = time_to_minutes(record["Start Time"])
        end_minutes = time_to_minutes(record["End Time"])
        block = clean_token(record["Block"])
        if start_minutes is None or end_minutes is None or not block:
            continue
        trips.append(
            {
                "id": int(record["_id"]),
                "block": block,
                "start": start_minutes,
                "end": end_minutes,
                "route": clean_token(record["Route"]),
                "start_place": clean_token(record["Start Location"]),
                "end_place": clean_token(record["End Location"]),
            }
        )
    trips.sort(key=lambda trip: (int(trip["start"]), int(trip["id"])))
    grouped: dict[str, list[dict[str, object]]] = {}
    for trip in trips:
        grouped.setdefault(str(trip["block"]), []).append(trip)
    reserved: set[int] = set()
    plans: list[dict[str, object]] = []
    while len(plans) < limit:
        best_block = ""
        best_ids: list[int] | None = None
        best_start = 10**9
        for block, ordered_all in grouped.items():
            ordered = [trip for trip in ordered_all if int(trip["id"]) not in reserved]
            chosen = _batch_cut(ordered, hub, route, min_hours, max_hours, start_after, end_by)
            if not chosen:
                continue
            start_minutes = next(int(trip["start"]) for trip in ordered if int(trip["id"]) == chosen[0])
            if start_minutes < best_start:
                best_block = block
                best_ids = chosen
                best_start = start_minutes
        if not best_ids:
            break
        reserved.update(best_ids)
        plans.append({"block": best_block, "row_ids": best_ids})
    return plans


def _clock_label(minutes: int) -> str:
    hour, minute = divmod(int(minutes) % (24 * 60), 60)
    suffix = "AM" if hour < 12 else "PM"
    hour12 = hour % 12 or 12
    return f"{hour12}:{minute:02d} {suffix}"


def _name_list(names: list[str]) -> str:
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return ", ".join(names[:-1]) + f", and {names[-1]}"


def explain_empty_batch(
    frame: pd.DataFrame,
    hub: str,
    route: str,
    min_hours: float,
    max_hours: float,
    include_garage: bool,
    start_after: int | None,
    end_by: int | None,
) -> str:
    """Say why a batch came back empty, in words the user can fix in the file."""
    hub_name = clean_token(hub) or "that place"
    target = hub_name.casefold()
    wanted = "" if not route or route == ALL_ROUTES else clean_token(route).casefold()
    route_name = clean_token(route)
    rows = assignable_rows(frame, include_garage)
    trips: list[dict[str, object]] = []
    for record in rows.to_dict("records"):
        start_minutes = time_to_minutes(record["Start Time"])
        end_minutes = time_to_minutes(record["End Time"])
        block = clean_token(record["Block"])
        if start_minutes is None or end_minutes is None or not block:
            continue
        route_value = clean_token(record["Route"])
        if wanted and route_value.casefold() != wanted:
            continue
        trips.append(
            {
                "id": int(record["_id"]),
                "block": block,
                "start": start_minutes,
                "end": end_minutes,
                "route": route_value,
                "start_place": clean_token(record["Start Location"]),
                "end_place": clean_token(record["End Location"]),
            }
        )
    if not trips:
        if wanted:
            return f"No open trip on route {route_name} has a start and end time."
        return "No open trip has a start and end time."
    grouped: dict[str, list[dict[str, object]]] = {}
    for trip in sorted(trips, key=lambda item: (int(item["start"]), int(item["id"]))):
        grouped.setdefault(str(trip["block"]), []).append(trip)
    starts = [trip for trip in trips if str(trip["start_place"]).casefold() == target]
    ends = [trip for trip in trips if str(trip["end_place"]).casefold() == target]
    if not starts:
        if wanted:
            return f"No open trip on route {route_name} starts at {hub_name}."
        return f"No open trip starts at {hub_name}."
    if not ends:
        returns: dict[str, int] = {}
        for ordered in grouped.values():
            for previous, following in zip(ordered, ordered[1:]):
                if str(following["start_place"]).casefold() != target:
                    continue
                gap = int(following["start"]) - int(previous["end"])
                if gap < 0:
                    gap += 24 * 60
                if gap > 45:
                    continue
                end_place = str(previous["end_place"])
                if end_place.casefold() == target:
                    continue
                returns[end_place] = returns.get(end_place, 0) + 1
        lead = f"{len(starts):,} trips start at {hub_name}, but none end there."
        if returns:
            names = [name for name, _count in sorted(returns.items(), key=lambda item: (-item[1], item[0]))[:3]]
            return (
                f"{lead} The return trips end at {_name_list(names)}. "
                f"Use {hub_name} in End Location as well, then generate again."
            )
        return (
            f"{lead} A shift has to start and end at the same place. "
            f"Use {hub_name} in End Location as well, then generate again."
        )

    def any_cut(after: int | None, by: int | None, low: float, high: float) -> bool:
        for ordered in grouped.values():
            if _batch_cut(ordered, hub, route, low, high, after, by):
                return True
        return False

    if (start_after is not None or end_by is not None) and any_cut(None, None, min_hours, max_hours):
        window: list[str] = []
        if start_after is not None:
            window.append(f"start after {_clock_label(start_after)}")
        if end_by is not None:
            window.append(f"end by {_clock_label(end_by)}")
        return (
            f"Trips do start and end at {hub_name} within {min_hours:g}–{max_hours:g} hours, "
            f"but none of them {' and '.join(window)}."
        )
    if max_hours > 0 and any_cut(None, None, 0, 24):
        return (
            f"Trips do start and end at {hub_name}, but none of them stay within "
            f"{min_hours:g}–{max_hours:g} hours."
        )
    return (
        f"No open trip both starts and ends at {hub_name} within these rules. "
        "Check the place, the hours, and the route."
    )


def cut_round_trip(
    frame: pd.DataFrame,
    ordered: list[int],
    start_id: int,
    hub: str,
    max_hours: float,
    end_by: int | None,
) -> list[int]:
    start_pos = ordered.index(start_id)
    running: list[int] = []
    first = _row(frame, start_id)
    target_place = clean_token(first["Start Location"]) if hub == ANY_PLACE else hub
    for row_id in ordered[start_pos:]:
        row = _row(frame, row_id)
        if not _end_allowed(row["End Time"], end_by):
            break
        hours = duration_hours(first["Start Time"], row["End Time"])
        if running and hours is not None and max_hours > 0 and hours - 1e-9 > max_hours:
            break
        running.append(row_id)
        if same_place(row["End Location"], target_place):
            break
    return running


def span_hours(frame: pd.DataFrame, row_ids: list[int]) -> float | None:
    if not row_ids:
        return None
    return duration_hours(_row(frame, row_ids[0])["Start Time"], _row(frame, row_ids[-1])["End Time"])


def interior_garage(frame: pd.DataFrame, block: str, row_ids: list[int]) -> list[str]:
    if len(row_ids) < 2:
        return []
    first_start = time_to_minutes(_row(frame, row_ids[0])["Start Time"])
    last_end = time_to_minutes(_row(frame, row_ids[-1])["End Time"])
    if first_start is None or last_end is None:
        return []
    notes: list[str] = []
    matched = frame.loc[frame["Block"].map(clean_token).eq(clean_token(block))]
    for _, row in matched.iterrows():
        if not is_garage(row["Type"]) or clean_token(row["Asn#"]):
            continue
        start_minutes = time_to_minutes(row["Start Time"])
        if start_minutes is None or start_minutes < first_start or start_minutes >= last_end:
            continue
        notes.append(f"{clean_token(row['Type'])} at {format_time(row['Start Time'])}")
    return notes


def describe_cut(
    frame: pd.DataFrame,
    row_ids: list[int],
    hub: str,
    min_hours: float,
    max_hours: float,
    mode: str,
    block: str,
) -> list[str]:
    warnings: list[str] = []
    if not row_ids:
        return ["No open trips matched this cut."]
    hours = span_hours(frame, row_ids)
    last = _row(frame, row_ids[-1])
    target = clean_token(_row(frame, row_ids[0])["Start Location"]) if hub == ANY_PLACE else hub
    if mode == "full" and hours is not None and min_hours > 0 and hours + 1e-9 < min_hours:
        warnings.append(f"This cut is {format_hours(hours)}, shorter than {min_hours:g} hours.")
    if hours is not None and max_hours > 0 and hours - 1e-9 > max_hours:
        warnings.append(f"This cut is {format_hours(hours)}, longer than {max_hours:g} hours.")
    if target and not same_place(last["End Location"], target):
        warnings.append(f"This cut does not end at {target}.")
    garage = interior_garage(frame, block, row_ids)
    if garage:
        shown = ", ".join(garage[:3])
        extra = f" and {len(garage) - 3} more" if len(garage) > 3 else ""
        warnings.append(f"Left blank inside this shift: {shown}{extra}.")
    return warnings


def last_assigned_row(frame: pd.DataFrame, label: str) -> pd.Series | None:
    matched = frame.loc[frame["Asn#"].map(clean_token).eq(clean_token(label))].copy()
    if matched.empty:
        return None
    matched["_end"] = matched["End Time"].map(time_to_minutes)
    matched = matched.sort_values(["_end", "_id"], na_position="last")
    return matched.iloc[-1]


def propose_label(frame: pd.DataFrame, requested: str) -> str:
    text = clean_token(requested)
    if text:
        return text
    return str(_next_number(frame))


def _next_number(frame: pd.DataFrame) -> int:
    highest = 0
    for value in frame["Asn#"].tolist():
        text = clean_token(value)
        if text.isdigit():
            highest = max(highest, int(text))
    return highest + 1


def label_exists(frame: pd.DataFrame, label: str) -> bool:
    text = clean_token(label)
    if not text:
        return False
    return bool(frame["Asn#"].map(clean_token).eq(text).any())
