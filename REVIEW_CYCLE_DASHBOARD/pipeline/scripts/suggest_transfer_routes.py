"""
Suggest transfer routes when origin→boarding or alighting→destination walk
distance exceeds threshold (1.85 mi) and transport is Walk.

Uses a stops source to find the most suitable route: either the details file
"stops" sheet or an xfers file (CSV/Excel) with stop and route columns.
"""

import pandas as pd
import argparse
from math import radians, sin, cos, sqrt, atan2
from pathlib import Path

# =========================
# CONFIG (override via CLI)
# =========================
DIST_THRESHOLD_MILES = 1.85

# Logical Elvis column names (we resolve to actual headers below)
ORIGIN_LAT = "ORIGIN_ADDRESS_LAT"
ORIGIN_LON = "ORIGIN_ADDRESS_LONG"
BOARDING_LAT = "STOP_ON_LAT"
BOARDING_LON = "STOP_ON_LONG"
ALIGHTING_LAT = "STOP_OFF_LAT"
ALIGHTING_LON = "STOP_OFF_LONG"
DESTIN_LAT = "DESTIN_ADDRESS_LAT"
DESTIN_LON = "DESTIN_ADDRESS_LONG"
ORIGIN_TRANSPORT = "ORIGIN_TRANSPORT"
DESTIN_TRANSPORT = "DESTIN_TRANSPORT"

# Details stops sheet columns (your schema)
STOPS_LAT = "stop_lat"
STOPS_LON = "stop_lon"
STOPS_ROUTE_ID = "ETC_ROUTE_ID"   # fallback: route_short_name
STOPS_ROUTE_NAME = "ETC_ROUTE_NAME"
STOPS_STOP_ID = "stop_id"
STOPS_STOP_NAME = "stop_name"


def haversine_miles(lat1, lon1, lat2, lon2):
    """Return distance in miles between (lat1,lon1) and (lat2,lon2)."""
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def _norm_col(s):
    """Normalize column name for matching (lowercase, no underscores/spaces)."""
    return str(s).lower().replace("_", "").replace(" ", "")


def resolve_elvis_columns(df):
    """
    Map logical column names to actual column names in the Elvis dataframe.
    Tries exact match, then common aliases, then normalized (lower, no _) match.
    """
    cols = list(df.columns)
    norm_to_col = {_norm_col(c): c for c in cols}
    resolved = {}

    logical_to_try = [
        (ORIGIN_LAT, ["OriginAddress_LAT", "originaddress_lat"]),
        (ORIGIN_LON, ["OriginAddress_LONG", "originaddress_long"]),
        (BOARDING_LAT, ["StopOn_LAT", "stopon_lat"]),
        (BOARDING_LON, ["StopOn_LONG", "stopon_long"]),
        (ALIGHTING_LAT, ["StopOff_LAT", "stopoff_lat"]),
        (ALIGHTING_LON, ["StopOff_LONG", "stopoff_long"]),
        (DESTIN_LAT, ["DestinAddress_LAT", "destinaddress_lat"]),
        (DESTIN_LON, ["DestinAddress_LONG", "destinaddress_long"]),
        (ORIGIN_TRANSPORT, ["OriginTransport", "origintransport"]),
        (DESTIN_TRANSPORT, ["DestinTransport", "destintransport"]),
    ]
    for logical, aliases in logical_to_try:
        if logical in cols:
            resolved[logical] = logical
            continue
        for a in aliases:
            if a in cols:
                resolved[logical] = a
                break
        if logical not in resolved:
            n = _norm_col(logical)
            resolved[logical] = norm_to_col.get(n)
    return resolved


def is_walk(transport_str, transport_code=None):
    """True if transport is walk-like (Walk, wheelchair, skateboard, or code 1/2)."""
    if transport_str and "walk" in str(transport_str).lower():
        return True
    if transport_str and any(x in str(transport_str).lower() for x in ("wheelchair", "skateboard", "walked")):
        return True
    if transport_code is not None and str(transport_code).strip() in ("1", "2", "3"):  # common walk codes
        return True
    return False


def _valid(num):
    """True if num is a finite float (usable as lat or lon)."""
    try:
        x = float(num)
        return pd.notna(x) and abs(x) < 1e10
    except (TypeError, ValueError):
        return False


def _coords(row, lat_col, lon_col):
    lat, lon = row.get(lat_col), row.get(lon_col)
    if _valid(lat) and _valid(lon):
        return float(lat), float(lon)
    return None


def _route_key(stops_df):
    """Column to use for grouping by route (route_id, ETC_ROUTE_ID, route_short_name, or short_clean)."""
    if "route_id" in stops_df.columns:
        return "route_id"
    if STOPS_ROUTE_ID in stops_df.columns:
        return STOPS_ROUTE_ID
    if "XFER_ROUTE_ID" in stops_df.columns:
        return "XFER_ROUTE_ID"
    if "route_short_name" in stops_df.columns:
        return "route_short_name"
    if "short_clean" in stops_df.columns:
        return "short_clean"
    norm = {_norm_col(c): c for c in stops_df.columns}
    if norm.get("routeid"):
        return norm["routeid"]
    if norm.get("xferrouteid"):
        return norm["xferrouteid"]
    if norm.get("routeshortname"):
        return norm["routeshortname"]
    if norm.get("shortclean"):
        return norm["shortclean"]
    raise KeyError("Stops/xfers file needs route_id, ETC_ROUTE_ID, XFER_ROUTE_ID, route_short_name, or short_clean")


def _route_name_col(stops_df):
    """Column to use for route display name."""
    if "route_name" in stops_df.columns:
        return "route_name"
    if STOPS_ROUTE_NAME in stops_df.columns:
        return STOPS_ROUTE_NAME
    if "route_long_name" in stops_df.columns:
        return "route_long_name"
    return _route_key(stops_df)


def _stops_lat_lon_cols(stops_df):
    """Return (lat_col, lon_col) for stops, with normalized fallback (e.g. STOP_LAT -> stop_lat)."""
    for lat, lon in [(STOPS_LAT, STOPS_LON), ("stop_lat", "stop_lon")]:
        if lat in stops_df.columns and lon in stops_df.columns:
            return lat, lon
    norm = {_norm_col(c): c for c in stops_df.columns}
    lat_c = norm.get("stoplat") or norm.get("stop_lat")
    lon_c = norm.get("stoplon") or norm.get("stop_lon")
    if lat_c and lon_c:
        return lat_c, lon_c
    raise KeyError("Stops/xfers file needs stop_lat and stop_lon (or STOP_LAT, STOP_LON)")


# Sheet name for transfer stops in details workbook
XFER_STOPS_SHEET = "XFER_STOPS"

# Standard columns for combined stops (used when merging STOPS + XFER_STOPS)
STOPS_STANDARD_COLS = ["stop_lat", "stop_lon", "route_id", "route_name", "stop_id", "stop_name"]


def _to_standard_stops_df(df, route_id_cols, route_name_cols):
    """
    Normalize a stops-like dataframe to standard columns: stop_lat, stop_lon, route_id, route_name, stop_id, stop_name.
    route_id_cols: list of column names to try for route id (first present wins).
    route_name_cols: list of column names to try for route name (first present wins).
    """
    norm = {_norm_col(c): c for c in df.columns}
    lat_c, lon_c = _stops_lat_lon_cols(df)
    id_c = next((c if c in df.columns else norm.get(_norm_col(c)) for c in ["stop_id", "ETC_STOP_ID"] if (c in df.columns or norm.get(_norm_col(c)))), None)
    name_c = next((c if c in df.columns else norm.get(_norm_col(c)) for c in ["stop_name", "ETC_STOP_NAME"] if (c in df.columns or norm.get(_norm_col(c)))), None)
    if not id_c:
        id_c = list(df.columns)[0]  # fallback
    if not name_c:
        name_c = id_c

    route_id_src = None
    for c in route_id_cols:
        if c in df.columns:
            route_id_src = c
            break
        if norm.get(_norm_col(c)):
            route_id_src = norm[_norm_col(c)]
            break
    route_name_src = None
    for c in route_name_cols:
        if c in df.columns:
            route_name_src = c
            break
        if norm.get(_norm_col(c)):
            route_name_src = norm[_norm_col(c)]
            break

    out = pd.DataFrame()
    out["stop_lat"] = pd.to_numeric(df[lat_c], errors="coerce")
    out["stop_lon"] = pd.to_numeric(df[lon_c], errors="coerce")
    out["stop_id"] = df[id_c] if id_c in df.columns else df.index.astype(str)
    out["stop_name"] = df[name_c] if name_c in df.columns else out["stop_id"].astype(str)
    out["route_id"] = df[route_id_src] if route_id_src else ""
    out["route_name"] = df[route_name_src] if route_name_src else (df[route_id_src].astype(str) if route_id_src else "")
    return out


def load_stops_df(xfers_path=None, details_path=None, stops_sheet="stops", xfers_sheet="xfer-stops v1", xfer_stops_sheet=XFER_STOPS_SHEET):
    """
    Load stops dataframe from xfers file (CSV/Excel) or from details workbook.
    When details_path is used: loads both "stops" and "XFER_STOPS" sheets (if present) and combines them.
    When xfers_path is set, use it and ignore details_path for stops.
    xfers_sheet: sheet name when xfers_path is Excel (default "xfer-stops v1"); ignored for CSV.
    xfer_stops_sheet: sheet name in details workbook for transfer stops (default "XFER_STOPS").
    Normalizes column names so standard names (stop_lat, stop_lon, etc.) exist.
    """
    if xfers_path:
        p = str(xfers_path).lower()
        if p.endswith(".csv"):
            stops_df = pd.read_csv(xfers_path)
        else:
            try:
                xl = pd.ExcelFile(xfers_path)
                sheet = next((s for s in xl.sheet_names if s.strip().lower() == xfers_sheet.strip().lower()), xfers_sheet)
            except Exception:
                sheet = xfers_sheet
            stops_df = pd.read_excel(xfers_path, sheet_name=sheet)
        # Normalize xfers columns to standard names (e.g. STOP_LAT -> stop_lat) for downstream
        renames = {}
        for c in stops_df.columns:
            n = _norm_col(c)
            for std in ["stop_lat", "stop_lon", "stop_id", "stop_name", "route_short_name", "route_long_name", "short_clean", "ETC_ROUTE_ID", "ETC_ROUTE_NAME", "ETC_STOP_ID", "ETC_STOP_NAME", "XFER_ROUTE_ID"]:
                if n == _norm_col(std) and c != std:
                    renames[c] = std
                    break
        if renames:
            stops_df = stops_df.rename(columns=renames)
        return stops_df
    if not details_path:
        raise ValueError("Provide either xfers_path or details_path for stops data")
    # Both STOPS and XFER_STOPS are in the same details workbook
    xl = pd.ExcelFile(details_path)
    sheet_names_lower = {s.strip().lower(): s for s in xl.sheet_names}
    # Load STOPS sheet from details file
    stops_sheet_actual = sheet_names_lower.get(stops_sheet.strip().lower(), stops_sheet)
    stops_df = pd.read_excel(details_path, sheet_name=stops_sheet_actual)
    lat_s, lon_s = _stops_lat_lon_cols(stops_df)
    stops_df[lat_s] = pd.to_numeric(stops_df[lat_s], errors="coerce")
    stops_df[lon_s] = pd.to_numeric(stops_df[lon_s], errors="coerce")
    # Normalize STOPS to standard schema (route_id, route_name)
    stops_std = _to_standard_stops_df(
        stops_df,
        route_id_cols=["ETC_ROUTE_ID", "route_short_name", "short_clean"],
        route_name_cols=["ETC_ROUTE_NAME", "route_long_name"],
    )
    # Load XFER_STOPS sheet if present
    xfer_stops_actual = sheet_names_lower.get(xfer_stops_sheet.strip().lower())
    if xfer_stops_actual:
        xfer_df = pd.read_excel(details_path, sheet_name=xfer_stops_actual)
        xfer_lat, xfer_lon = _stops_lat_lon_cols(xfer_df)
        xfer_df[xfer_lat] = pd.to_numeric(xfer_df[xfer_lat], errors="coerce")
        xfer_df[xfer_lon] = pd.to_numeric(xfer_df[xfer_lon], errors="coerce")
        xfer_std = _to_standard_stops_df(
            xfer_df,
            route_id_cols=["XFER_ROUTE_ID", "route_short_name", "short_clean"],
            route_name_cols=["route_long_name", "route_short_name"],
        )
        stops_df = pd.concat([stops_std, xfer_std], ignore_index=True)
    else:
        stops_df = stops_std
    return stops_df


def find_most_suitable_route(stops_clean, start_lat, start_lon, end_lat, end_lon):
    """
    Find the route that minimizes total walk:
    min over routes of (distance(start, nearest_stop_to_start) + distance(end, nearest_stop_to_end)).

    Returns dict with route_id, route_name, stop_near_start_*, stop_near_end_*, total_walk_miles.
    """
    route_col = _route_key(stops_clean)
    name_col = _route_name_col(stops_clean)
    lat_col = STOPS_LAT if STOPS_LAT in stops_clean.columns else "stop_lat"
    lon_col = STOPS_LON if STOPS_LON in stops_clean.columns else "stop_lon"
    id_col = STOPS_STOP_ID if STOPS_STOP_ID in stops_clean.columns else "stop_id"
    stop_name_col = STOPS_STOP_NAME if STOPS_STOP_NAME in stops_clean.columns else "stop_name"

    best = None
    best_score = float("inf")

    for route_id, grp in stops_clean.groupby(route_col, dropna=False):
        if pd.isna(route_id) or not len(grp):
            continue
        route_name = grp[name_col].iloc[0] if name_col in grp.columns else str(route_id)

        d_start = float("inf")
        d_end = float("inf")
        near_start = None
        near_end = None

        for _, r in grp.iterrows():
            slat, slon = r.get(lat_col), r.get(lon_col)
            if not _valid(slat) or not _valid(slon):
                continue
            slat, slon = float(slat), float(slon)
            ds = haversine_miles(start_lat, start_lon, slat, slon)
            de = haversine_miles(end_lat, end_lon, slat, slon)
            if ds < d_start:
                d_start = ds
                near_start = (r.get(id_col), r.get(stop_name_col), slat, slon)
            if de < d_end:
                d_end = de
                near_end = (r.get(id_col), r.get(stop_name_col), slat, slon)

        if near_start is None or near_end is None:
            continue
        score = d_start + d_end
        if score < best_score:
            best_score = score
            best = {
                "route_id": route_id,
                "route_name": route_name,
                "stop_near_start_id": near_start[0],
                "stop_near_start_name": near_start[1],
                "stop_near_start_lat": near_start[2],
                "stop_near_start_lon": near_start[3],
                "stop_near_end_id": near_end[0],
                "stop_near_end_name": near_end[1],
                "stop_near_end_lat": near_end[2],
                "stop_near_end_lon": near_end[3],
                "total_walk_miles": round(score, 3),
            }
    return best


def run(elvis_path, details_path=None, xfers_path=None, stops_sheet="stops", xfers_sheet="xfer-stops v1", threshold_miles=1.85, output_path=None, id_col="id", verbose=False):
    """
    elvis_path: path to Elvis export (CSV or Excel)
    details_path: path to details workbook (optional if xfers_path is set)
    xfers_path: path to xfers file (CSV or Excel) with stop/route columns; if set, used instead of details
    stops_sheet: sheet name in details workbook (default "stops"); ignored when xfers_path is set
    xfers_sheet: sheet name in xfers Excel file (default "xfer-stops v1"); ignored when xfers_path is CSV
    threshold_miles: flag when walk segment > this (default 1.85)
    output_path: where to write suggested_transfers CSV
    id_col: Elvis row ID column name (or use first column if missing)
    verbose: print diagnostic counts when True
    """
    if str(elvis_path).lower().endswith(".csv"):
        elvis_df = pd.read_csv(elvis_path, dtype=str)
    else:
        elvis_df = pd.read_excel(elvis_path, sheet_name=0, dtype=str)

    # Resolve Elvis column names (export may use OriginAddress_LAT, StopOn_LAT, etc.)
    r = resolve_elvis_columns(elvis_df)
    o_lat = r.get(ORIGIN_LAT)
    o_lon = r.get(ORIGIN_LON)
    b_lat = r.get(BOARDING_LAT)
    b_lon = r.get(BOARDING_LON)
    a_lat = r.get(ALIGHTING_LAT)
    a_lon = r.get(ALIGHTING_LON)
    d_lat = r.get(DESTIN_LAT)
    d_lon = r.get(DESTIN_LON)
    o_tr = r.get(ORIGIN_TRANSPORT)
    d_tr = r.get(DESTIN_TRANSPORT)
    # Transport code columns (e.g. OriginTransportCode)
    o_tr_code = next((c for c in elvis_df.columns if _norm_col(c) == "origintransportcode"), None)
    d_tr_code = next((c for c in elvis_df.columns if _norm_col(c) == "destintransportcode"), None)

    if verbose:
        print("Elvis columns resolved:")
        for k, v in r.items():
            print(f"  {k} -> {v}")
        if o_tr_code:
            print(f"  ORIGIN_TRANSPORT_Code -> {o_tr_code}")
        if d_tr_code:
            print(f"  DESTIN_TRANSPORT_Code -> {d_tr_code}")

    # Resolve id column
    id_actual = id_col if id_col in elvis_df.columns else next((c for c in elvis_df.columns if _norm_col(c) == "id"), list(elvis_df.columns)[0] if len(elvis_df.columns) else "id")

    # Coerce numeric for resolved coord columns
    for col in [o_lat, o_lon, b_lat, b_lon, a_lat, a_lon, d_lat, d_lon]:
        if col and col in elvis_df.columns:
            elvis_df[col] = pd.to_numeric(elvis_df[col], errors="coerce")

    # Load stops from xfers file or details workbook
    stops_df = load_stops_df(xfers_path=xfers_path, details_path=details_path, stops_sheet=stops_sheet, xfers_sheet=xfers_sheet)
    lat_s, lon_s = _stops_lat_lon_cols(stops_df)
    stops_df[lat_s] = pd.to_numeric(stops_df[lat_s], errors="coerce")
    stops_df[lon_s] = pd.to_numeric(stops_df[lon_s], errors="coerce")
    stops_clean = stops_df.dropna(subset=[lat_s, lon_s])

    if stops_clean.empty:
        raise ValueError(f"No valid stops (need {lat_s}, {lon_s}). Check xfers_path or details_path.")

    if verbose:
        print(f"Elvis rows: {len(elvis_df)}")
        print(f"Stops rows: {len(stops_clean)}")

    results = []
    n_prev_candidates = 0  # walk + has coords
    n_prev_over_threshold = 0
    n_next_candidates = 0
    n_next_over_threshold = 0

    for idx, row in elvis_df.iterrows():
        rid = row.get(id_actual, idx)

        # ----- PREV: Origin → Boarding -----
        origin_xy = _coords(row, o_lat, o_lon) if o_lat and o_lon else None
        board_xy = _coords(row, b_lat, b_lon) if b_lat and b_lon else None
        orig_walk = is_walk(row.get(o_tr) if o_tr else None, row.get(o_tr_code) if o_tr_code else None)
        if orig_walk and origin_xy and board_xy:
            n_prev_candidates += 1
            d_ob = haversine_miles(origin_xy[0], origin_xy[1], board_xy[0], board_xy[1])
            if d_ob > threshold_miles:
                n_prev_over_threshold += 1
                best = find_most_suitable_route(
                    stops_clean, origin_xy[0], origin_xy[1], board_xy[0], board_xy[1]
                )
                if best:
                    results.append({
                        id_col: rid,
                        "transfer_type": "PREV",
                        "gap_miles": round(d_ob, 2),
                        "suggested_route_id": best["route_id"],
                        "suggested_route_name": best["route_name"],
                        "stop_near_origin_id": best["stop_near_start_id"],
                        "stop_near_origin_name": best["stop_near_start_name"],
                        "stop_near_boarding_id": best["stop_near_end_id"],
                        "stop_near_boarding_name": best["stop_near_end_name"],
                        "total_walk_miles": best["total_walk_miles"],
                    })

        # ----- NEXT: Alighting → Destination -----
        off_xy = _coords(row, a_lat, a_lon) if a_lat and a_lon else None
        dest_xy = _coords(row, d_lat, d_lon) if d_lat and d_lon else None
        dest_walk = is_walk(row.get(d_tr) if d_tr else None, row.get(d_tr_code) if d_tr_code else None)
        if dest_walk and off_xy and dest_xy:
            n_next_candidates += 1
            d_ad = haversine_miles(off_xy[0], off_xy[1], dest_xy[0], dest_xy[1])
            if d_ad > threshold_miles:
                n_next_over_threshold += 1
                best = find_most_suitable_route(
                    stops_clean, off_xy[0], off_xy[1], dest_xy[0], dest_xy[1]
                )
                if best:
                    results.append({
                        id_col: rid,
                        "transfer_type": "NEXT",
                        "gap_miles": round(d_ad, 2),
                        "suggested_route_id": best["route_id"],
                        "suggested_route_name": best["route_name"],
                        "stop_near_alighting_id": best["stop_near_start_id"],
                        "stop_near_alighting_name": best["stop_near_start_name"],
                        "stop_near_dest_id": best["stop_near_end_id"],
                        "stop_near_dest_name": best["stop_near_end_name"],
                        "total_walk_miles": best["total_walk_miles"],
                    })

    if verbose:
        print("Diagnostics:")
        print(f"  PREV: origin=walk + has coords: {n_prev_candidates}, distance > {threshold_miles} mi: {n_prev_over_threshold}")
        print(f"  NEXT: destin=walk + has coords: {n_next_candidates}, distance > {threshold_miles} mi: {n_next_over_threshold}")
        print(f"  Suggestions found: {len(results)}")

    out_df = pd.DataFrame(results)
    out_path = output_path or Path(elvis_path).parent / "suggested_transfer_routes.csv"
    out_df.to_csv(out_path, index=False)
    print(f"Done. {len(out_df)} transfer suggestions written to {out_path}")
    return out_df


def main():
    ap = argparse.ArgumentParser(
        description="Suggest transfer routes when origin↔boarding or alighting↔destination walk > threshold and transport is Walk."
    )
    ap.add_argument("elvis_file", help="Elvis database export (CSV or Excel)")
    ap.add_argument("details_file", nargs="?", default=None, help="Details workbook (Excel) with stops sheet; omit if using --xfers")
    ap.add_argument("--xfers", "-x", default=None, metavar="FILE", help="Use xfers file (CSV or Excel) instead of details file for stops/routes")
    ap.add_argument("--xfers-sheet", default="xfer-stops v1", metavar="NAME", help="Sheet name in xfers Excel file (default: xfer-stops v1); ignored for CSV")
    ap.add_argument("--stops-sheet", default="stops", help="Sheet name in details workbook (default: stops); ignored with --xfers")
    ap.add_argument("--threshold", type=float, default=DIST_THRESHOLD_MILES, help="Distance threshold in miles (default: 1.85)")
    ap.add_argument("--output", "-o", default=None, help="Output CSV path")
    ap.add_argument("--id-column", default="id", help="Elvis row ID column name")
    ap.add_argument("--verbose", "-v", action="store_true", help="Print column mapping and diagnostic counts")
    args = ap.parse_args()

    if not args.xfers and not args.details_file:
        ap.error("Provide either details_file or --xfers FILE for stops data")

    run(
        elvis_path=args.elvis_file,
        details_path=args.details_file,
        xfers_path=args.xfers,
        stops_sheet=args.stops_sheet,
        xfers_sheet=args.xfers_sheet,
        threshold_miles=args.threshold,
        output_path=args.output,
        id_col=args.id_column,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()



# Run Command
# .\suggest_transfer_routes.py .\elvis_transit_ls6_733524_export_odbc.csv .\details_lacmta-feeder_733524_od_excel.xlsx --stops-sheet STOPS --verbose

