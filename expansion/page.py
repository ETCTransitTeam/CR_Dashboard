"""Survey Expansion screen. Super admins open this from the portal card."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from expansion.engine import KEY
from expansion.runs import (
    adjust,
    default_workbook,
    export_bundle,
    export_fix,
    load_legacy,
    run_workbook,
    status_message,
)

GROUP = KEY[:3]
VIEWS = {
    "Checks": ["CHECK", *KEY, "ACTUAL", "EXPECTED", "DETAIL"],
    "Areas to fix": ["ISSUE", *GROUP, "SURVEYS", "RIDERSHIP", "ACTION"],
    "Expansion cells": [
        *KEY,
        "TABLE6_RIDERSHIP",
        "RIDERSHIP_USED",
        "SURVEYS",
        "RAW_WEIGHT",
        "FINAL_WEIGHT",
        "FINAL_NORMALIZED_WEIGHT",
        "FINAL_EXPANDED",
        "WEIGHT_SOURCE",
        "POOL",
    ],
    "Pooling & overrides": ["ACTION", *KEY, "VALUE", "MEMBERS"],
    "Route totals": [
        *GROUP,
        "APC_TOTAL",
        "FITTED_RIDERSHIP",
        "ADJUSTED_RIDERSHIP",
        "EXPANDED_RIDERSHIP",
        "OD_SURVEYS",
    ],
}
LABELS = {
    "ROUTE_SURVEY": "Route",
    "FINAL_ROUTE_DIRECTION": "Direction",
    "TIME_PERIOD": "Time",
    "BOARDING_SEG": "Board",
    "ALIGHTING_SEG": "Alight",
    "TABLE6_RIDERSHIP": "Original fitted ridership",
    "RIDERSHIP_USED": "Adjusted ridership",
    "RAW_WEIGHT": "Raw weight",
    "FINAL_WEIGHT": "Final weight",
    "FINAL_NORMALIZED_WEIGHT": "Normalized weight",
    "FINAL_EXPANDED": "Expanded riders",
    "WEIGHT_SOURCE": "Weight source",
    "OD_SURVEYS": "OD surveys",
    "APC_TOTAL": "APC target",
}
RESULT_FRAMES = {
    "Checks": "checks",
    "Areas to fix": "areas_to_fix",
    "Expansion cells": "cells",
    "Pooling & overrides": "adjustments",
    "Route totals": "groups",
}


def _selected_rows(event) -> list[int]:
    if event is None:
        return []
    selection = event.get("selection") if isinstance(event, dict) else getattr(event, "selection", None)
    if selection is None:
        return []
    rows = selection.get("rows") if isinstance(selection, dict) else getattr(selection, "rows", None)
    return list(rows or [])


def _forget_downloads() -> None:
    st.session_state.pop("expansion_fix", None)
    st.session_state.pop("expansion_zip", None)


def _members(frame: pd.DataFrame) -> list[list[str]]:
    return [[str(row[column]) for column in KEY] for _, row in frame.iterrows()]


def _same_group(frame: pd.DataFrame) -> bool:
    groups = {tuple(str(row[column]) for column in GROUP) for _, row in frame.iterrows()}
    return len(groups) == 1


def _show_table(frame: pd.DataFrame, columns: list[str], widget_key: str, selectable: bool = False) -> pd.DataFrame:
    present = [column for column in columns if column in frame.columns]
    view = frame.loc[:, present] if present else frame.iloc[:, 0:0]
    query = st.text_input("Search", key=f"{widget_key}_search", placeholder="Search route, segment, or check")
    if query.strip():
        needle = query.strip().lower()
        mask = (
            view.astype(str)
            .apply(lambda column: column.str.lower().str.contains(needle, na=False, regex=False))
            .any(axis=1)
        )
        view = view.loc[mask]
    display = view.rename(columns=LABELS)
    if display.empty:
        st.info("No matching rows.")
        return display.iloc[0:0]
    if not selectable:
        st.dataframe(display, use_container_width=True, hide_index=True)
        return display.iloc[0:0]
    event = st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="multi-row",
        key=widget_key,
    )
    picked = _selected_rows(event)
    if not picked:
        return view.iloc[0:0]
    return view.iloc[picked]


def _download(slot: str, label: str, mime: str, revision: int) -> None:
    packed = st.session_state.get(slot)
    if not packed or packed.get("rev") != revision:
        return
    st.download_button(
        label,
        data=packed["data"],
        file_name=packed["name"],
        mime=mime,
        key=f"{slot}_download_{revision}",
        use_container_width=True,
    )


def render_survey_expansion_page() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    title_col, action_col = st.columns([5, 1], vertical_alignment="center")
    with title_col:
        st.title("Survey Expansion")
        st.caption("Upload one workbook with O2O, OD, APC, and SAMPLING sheets.")
    with action_col:
        if st.button("Switch Portal", key="expansion_switch_portal", use_container_width=True):
            st.query_params["page"] = "portal_select"
            st.rerun()

    legacy, legacy_warning = load_legacy()
    if legacy_warning:
        st.warning(legacy_warning)
    fallback = default_workbook()

    uploaded = st.file_uploader("Input workbook", type=["xlsx"], key="expansion_workbook")
    if uploaded is not None:
        st.caption(uploaded.name)
    elif fallback is not None:
        st.caption(f"Current workbook: {fallback.name}")
    else:
        st.caption("Choose an .xlsx workbook to begin.")

    rate_col, auto_col, legacy_col = st.columns([1, 2, 2])
    with rate_col:
        rate = st.number_input(
            "Sampling rate %",
            min_value=0.001,
            max_value=100.0,
            value=4.25,
            step=0.25,
            key="expansion_rate",
        )
    with auto_col:
        st.markdown("<div style='height: 1.7rem'></div>", unsafe_allow_html=True)
        auto = st.checkbox("Pool nearby segments for new inputs", value=True, key="expansion_auto")
    with legacy_col:
        st.markdown("<div style='height: 1.7rem'></div>", unsafe_allow_html=True)
        if legacy is not None:
            use_legacy = st.checkbox(
                "Use reviewed Excel adjustments when inputs match",
                value=True,
                key="expansion_existing",
            )
        else:
            use_legacy = False
    if 0 < float(rate) <= 100:
        st.caption(f"Guideline weight factor: {300 / float(rate):,.6f}")

    if st.button("Run expansion", type="primary", key="expansion_run"):
        try:
            if uploaded is not None:
                data = uploaded.getvalue()
                name = uploaded.name
                size = uploaded.size
            elif fallback is not None:
                data = fallback.read_bytes()
                name = fallback.name
                size = len(data)
            else:
                raise ValueError("Select your input workbook first.")
            with st.spinner("Reading inputs and fitting ridership…"):
                state = run_workbook(data, name, float(rate), bool(auto), bool(use_legacy), legacy)
            state["input_size"] = size
            st.session_state["expansion_state"] = state
            _forget_downloads()
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    state = st.session_state.get("expansion_state")
    if not state:
        st.info("Results appear here after the expansion runs.")
        return

    dirty = False
    if uploaded is not None and (
        uploaded.name != state.get("input_name") or int(uploaded.size) != int(state.get("input_size") or -1)
    ):
        dirty = True
    if float(rate) != float(state.get("rate")) or bool(auto) != bool(state.get("auto")):
        dirty = True
    if bool(use_legacy) != bool(state.get("existing")) and legacy is not None:
        dirty = True

    info = state["result"]["info"]
    metric_cols = st.columns(4)
    metric_cols[0].metric("OD surveys", f"{int(info.get('od_records') or 0):,}")
    metric_cols[1].metric("Expansion cells", f"{int(info.get('expansion_cells') or 0):,}")
    metric_cols[2].metric("APC ridership", f"{float(info.get('apc_ridership') or 0):,.3f}")
    metric_cols[3].metric("Final expanded", f"{float(info.get('final_expanded_ridership') or 0):,.3f}")
    st.caption(info.get("adjustment_mode") or "")
    if dirty:
        st.warning("Inputs or settings changed. Run expansion again to update the results.")
    else:
        st.success(status_message(state["result"]))
    edits = len(state.get("manual") or [])
    if edits:
        st.caption(f"{edits} session edit{'s' if edits != 1 else ''}.")

    revision = int(state.get("rev") or 0)
    export_cols = st.columns(2)
    with export_cols[0]:
        if st.button("Prepare areas to fix", disabled=dirty, key="expansion_prepare_fix", use_container_width=True):
            try:
                with st.spinner("Preparing the fix list and updated APC workbook…"):
                    data, name = export_fix(state)
                st.session_state["expansion_fix"] = {"rev": revision, "data": data, "name": name}
            except Exception as exc:
                st.error(str(exc))
        _download(
            "expansion_fix",
            "Download areas to fix",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            revision,
        )
    with export_cols[1]:
        if st.button("Prepare Excel + CSV", disabled=dirty, key="expansion_prepare_zip", use_container_width=True):
            try:
                with st.spinner("Creating the Excel workbook and CSV bundle…"):
                    data, name = export_bundle(state)
                st.session_state["expansion_zip"] = {"rev": revision, "data": data, "name": name}
            except Exception as exc:
                st.error(str(exc))
        _download("expansion_zip", "Download Excel + CSV", "application/zip", revision)

    tabs = st.tabs([*VIEWS.keys(), "Method & help"])
    for tab, title in zip(tabs, VIEWS):
        with tab:
            frame = state["result"].get(RESULT_FRAMES[title])
            if not isinstance(frame, pd.DataFrame):
                st.info("No rows for this view.")
                continue
            selectable = title == "Expansion cells" and not dirty
            chosen = _show_table(
                frame,
                VIEWS[title],
                f"expansion_table_{title}_{revision}",
                selectable=selectable,
            )
            if title != "Expansion cells" or dirty:
                continue
            st.caption("Select cells from the same route, direction, and time period.")
            pool_col, weight_col, clear_col = st.columns(3)
            with pool_col:
                if st.button("Pool selected cells", key=f"expansion_pool_{revision}", use_container_width=True):
                    if len(chosen) < 2:
                        st.error("Select at least two cells to pool.")
                    elif not _same_group(chosen):
                        st.error("Select cells from the same route, direction, and time period.")
                    else:
                        try:
                            st.session_state["expansion_state"] = adjust(state, {"members": _members(chosen)})
                            _forget_downloads()
                            st.rerun()
                        except Exception as exc:
                            st.error(str(exc))
            with weight_col:
                weight = st.number_input(
                    "Reviewed weight",
                    min_value=0.0,
                    value=0.0,
                    step=0.1,
                    key=f"expansion_weight_{revision}",
                )
                if st.button("Set reviewed weight", key=f"expansion_set_weight_{revision}", use_container_width=True):
                    if chosen.empty:
                        st.error("Select cells first.")
                    else:
                        try:
                            st.session_state["expansion_state"] = adjust(
                                state,
                                {"members": _members(chosen), "weight": float(weight)},
                            )
                            _forget_downloads()
                            st.rerun()
                        except Exception as exc:
                            st.error(str(exc))
            with clear_col:
                st.markdown("<div style='height: 1.7rem'></div>", unsafe_allow_html=True)
                if st.button("Clear session edits", key=f"expansion_clear_{revision}", use_container_width=True):
                    try:
                        st.session_state["expansion_state"] = adjust(state, {"clear": True})
                        _forget_downloads()
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
    with tabs[-1]:
        method_path = Path(__file__).resolve().parent / "METHOD.md"
        st.markdown(method_path.read_text(encoding="utf-8"))
