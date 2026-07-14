"""Field Assignments portal page (RunCut upload, assign, export)."""
from __future__ import annotations

import html

import streamlit as st

from field_assignments.core.assign import fill_assignment_numbers
from field_assignments.core.constants import ANY_LOCATION, ANY_ROUTE
from field_assignments.core.export_docs import COMBINED_DOC_FILENAME, export_reports_zip
from field_assignments.core.storage import list_workbook_versions, load_workbook_version, save_workbook_version
from field_assignments.core.summary import (
    coverage_summary,
    default_tod_ranges,
    start_location_routes_summary,
    style_coverage_dataframe,
    style_start_location_dataframe,
)
from field_assignments.core.time_utils import parse_assignment_filter
from field_assignments.core.workbook import build_header_template, workbook_options

MAX_ASSIGNMENT_GUIDELINES = 50
MAX_ASSIGNMENTS_PER_GUIDELINE = 50
FA_TABS = {
    "export": "📄 Export Reports",
    "assign": "✏️ Create Assignments",
    "summary": "📊 Summary & Versions",
}
COVERAGE_VIEWS = {
    "route": "Overall Route",
    "route_direction": "Route Direction",
    "route_tod": "Overall Route & Time of Day",
    "route_direction_tod": "Route Direction & Time of Day",
}


def _workbook_bytes() -> bytes | None:
    return st.session_state.get("fa_workbook_bytes")


def _set_workbook(uploaded, sheet_name: str | None = None) -> None:
    st.session_state["fa_workbook_bytes"] = uploaded.getvalue()
    st.session_state["fa_workbook_name"] = uploaded.name
    st.session_state["fa_sheet_name"] = sheet_name or None
    try:
        st.session_state["fa_workbook_options"] = workbook_options(
            st.session_state["fa_workbook_bytes"],
            st.session_state["fa_sheet_name"],
        )
    except Exception as exc:
        st.session_state.pop("fa_workbook_options", None)
        raise exc


def _render_header_template_download(key: str) -> None:
    st.download_button(
        "Download Headers Template",
        data=build_header_template(),
        file_name="RunCut_Headers_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=key,
        use_container_width=True,
    )


def _assignment_guideline_count() -> int:
    return int(st.session_state.get("fa_assignment_count", 1))


def _ensure_assignment_guideline_count() -> None:
    if "fa_assignment_count" not in st.session_state:
        st.session_state["fa_assignment_count"] = 1


_ASSIGNMENT_WIDGET_SUFFIXES = (
    "use",
    "block",
    "route",
    "start_loc",
    "end_loc",
    "start_from",
    "start_to",
    "end_from",
    "end_to",
    "count",
    "route_only",
)


def _assignment_widget_key(index: int, suffix: str) -> str:
    return f"fa_assignment_{index}_{suffix}"


def _guideline_store_key(index: int) -> str:
    return f"fa_guideline_store_{index}"


def _get_guideline_store(index: int) -> dict[str, str]:
    key = _guideline_store_key(index)
    if key not in st.session_state:
        st.session_state[key] = {
            "block": "(All blocks)",
            "route": "",
            "start_loc": "",
            "end_loc": "",
            "count": "1",
            "route_only": "false",
        }
    store = st.session_state[key]
    store.setdefault("count", "1")
    store.setdefault("route_only", "false")
    return store


def _save_guideline_store(index: int) -> None:
    store = _get_guideline_store(index)
    store["block"] = str(
        st.session_state.get(_assignment_widget_key(index, "block"), store["block"]) or store["block"]
    )
    store["route"] = str(
        st.session_state.get(_assignment_widget_key(index, "route"), store["route"]) or ""
    )
    store["start_loc"] = str(
        st.session_state.get(_assignment_widget_key(index, "start_loc"), store["start_loc"]) or ""
    )
    store["end_loc"] = str(
        st.session_state.get(_assignment_widget_key(index, "end_loc"), store["end_loc"]) or ""
    )
    count_val = st.session_state.get(_assignment_widget_key(index, "count"), store.get("count", "1"))
    store["count"] = str(count_val if count_val is not None else "1")
    store["route_only"] = (
        "true" if st.session_state.get(_assignment_widget_key(index, "route_only"), False) else "false"
    )


def _restore_guideline_store_to_widgets(index: int) -> None:
    store = _get_guideline_store(index)
    st.session_state[_assignment_widget_key(index, "use")] = True
    for suffix in ("block", "route", "start_loc", "end_loc"):
        value = store.get(suffix, "")
        if value:
            st.session_state[_assignment_widget_key(index, suffix)] = value
    try:
        st.session_state[_assignment_widget_key(index, "count")] = int(float(store.get("count", "1") or "1"))
    except ValueError:
        st.session_state[_assignment_widget_key(index, "count")] = 1
    st.session_state[_assignment_widget_key(index, "route_only")] = store.get("route_only") == "true"
    if store.get("route"):
        st.session_state[_loc_route_key(index)] = store["route"]


def _seed_assignment_guideline_defaults(index: int) -> None:
    use_key = _assignment_widget_key(index, "use")
    if use_key in st.session_state:
        return
    st.session_state[use_key] = True
    st.session_state[_assignment_widget_key(index, "block")] = "(All blocks)"
    for suffix in ("route", "start_loc", "end_loc"):
        st.session_state[_assignment_widget_key(index, suffix)] = ""
    st.session_state[_assignment_widget_key(index, "count")] = 1
    st.session_state[_assignment_widget_key(index, "route_only")] = False


def _selectbox_options(options: list[str], current: str, *, include_blank: bool = True) -> list[str]:
    """Build selectbox options and keep the current session value even if not in the route map."""
    ordered: list[str] = [""] if include_blank else []
    seen = set(ordered)
    for value in options:
        text = str(value)
        if text and text not in seen:
            ordered.append(text)
            seen.add(text)
    text = str(current or "")
    if text and text not in seen:
        ordered.append(text)
    return ordered


def _location_options(
    route_sel: str,
    route_map: dict[str, list[str]],
    fallback: list[str],
) -> list[str]:
    if not route_sel or route_sel == ANY_ROUTE:
        return list(fallback or [])
    if route_sel in route_map:
        return list(route_map[route_sel] or [])
    for key, values in route_map.items():
        if str(key) == str(route_sel):
            return list(values or [])
    return list(fallback or [])


def _clear_guidelines_after_fill() -> None:
    """Reset guideline widgets so the user can keep adding on the updated workbook."""
    count = _assignment_guideline_count()
    for index in range(1, count + 1):
        _clear_assignment_widget_state(index)
        st.session_state.pop(_guideline_store_key(index), None)
    st.session_state["fa_assignment_count"] = 1
    _seed_assignment_guideline_defaults(1)


def _reset_to_new_runcut() -> None:
    """Clear workbook and assignment state so a new RunCut can be uploaded."""
    count = _assignment_guideline_count()
    for index in range(1, count + 1):
        _clear_assignment_widget_state(index)
        st.session_state.pop(_guideline_store_key(index), None)
    for key in (
        "fa_workbook_bytes",
        "fa_workbook_name",
        "fa_sheet_name",
        "fa_workbook_options",
        "fa_rules_loaded",
        "fa_updated_workbook",
        "fa_last_export_zip",
        "fa_last_export_summary",
        "fa_last_export_combined",
        "fa_assignment_count",
        "fa_restore_guidelines",
        "fa_pending_remove_index",
        "fa_fill_success",
        "fa_coverage_ready",
    ):
        st.session_state.pop(key, None)


def _loc_route_key(index: int) -> str:
    return f"fa_assignment_{index}_loc_route"


def _reset_assignment_locations(index: int) -> None:
    st.session_state[_assignment_widget_key(index, "start_loc")] = ""
    st.session_state[_assignment_widget_key(index, "end_loc")] = ""


def _apply_route_location_binding(index: int, route_sel: str) -> None:
    """Clear locations only when the route truly changed, not on add/rerun."""
    bound_route = st.session_state.get(_loc_route_key(index))
    if bound_route not in (None, "") and str(bound_route) != str(route_sel):
        _reset_assignment_locations(index)
    if route_sel:
        st.session_state[_loc_route_key(index)] = route_sel


def _clear_assignment_widget_state(index: int) -> None:
    for suffix in _ASSIGNMENT_WIDGET_SUFFIXES:
        st.session_state.pop(_assignment_widget_key(index, suffix), None)
    st.session_state.pop(_loc_route_key(index), None)


def _restore_all_guideline_stores() -> None:
    for index in range(1, _assignment_guideline_count() + 1):
        _restore_guideline_store_to_widgets(index)


def _remove_assignment_guideline(index: int) -> None:
    count = _assignment_guideline_count()
    if count <= 1 or index < 1 or index > count:
        return

    for position in range(1, count + 1):
        _save_guideline_store(position)

    remaining_stores = [_get_guideline_store(position).copy() for position in range(1, count + 1)]
    remaining_stores.pop(index - 1)

    for position in range(1, count + 1):
        _clear_assignment_widget_state(position)
        st.session_state.pop(_guideline_store_key(position), None)

    st.session_state["fa_assignment_count"] = count - 1
    for new_index, store in enumerate(remaining_stores, start=1):
        st.session_state[_guideline_store_key(new_index)] = store
        _restore_guideline_store_to_widgets(new_index)


def _queue_remove_assignment_guideline(index: int) -> None:
    st.session_state["fa_pending_remove_index"] = index


def _process_pending_guideline_removals() -> bool:
    pending_remove = st.session_state.pop("fa_pending_remove_index", None)
    if pending_remove is None:
        return False
    _remove_assignment_guideline(int(pending_remove))
    return True


def _build_guidelines_list(
    opts: dict[str, object],
    guideline_count: int,
    *,
    scan_order: str,
    tolerance: int,
) -> list[dict[str, str]]:
    blocks = ["(All blocks)"] + list(opts.get("blocks") or [])
    guidelines_list: list[dict[str, str]] = []

    for index in range(1, guideline_count + 1):
        use_key = f"fa_assignment_{index}_use"
        if not st.session_state.get(use_key):
            continue

        block_sel = st.session_state.get(f"fa_assignment_{index}_block", blocks[0])
        route_sel = str(st.session_state.get(f"fa_assignment_{index}_route", "") or "")
        start_loc = str(st.session_state.get(f"fa_assignment_{index}_start_loc", "") or "")
        end_loc = str(st.session_state.get(f"fa_assignment_{index}_end_loc", "") or "")
        start_from = st.session_state.get(f"fa_assignment_{index}_start_from")
        start_to = st.session_state.get(f"fa_assignment_{index}_start_to")
        end_from = st.session_state.get(f"fa_assignment_{index}_end_from")
        end_to = st.session_state.get(f"fa_assignment_{index}_end_to")
        count_raw = st.session_state.get(f"fa_assignment_{index}_count", 1)
        route_only = bool(st.session_state.get(f"fa_assignment_{index}_route_only", False))

        # Route/locations may be specific or Any; blank is not enough on its own.
        if not route_sel or not start_loc or not end_loc:
            continue
        if not all([start_from, start_to, end_from, end_to]):
            continue

        try:
            count = max(1, min(MAX_ASSIGNMENTS_PER_GUIDELINE, int(count_raw or 1)))
        except (TypeError, ValueError):
            count = 1

        include_interlined = not route_only
        guidelines_list.append(
            {
                "block": "" if block_sel == "(All blocks)" else str(block_sel),
                "route": "" if route_sel == ANY_ROUTE else route_sel,
                "start_location": "" if start_loc == ANY_LOCATION else start_loc,
                "end_location": "" if end_loc == ANY_LOCATION else end_loc,
                "start_from": start_from.strftime("%H:%M"),
                "start_to": start_to.strftime("%H:%M"),
                "shift_from": end_from.strftime("%H:%M"),
                "shift_to": end_to.strftime("%H:%M"),
                "scan_order": scan_order,
                "tolerance": str(tolerance),
                "include_interlined": "true" if include_interlined else "false",
                "mode": "route_only" if route_only else "block",
                "route_only": "true" if route_only else "false",
                "count": str(count),
            }
        )

    return guidelines_list


@st.fragment
def _render_assign_guidelines_and_output() -> None:
    """Interactive assignment configuration; reruns only this section on widget changes."""
    opts = st.session_state.get("fa_workbook_options") or {}
    if not opts:
        st.warning("Workbook options are not loaded.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Blank Asn# rows", int(opts["blank_rows"]))
    c2.metric("Next Asn#", int(opts["next_assignment"]))
    c3.metric("Sheet", str(opts["sheet"]))
    st.caption(
        "Each guideline can create one or more assignment numbers. "
        "After filling, keep adding from step 3 on the updated workbook."
    )

    with st.container(border=True):
        _step_title(2, "Assignment settings")
        set_col1, set_col2 = st.columns(2)
        with set_col1:
            tolerance = st.selectbox(
                "Time tolerance",
                options=[
                    ("Exact times", 0),
                    ("+/- 5 minutes", 5),
                    ("+/- 10 minutes", 10),
                    ("+/- 15 minutes", 15),
                    ("+/- 20 minutes", 20),
                    ("+/- 30 minutes", 30),
                    ("+/- 45 minutes", 45),
                    ("+/- 60 minutes", 60),
                    ("+/- 90 minutes", 90),
                    ("+/- 120 minutes", 120),
                ],
                format_func=lambda item: item[0],
                index=3,
                key="fa_tolerance",
            )[1]
        with set_col2:
            scan_order = st.selectbox(
                "Scan order",
                options=[
                    ("Top to bottom", "top_to_bottom"),
                    ("Bottom to top", "bottom_to_top"),
                    ("Random", "random"),
                ],
                format_func=lambda item: item[0],
                key="fa_scan_order",
            )[1]
        st.caption(
            "Default mode stays on the same block (interlining). "
            "Enable **No Interline (Route Only)** on a guideline to stay on one route across blocks."
        )

    route_start = opts.get("route_start_locations") or {}
    route_end = opts.get("route_end_locations") or {}
    blocks = ["(All blocks)"] + list(opts.get("blocks") or [])
    routes = list(opts.get("routes") or [])
    route_choices = [ANY_ROUTE] + routes

    if _process_pending_guideline_removals():
        st.rerun()

    guideline_count = _assignment_guideline_count()
    if st.session_state.pop("fa_restore_guidelines", False):
        _restore_all_guideline_stores()

    with st.container(border=True):
        _step_title(3, "Assignment guidelines")
        st.caption(
            "Start Location and Start From/To find the first row. "
            "Final End Location and End From/To find the last row. "
            "Use (Any route) / (Any location) for bulk searches across routes."
        )
        add_col, remove_col, info_col = st.columns([1, 1, 2])
        with add_col:
            if st.button(
                "Add Assignment",
                key="fa_add_assignment",
                use_container_width=True,
                disabled=guideline_count >= MAX_ASSIGNMENT_GUIDELINES,
            ):
                for index in range(1, guideline_count + 1):
                    _save_guideline_store(index)
                new_index = min(guideline_count + 1, MAX_ASSIGNMENT_GUIDELINES)
                st.session_state["fa_assignment_count"] = new_index
                _seed_assignment_guideline_defaults(new_index)
                st.session_state["fa_restore_guidelines"] = True
                st.rerun()
        with remove_col:
            if st.button(
                "Remove Last",
                key="fa_remove_last_assignment",
                use_container_width=True,
                disabled=guideline_count <= 1,
                help="Remove the last assignment guideline.",
            ):
                _queue_remove_assignment_guideline(guideline_count)
                st.rerun()
        with info_col:
            st.caption(
                f"Showing {guideline_count} assignment guideline(s). "
                f"Maximum {MAX_ASSIGNMENT_GUIDELINES}."
            )

        for index in range(1, guideline_count + 1):
            use_key = _assignment_widget_key(index, "use")
            if use_key not in st.session_state:
                st.session_state[use_key] = True
            label_col, remove_col = st.columns([5, 1], vertical_alignment="center")
            with label_col:
                st.checkbox(f"Assignment #{index} Guideline", key=use_key)
            with remove_col:
                if guideline_count > 1 and st.button(
                    "Remove",
                    key=f"fa_remove_assignment_{index}",
                    use_container_width=True,
                    help=f"Remove Assignment #{index} guideline.",
                ):
                    _queue_remove_assignment_guideline(index)
                    st.rerun()
            if not st.session_state.get(use_key):
                continue

            block_key = _assignment_widget_key(index, "block")
            route_key = _assignment_widget_key(index, "route")
            start_key = _assignment_widget_key(index, "start_loc")
            end_key = _assignment_widget_key(index, "end_loc")
            count_key = _assignment_widget_key(index, "count")
            route_only_key = _assignment_widget_key(index, "route_only")
            store = _get_guideline_store(index)
            route_sel = str(st.session_state.get(route_key, store.get("route", "")) or "")

            with st.container(border=True):
                st.markdown(f"**Assignment #{index}**")
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    st.selectbox(
                        "Block",
                        _selectbox_options(blocks, str(st.session_state.get(block_key, store["block"]))),
                        key=block_key,
                    )
                with c2:
                    st.selectbox(
                        "Route",
                        _selectbox_options(route_choices, route_sel),
                        key=route_key,
                    )
                route_sel = str(st.session_state.get(route_key, "") or "")
                _apply_route_location_binding(index, route_sel)
                start_locs = _location_options(
                    route_sel,
                    route_start,
                    list(opts.get("start_locations") or []),
                )
                end_locs = _location_options(
                    route_sel,
                    route_end,
                    list(opts.get("end_locations") or []),
                )
                with c3:
                    st.selectbox(
                        "Start location",
                        _selectbox_options(
                            [ANY_LOCATION] + start_locs,
                            str(st.session_state.get(start_key, store.get("start_loc", ""))),
                        ),
                        key=start_key,
                    )
                with c4:
                    st.selectbox(
                        "Final end location",
                        _selectbox_options(
                            [ANY_LOCATION] + end_locs,
                            str(st.session_state.get(end_key, store.get("end_loc", ""))),
                        ),
                        key=end_key,
                    )

                t1, t2, t3, t4 = st.columns(4)
                with t1:
                    st.time_input("Start from", key=f"fa_assignment_{index}_start_from")
                with t2:
                    st.time_input("Start to", key=f"fa_assignment_{index}_start_to")
                with t3:
                    st.time_input("End from", key=f"fa_assignment_{index}_end_from")
                with t4:
                    st.time_input("End to", key=f"fa_assignment_{index}_end_to")

                opt1, opt2 = st.columns(2)
                with opt1:
                    if count_key not in st.session_state:
                        try:
                            st.session_state[count_key] = int(float(store.get("count", "1") or "1"))
                        except ValueError:
                            st.session_state[count_key] = 1
                    st.number_input(
                        "Number of Assignments",
                        min_value=1,
                        max_value=MAX_ASSIGNMENTS_PER_GUIDELINE,
                        step=1,
                        key=count_key,
                        help="Create multiple assignments from the same search criteria (batch/bulk).",
                    )
                with opt2:
                    if route_only_key not in st.session_state:
                        st.session_state[route_only_key] = store.get("route_only") == "true"
                    st.checkbox(
                        "No Interline (Route Only)",
                        key=route_only_key,
                        help=(
                            "Stay on the selected route across blocks. Next trip must start where the "
                            "previous ended, within 60 minutes. Ends early when the next gap exceeds 60 minutes."
                        ),
                    )

            _save_guideline_store(index)

            route_sel = st.session_state.get(f"fa_assignment_{index}_route", "")
            start_loc = st.session_state.get(f"fa_assignment_{index}_start_loc", "")
            end_loc = st.session_state.get(f"fa_assignment_{index}_end_loc", "")
            if not route_sel or not start_loc or not end_loc:
                st.warning(f"Assignment #{index}: select route and both locations (or Any).")

    guidelines_list = _build_guidelines_list(
        opts,
        guideline_count,
        scan_order=scan_order,
        tolerance=tolerance,
    )

    with st.container(border=True):
        _step_title(4, "Create output files")
        if st.button("Fill Assignment Numbers", type="primary", key="fa_fill_btn", use_container_width=True):
            data = _workbook_bytes()
            if not data:
                st.error("Upload and load a workbook first.")
                return
            if not guidelines_list:
                st.error("Enable and complete at least one assignment guideline.")
                return
            try:
                updated_bytes, results = fill_assignment_numbers(
                    data, st.session_state.get("fa_sheet_name"), guidelines_list
                )
                st.session_state["fa_workbook_bytes"] = updated_bytes
                st.session_state["fa_workbook_options"] = workbook_options(
                    updated_bytes, st.session_state.get("fa_sheet_name")
                )
                zip_bytes, summaries, combined_bytes = export_reports_zip(
                    updated_bytes, st.session_state.get("fa_sheet_name")
                )
                st.session_state["fa_last_export_summary"] = summaries
                st.session_state["fa_last_export_zip"] = zip_bytes
                st.session_state["fa_last_export_combined"] = combined_bytes
                st.session_state["fa_updated_workbook"] = updated_bytes
                _clear_guidelines_after_fill()
                parts = [f"assignment {item['assignment']} on {item['count']} row(s)" for item in results]
                st.session_state["fa_fill_success"] = (
                    f"Created {len(results)} assignment(s): " + "; ".join(parts) + ". "
                    "You can keep adding from step 3, or start a new RunCut in step 6."
                )
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    fill_success = st.session_state.pop("fa_fill_success", None)
    if fill_success:
        st.success(fill_success)

    updated_bytes = st.session_state.get("fa_updated_workbook")
    zip_bytes = st.session_state.get("fa_last_export_zip")
    combined_bytes = st.session_state.get("fa_last_export_combined")
    if updated_bytes or zip_bytes or combined_bytes:
        d1, d2, d3 = st.columns(3)
        with d1:
            if updated_bytes:
                st.download_button(
                    "Download updated Excel",
                    data=updated_bytes,
                    file_name="RunCut_Assignments_Updated.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
        with d2:
            if zip_bytes:
                st.download_button(
                    "Download Word reports ZIP",
                    data=zip_bytes,
                    file_name="RunCut_Assignment_Documents.zip",
                    mime="application/zip",
                    use_container_width=True,
                )
        with d3:
            if combined_bytes:
                st.download_button(
                    "Download combined Word (Print All)",
                    data=combined_bytes,
                    file_name=COMBINED_DOC_FILENAME,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True,
                )

    summaries = st.session_state.get("fa_last_export_summary")
    if summaries:
        with st.container(border=True):
            _step_title(5, "Export summary")
            st.dataframe(summaries, use_container_width=True, hide_index=True)

    with st.container(border=True):
        _step_title(6, "Start new RunCut")
        st.caption(
            "To upload a different RunCut file, reset here. "
            "Or keep adding assignments from step 3 using the workbook still in session."
        )
        if st.button("Refresh page / upload new RunCut", key="fa_reset_runcut", use_container_width=True):
            _reset_to_new_runcut()
            st.rerun()


def _display_name_for_portal(user: dict) -> str:
    name = str(user.get("username") or user.get("name") or user.get("email") or "User")
    return name.split("@", 1)[0].replace(".", " ").title() if "@" in name else name


def _user_initials(user: dict) -> str:
    name = str(user.get("username") or user.get("name") or user.get("email") or "User")
    if "@" in name:
        name = name.split("@", 1)[0]
    parts = [part for part in name.replace(".", " ").replace("_", " ").split() if part]
    if not parts:
        return "U"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _inject_page_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp:has(.fa-page-marker) {
            background:
                radial-gradient(circle at top right, rgba(11, 107, 203, 0.08), transparent 28%),
                linear-gradient(180deg, #e8f1fa 0%, #f5f9fc 48%, #ffffff 100%);
        }
        .stApp:has(.fa-page-marker) .block-container {
            padding-top: 1.25rem;
            padding-bottom: 2.75rem;
            max-width: 1120px;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] {
            margin-bottom: 0.85rem;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab-list"] {
            gap: 0.35rem;
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #d4e0ec;
            border-radius: 14px;
            padding: 0.4rem;
            box-shadow: 0 4px 18px rgba(15, 45, 75, 0.07);
            backdrop-filter: blur(6px);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab"] {
            height: 2.55rem;
            border-radius: 10px;
            color: #516579;
            font-weight: 650;
            padding: 0 1.05rem;
            transition: all 0.15s ease;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab"]:hover {
            background: #f0f7fd;
            color: #0b6bcb;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [aria-selected="true"] {
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff !important;
            box-shadow: 0 6px 16px rgba(11, 107, 203, 0.28);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
            background-color: transparent !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTabs"] [data-baseweb="tab-panel"] {
            padding-top: 1.15rem;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stRadio"] > div {
            gap: 0.35rem;
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #d4e0ec;
            border-radius: 14px;
            padding: 0.4rem;
            box-shadow: 0 4px 18px rgba(15, 45, 75, 0.07);
            backdrop-filter: blur(6px);
            width: 100%;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stRadio"] label {
            background: transparent;
            border-radius: 10px;
            color: #516579;
            font-weight: 650;
            padding: 0.55rem 1.05rem;
            margin: 0 !important;
            transition: all 0.15s ease;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stRadio"] label:hover {
            background: #f0f7fd;
            color: #0b6bcb;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stRadio"] label[data-checked="true"],
        .stApp:has(.fa-page-marker) div[data-testid="stRadio"] label:has(input:checked) {
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff !important;
            box-shadow: 0 6px 16px rgba(11, 107, 203, 0.28);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(255, 255, 255, 0.96);
            border-color: #d7e3ef !important;
            border-radius: 16px !important;
            box-shadow: 0 10px 28px rgba(18, 52, 86, 0.07);
            padding: 0.95rem 1rem 1rem 1rem;
            transition: box-shadow 0.15s ease, transform 0.15s ease;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            box-shadow: 0 14px 32px rgba(18, 52, 86, 0.09);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stMetric"] {
            background: linear-gradient(180deg, #f8fbff 0%, #ffffff 100%);
            border: 1px solid #dce8f3;
            border-radius: 14px;
            padding: 0.85rem 1rem;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.85), 0 3px 10px rgba(18, 52, 86, 0.05);
        }
        .stApp:has(.fa-page-marker) div[data-testid="stMetric"] label {
            color: #6b7f92;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #173044;
            font-weight: 800;
            font-size: 1.35rem;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stFileUploader"] section {
            border: 1.5px dashed #9ec3e4 !important;
            background: linear-gradient(180deg, #f8fbff 0%, #f3f8fd 100%) !important;
            border-radius: 14px !important;
            min-height: 96px;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stTextInput"] input,
        .stApp:has(.fa-page-marker) div[data-testid="stSelectbox"] > div > div,
        .stApp:has(.fa-page-marker) div[data-testid="stTimeInput"] input {
            border-radius: 10px !important;
            border-color: #d7e3ef !important;
            background: #fbfdff !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stButton"] > button[kind="primary"],
        .stApp:has(.fa-page-marker) div[data-testid="stDownloadButton"] > button[kind="primary"] {
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%) !important;
            border: none !important;
            border-radius: 10px !important;
            font-weight: 700 !important;
            box-shadow: 0 8px 18px rgba(11, 107, 203, 0.24) !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stButton"] > button[kind="primary"]:hover,
        .stApp:has(.fa-page-marker) div[data-testid="stDownloadButton"] > button[kind="primary"]:hover {
            box-shadow: 0 10px 22px rgba(11, 107, 203, 0.32) !important;
        }
        .stApp:has(.fa-page-marker) div[data-testid="stAlert"] {
            border-radius: 12px;
            border-width: 1px;
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro {
            position: relative;
            overflow: hidden;
            background: linear-gradient(120deg, #0657c9 0%, #0877db 52%, #22b8e6 100%);
            border-radius: 16px;
            padding: 1.2rem 1.35rem;
            margin-bottom: 1rem;
            color: #ffffff;
            box-shadow: 0 14px 30px rgba(11, 107, 203, 0.22);
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro:before {
            content: "";
            position: absolute;
            right: -2rem;
            top: -2rem;
            width: 8rem;
            height: 8rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.12);
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro:after {
            content: "";
            position: absolute;
            right: 3.5rem;
            bottom: -2.5rem;
            width: 6rem;
            height: 6rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.08);
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro h3 {
            position: relative;
            margin: 0;
            font-size: 1.22rem;
            font-weight: 800;
            color: #ffffff;
            letter-spacing: -0.01em;
        }
        .stApp:has(.fa-page-marker) .fa-tab-intro p {
            position: relative;
            margin: 0.4rem 0 0 0;
            color: #e8f6ff;
            font-size: 0.93rem;
            line-height: 1.5;
            max-width: 760px;
        }
        .stApp:has(.fa-page-marker) .fa-step-title {
            display: flex;
            align-items: center;
            gap: 0.65rem;
            margin: 0 0 0.85rem 0;
            padding-bottom: 0.65rem;
            border-bottom: 1px solid #edf2f7;
            font-size: 1.02rem;
            font-weight: 700;
            color: #173044;
        }
        .stApp:has(.fa-page-marker) .fa-step-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 1.75rem;
            height: 1.75rem;
            border-radius: 999px;
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff;
            font-size: 0.8rem;
            font-weight: 800;
            flex-shrink: 0;
            box-shadow: 0 4px 10px rgba(11, 107, 203, 0.22);
        }
        .stApp:has(.fa-page-marker) .fa-workflow-strip {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.65rem;
            margin-bottom: 1rem;
        }
        .stApp:has(.fa-page-marker) .fa-workflow-item {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #dbe7f2;
            border-radius: 12px;
            padding: 0.75rem 0.85rem;
            box-shadow: 0 4px 14px rgba(18, 52, 86, 0.05);
        }
        .stApp:has(.fa-page-marker) .fa-workflow-item strong {
            display: block;
            color: #173044;
            font-size: 0.86rem;
            margin-bottom: 0.15rem;
        }
        .stApp:has(.fa-page-marker) .fa-workflow-item span {
            color: #6b7f92;
            font-size: 0.78rem;
            line-height: 1.35;
        }
        .stApp:has(.fa-page-marker) .fa-version-row {
            display: grid;
            grid-template-columns: 2.2fr 1fr 1.2fr;
            gap: 0.75rem;
            align-items: center;
            padding: 0.75rem 0.85rem;
            margin-bottom: 0.55rem;
            background: #f8fbff;
            border: 1px solid #e1ebf4;
            border-radius: 12px;
        }
        .stApp:has(.fa-page-marker) .fa-version-row .fa-version-name {
            color: #173044;
            font-weight: 650;
            font-size: 0.9rem;
        }
        .stApp:has(.fa-page-marker) .fa-version-row .fa-version-meta {
            color: #6b7f92;
            font-size: 0.82rem;
        }
        .stApp:has(.fa-page-marker) .fa-header-user-wrap {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 0.65rem;
            margin-bottom: 0.55rem;
        }
        .stApp:has(.fa-page-marker) .fa-header-avatar {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 2.35rem;
            height: 2.35rem;
            border-radius: 999px;
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff;
            font-size: 0.74rem;
            font-weight: 800;
            box-shadow: 0 4px 14px rgba(11, 107, 203, 0.28);
            border: 2px solid #ffffff;
        }
        .stApp:has(.fa-page-marker) .fa-header-user-text {
            text-align: right;
        }
        .stApp:has(.fa-page-marker) .fa-header-title-row {
            display: flex;
            align-items: center;
            gap: 0.8rem;
        }
        .stApp:has(.fa-page-marker) .fa-header-app-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 2.65rem;
            height: 2.65rem;
            border-radius: 14px;
            background: linear-gradient(135deg, #0b6bcb 0%, #0891c7 100%);
            color: #ffffff;
            font-size: 1.15rem;
            box-shadow: 0 8px 18px rgba(11, 107, 203, 0.24);
            flex-shrink: 0;
        }
        .fa-page-marker,
        .fa-header-marker {
            display: none;
        }
        @media (max-width: 768px) {
            .stApp:has(.fa-page-marker) .fa-workflow-strip {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _workflow_strip() -> None:
    st.markdown(
        """
        <div class="fa-workflow-strip">
            <div class="fa-workflow-item">
                <strong>1. Load workbook</strong>
                <span>Upload RunCut Excel and load route/location dropdowns.</span>
            </div>
            <div class="fa-workflow-item">
                <strong>2. Build assignments</strong>
                <span>Add assignment guidelines to fill blank Asn# rows.</span>
            </div>
            <div class="fa-workflow-item">
                <strong>3. Export reports</strong>
                <span>Generate Word documents and download ZIP or Excel.</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _tab_intro(title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="fa-tab-intro">
            <h3>{html.escape(title)}</h3>
            <p>{html.escape(description)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _step_title(step: int, title: str) -> None:
    st.markdown(
        f"""
        <div class="fa-step-title">
            <span class="fa-step-badge">{step}</span>
            <span>{html.escape(title)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_page_header() -> None:
    from authentication.auth import allowed_portals, logout

    st.markdown(
        """
        <style>
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) {
            background: linear-gradient(135deg, #dceeff 0%, #eaf4fc 38%, #f3f9ff 100%);
            border: 1px solid #c5daf0;
            border-radius: 18px;
            padding: 1.05rem 1.2rem 1rem 1.2rem;
            margin-bottom: 0.85rem;
            box-shadow: 0 12px 30px rgba(11, 107, 203, 0.1);
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-actions) > div[data-testid="stVerticalBlock"] {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 0 !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-actions) div[data-testid="stHorizontalBlock"]:has(.fa-header-btn-switch) {
            gap: 0.5rem !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="stHorizontalBlock"] {
            align-items: center !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) h1 {
            font-size: 1.75rem;
            font-weight: 800;
            margin: 0;
            padding: 0;
            line-height: 1.15;
            color: #173044;
            letter-spacing: -0.02em;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="stCaptionContainer"] p {
            margin-top: 0.35rem;
            color: #6b7f92;
            font-size: 0.92rem;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) .fa-header-user-name {
            font-weight: 700;
            font-size: 0.92rem;
            color: #173044;
            margin: 0;
            line-height: 1.2;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) .fa-header-user-role {
            color: #6b7f92;
            font-size: 0.78rem;
            margin: 0.1rem 0 0 0;
            line-height: 1.2;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="stButton"] {
            margin-bottom: 0 !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button,
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button[kind="secondary"] {
            min-height: 2.15rem;
            height: 2.15rem;
            min-width: 7.1rem;
            padding: 0 0.75rem;
            font-size: 0.78rem;
            font-weight: 700;
            white-space: nowrap !important;
            border-radius: 10px;
            border: 1px solid #93c5fd !important;
            background: linear-gradient(180deg, #ffffff 0%, #eff6ff 100%) !important;
            color: #1d4ed8 !important;
            box-shadow: 0 2px 8px rgba(37, 99, 235, 0.12) !important;
            transition: all 0.15s ease;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button p,
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button p {
            white-space: nowrap !important;
            overflow: visible !important;
            text-overflow: clip !important;
            font-size: 0.78rem !important;
            line-height: 1 !important;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-switch) div[data-testid="stButton"] > button:hover {
            background: linear-gradient(180deg, #dbeafe 0%, #bfdbfe 100%) !important;
            border-color: #60a5fa !important;
            color: #1e40af !important;
            box-shadow: 0 4px 12px rgba(37, 99, 235, 0.18) !important;
            transform: translateY(-1px);
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button,
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button[kind="secondary"] {
            min-height: 2.15rem;
            height: 2.15rem;
            min-width: 5.2rem;
            padding: 0 0.75rem;
            font-size: 0.78rem;
            font-weight: 700;
            white-space: nowrap !important;
            border-radius: 10px;
            border: 1px solid #cbd5e1 !important;
            background: rgba(255, 255, 255, 0.92) !important;
            color: #475569 !important;
            box-shadow: 0 2px 8px rgba(15, 23, 42, 0.05) !important;
            transition: all 0.15s ease;
        }
        div[data-testid="stVerticalBlock"]:has(.fa-header-marker) div[data-testid="column"]:has(.fa-header-btn-logout) div[data-testid="stButton"] > button:hover {
            background: #fff5f5 !important;
            border-color: #fca5a5 !important;
            color: #dc2626 !important;
            box-shadow: 0 4px 12px rgba(220, 38, 38, 0.12) !important;
            transform: translateY(-1px);
        }
        .fa-header-btn-switch,
        .fa-header-btn-logout,
        .fa-header-actions {
            display: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    user = st.session_state.get("user", {})
    display_name = _display_name_for_portal(user)
    initials = _user_initials(user)
    email = str(user.get("email", ""))
    role = str(user.get("role", ""))
    user_portals = allowed_portals(email, role)

    st.markdown('<div class="fa-header-marker"></div>', unsafe_allow_html=True)
    title_col, actions_col = st.columns([5.4, 2.8], vertical_alignment="center")
    with title_col:
        st.markdown(
            """
            <div class="fa-header-title-row">
                <span class="fa-header-app-icon">▤</span>
                <div>
                    <h1 style="margin:0;padding:0;font-size:1.75rem;font-weight:800;color:#173044;letter-spacing:-0.02em;">
                        Survey Assignment Manager
                    </h1>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.caption("Upload a RunCut workbook, assign surveyors, and export Word assignment reports.")
    with actions_col:
        st.markdown('<div class="fa-header-actions"></div>', unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class="fa-header-user-wrap">
                <div class="fa-header-user-text">
                    <p class="fa-header-user-name">{html.escape(display_name)}</p>
                    <p class="fa-header-user-role">Administrator</p>
                </div>
                <span class="fa-header-avatar">{html.escape(initials)}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        switch_col, logout_col = st.columns([1.35, 1], gap="small")
        with switch_col:
            st.markdown('<div class="fa-header-btn-switch"></div>', unsafe_allow_html=True)
            if len(user_portals) > 1:
                if st.button("Switch Portal", type="secondary"):
                    st.query_params["page"] = "portal_select"
                    st.rerun()
            elif "od" in user_portals and st.button("OD Dashboard", type="secondary"):
                st.query_params["page"] = "od_project_select"
                st.rerun()
        with logout_col:
            st.markdown('<div class="fa-header-btn-logout"></div>', unsafe_allow_html=True)
            if st.button("Logout", type="secondary"):
                logout()


def _render_export_tab() -> None:
    _tab_intro(
        "Create Reports",
        "Upload a workbook with assignment numbers already filled, then export Word documents.",
    )

    with st.container(border=True):
        _step_title(1, "Upload workbook")
        _render_export_upload_section()

    with st.container(border=True):
        _step_title(2, "Generate documents")
        _render_export_generate_section()

    summaries = st.session_state.get("fa_last_export_summary")
    if summaries:
        with st.container(border=True):
            _step_title(3, "Export summary")
            st.dataframe(summaries, use_container_width=True, hide_index=True)


@st.fragment
def _render_export_upload_section() -> None:
    _render_header_template_download("fa_export_template")
    uploaded = st.file_uploader("RunCut Excel file (.xlsx)", type=["xlsx"], key="fa_export_upload")
    sheet_name = st.text_input("Sheet name (blank = active sheet)", key="fa_export_sheet")
    st.caption("Choose a file, then click **Load Workbook** to parse it.")

    if st.button("Load Workbook", key="fa_export_load", type="primary", use_container_width=True):
        if uploaded is None:
            st.error("Choose an .xlsx workbook first.")
            return
        try:
            _set_workbook(uploaded, sheet_name or None)
            st.session_state["fa_rules_loaded"] = True
            st.success(f"Loaded {uploaded.name}")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    data = _workbook_bytes()
    if data:
        opts = st.session_state.get("fa_workbook_options", {})
        c1, c2, c3 = st.columns(3)
        c1.metric("Sheet", str(opts.get("sheet", "")))
        c2.metric("Max Asn#", int(opts.get("max_assignment", 0)))
        c3.metric("Blank rows", int(opts.get("blank_rows", 0)))


@st.fragment
def _render_export_generate_section() -> None:
    assignment_filter = st.text_input(
        "Assignments (optional)",
        placeholder="All, or enter 1,2,9",
        key="fa_export_filter",
        help="Leave blank to export every assignment with a nonblank Asn#.",
    )

    if st.button("Generate Word Documents", type="primary", key="fa_export_btn", use_container_width=True):
        data = _workbook_bytes()
        if not data:
            st.error("Upload a RunCut workbook and click **Load Workbook** first.")
            return
        try:
            wanted = parse_assignment_filter(assignment_filter)
            zip_bytes, summaries, combined_bytes = export_reports_zip(
                data, st.session_state.get("fa_sheet_name"), wanted
            )
            st.session_state["fa_last_export_summary"] = summaries
            st.session_state["fa_last_export_zip"] = zip_bytes
            st.session_state["fa_last_export_combined"] = combined_bytes
            st.success(f"Exported {len(summaries)} Word document(s) plus a combined Print All file.")
        except Exception as exc:
            st.error(str(exc))

    zip_bytes = st.session_state.get("fa_last_export_zip")
    combined_bytes = st.session_state.get("fa_last_export_combined")
    if zip_bytes or combined_bytes:
        d1, d2 = st.columns(2)
        with d1:
            if zip_bytes:
                st.download_button(
                    "Download ZIP",
                    data=zip_bytes,
                    file_name="RunCut_Assignment_Documents.zip",
                    mime="application/zip",
                    type="primary",
                    use_container_width=True,
                )
        with d2:
            if combined_bytes:
                st.download_button(
                    "Download combined Word (Print All)",
                    data=combined_bytes,
                    file_name=COMBINED_DOC_FILENAME,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True,
                )


def _render_assign_tab() -> None:
    _ensure_assignment_guideline_count()
    _tab_intro(
        "Create Assignments",
        "Load route dropdowns, define assignment guidelines, and fill blank Asn# rows in the workbook.",
    )

    with st.container(border=True):
        _step_title(1, "Load workbook")
        _render_assign_upload_section()

    if not st.session_state.get("fa_rules_loaded"):
        st.info("Upload a workbook and click **Load Assignment Dropdowns** to begin.")
        return

    _render_assign_guidelines_and_output()


@st.fragment
def _render_assign_upload_section() -> None:
    _render_header_template_download("fa_assign_template")
    uploaded = st.file_uploader("RunCut Excel file (.xlsx)", type=["xlsx"], key="fa_assign_upload")
    sheet_name = st.text_input("Sheet name (blank = active sheet)", key="fa_assign_sheet")
    st.caption("Choose a file, then click **Load Assignment Dropdowns**.")

    if st.button("Load Assignment Dropdowns", key="fa_load_dropdowns", type="primary", use_container_width=True):
        if uploaded is None:
            st.error("Choose an .xlsx workbook first.")
            return
        try:
            _set_workbook(uploaded, sheet_name or None)
            st.session_state["fa_rules_loaded"] = True
            st.success("Dropdowns loaded.")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    data = _workbook_bytes()
    if data and st.session_state.get("fa_rules_loaded"):
        opts = st.session_state.get("fa_workbook_options", {})
        c1, c2, c3 = st.columns(3)
        c1.metric("Blank Asn# rows", int(opts.get("blank_rows", 0)))
        c2.metric("Next Asn#", int(opts.get("next_assignment", 0)))
        c3.metric("Sheet", str(opts.get("sheet", "")))


def _ensure_tod_ranges() -> None:
    if "fa_tod_ranges" not in st.session_state:
        st.session_state["fa_tod_ranges"] = default_tod_ranges()


def _tod_count() -> int:
    _ensure_tod_ranges()
    return len(st.session_state["fa_tod_ranges"])


def _add_tod_range() -> None:
    _ensure_tod_ranges()
    ranges = st.session_state["fa_tod_ranges"]
    if not ranges:
        st.session_state["fa_tod_ranges"] = default_tod_ranges()
        return
    # Split the last range: previous end becomes midpoint; new range continues to 23:59:59.
    from datetime import datetime, timedelta

    last = ranges[-1]
    start = datetime.strptime(last.get("start", "00:00:00"), "%H:%M:%S")
    end = datetime.strptime("23:59:59", "%H:%M:%S")
    # If last already ends at end of day, set previous end to noon-ish mid or keep user-editable.
    mid = start + (end - start) / 2
    mid = mid.replace(second=0, microsecond=0)
    mid_end = mid
    new_start = mid_end + timedelta(seconds=1)
    last["end"] = mid_end.strftime("%H:%M:%S")
    last["label"] = last.get("label") or f"TOD {len(ranges)}"
    ranges.append(
        {
            "label": f"TOD {len(ranges) + 1}",
            "start": new_start.strftime("%H:%M:%S"),
            "end": "23:59:59",
        }
    )
    # Relabel sequentially
    for i, item in enumerate(ranges, start=1):
        if not item.get("label") or item["label"].startswith("TOD ") or item["label"] == "All Day":
            item["label"] = f"TOD {i}" if len(ranges) > 1 else "All Day"
    st.session_state["fa_tod_ranges"] = ranges


def _remove_tod_range(index: int) -> None:
    _ensure_tod_ranges()
    ranges = st.session_state["fa_tod_ranges"]
    if len(ranges) <= 1 or index < 0 or index >= len(ranges):
        return
    ranges.pop(index)
    # Chain times: first starts 00:00:00, last ends 23:59:59, middles link.
    from datetime import datetime, timedelta

    for i, item in enumerate(ranges):
        if i == 0:
            item["start"] = "00:00:00"
        else:
            prev_end = datetime.strptime(ranges[i - 1]["end"], "%H:%M:%S")
            item["start"] = (prev_end + timedelta(seconds=1)).strftime("%H:%M:%S")
        if i == len(ranges) - 1:
            item["end"] = "23:59:59"
        item["label"] = f"TOD {i + 1}" if len(ranges) > 1 else "All Day"
    st.session_state["fa_tod_ranges"] = ranges


def _sync_tod_from_widgets() -> list[dict[str, str]]:
    """Read TOD widget values and keep ranges contiguous with no gaps."""
    from datetime import datetime, time, timedelta

    _ensure_tod_ranges()
    ranges = st.session_state["fa_tod_ranges"]
    updated: list[dict[str, str]] = []
    for i in range(len(ranges)):
        label = str(st.session_state.get(f"fa_tod_label_{i}", ranges[i].get("label", f"TOD {i + 1}")))
        start_t = st.session_state.get(f"fa_tod_start_{i}")
        end_t = st.session_state.get(f"fa_tod_end_{i}")
        if isinstance(start_t, time):
            start_s = start_t.strftime("%H:%M:%S")
        else:
            start_s = ranges[i].get("start", "00:00:00")
        if isinstance(end_t, time):
            end_s = end_t.strftime("%H:%M:%S")
        else:
            end_s = ranges[i].get("end", "23:59:59")
        updated.append({"label": label or f"TOD {i + 1}", "start": start_s, "end": end_s})

    if not updated:
        return default_tod_ranges()

    # Enforce chain: first start 00:00:00, each next start = prev end + 1s, last end 23:59:59.
    updated[0]["start"] = "00:00:00"
    for i in range(len(updated)):
        if i > 0:
            prev_end = datetime.strptime(updated[i - 1]["end"][:8], "%H:%M:%S")
            updated[i]["start"] = (prev_end + timedelta(seconds=1)).strftime("%H:%M:%S")
        if i == len(updated) - 1:
            updated[i]["end"] = "23:59:59"
    st.session_state["fa_tod_ranges"] = updated
    return updated


def _render_summary_tab() -> None:
    from datetime import datetime, time

    _tab_intro(
        "Summary & Versions",
        "Review route coverage by direction and time of day, and manage saved workbook versions.",
    )
    _ensure_tod_ranges()
    data = _workbook_bytes()

    with st.container(border=True):
        _step_title(1, "Time of Day Assignment")
        st.caption(
            "Define TOD ranges used by coverage views. "
            "New ranges start one second after the previous end; the last range always ends at 11:59:59 PM."
        )
        tod_ranges = st.session_state["fa_tod_ranges"]
        add_col, info_col = st.columns([1, 3])
        with add_col:
            if st.button("Add TOD", key="fa_add_tod", use_container_width=True):
                _sync_tod_from_widgets()
                _add_tod_range()
                st.rerun()
        with info_col:
            st.caption(f"{len(tod_ranges)} Time of Day range(s).")

        for i, tod in enumerate(tod_ranges):
            row = st.columns([2, 2, 2, 1], vertical_alignment="bottom")
            with row[0]:
                st.text_input("Label", value=tod.get("label", f"TOD {i + 1}"), key=f"fa_tod_label_{i}")
            with row[1]:
                try:
                    start_default = datetime.strptime(tod.get("start", "00:00:00")[:8], "%H:%M:%S").time()
                except ValueError:
                    start_default = time(0, 0, 0)
                st.time_input(
                    "Start",
                    value=start_default,
                    key=f"fa_tod_start_{i}",
                    disabled=i > 0,
                    help="First TOD starts at 12:00 AM. Later TODs start right after the previous end.",
                )
            with row[2]:
                try:
                    end_default = datetime.strptime(tod.get("end", "23:59:59")[:8], "%H:%M:%S").time()
                except ValueError:
                    end_default = time(23, 59, 59)
                st.time_input(
                    "End",
                    value=end_default,
                    key=f"fa_tod_end_{i}",
                    disabled=i == len(tod_ranges) - 1,
                    help="Last TOD always ends at 11:59:59 PM.",
                )
            with row[3]:
                if len(tod_ranges) > 1 and st.button("Remove", key=f"fa_remove_tod_{i}", use_container_width=True):
                    _sync_tod_from_widgets()
                    _remove_tod_range(i)
                    st.rerun()

    with st.container(border=True):
        _step_title(2, "Route coverage")
        view_key = st.selectbox(
            "Coverage view",
            options=list(COVERAGE_VIEWS.keys()),
            format_func=lambda key: COVERAGE_VIEWS[key],
            key="fa_coverage_view",
        )
        refresh = st.button(
            "Generate / refresh table",
            key="fa_refresh_coverage",
            type="primary",
            use_container_width=True,
        )
        if refresh or st.session_state.get("fa_coverage_ready"):
            st.session_state["fa_coverage_ready"] = True
            if data:
                try:
                    tod_ranges = _sync_tod_from_widgets()
                    df = coverage_summary(
                        data,
                        st.session_state.get("fa_sheet_name"),
                        group_by=view_key,
                        tod_ranges=tod_ranges,
                    )
                    if df.empty:
                        st.info("No route rows found in the workbook.")
                    else:
                        st.dataframe(style_coverage_dataframe(df), use_container_width=True, hide_index=True)
                except Exception as exc:
                    st.error(str(exc))
            else:
                st.info("Upload a workbook on the Export or Assign tab to see route coverage.")
        elif not data:
            st.info("Upload a workbook on the Export or Assign tab to see route coverage.")
        else:
            st.caption("Click **Generate / refresh table** after setting Time of Day ranges.")

    with st.container(border=True):
        _step_title(3, "Start Location & Routes")
        if data:
            try:
                location_df = start_location_routes_summary(data, st.session_state.get("fa_sheet_name"))
                if location_df.empty:
                    st.info("No start locations found in the workbook.")
                else:
                    st.dataframe(
                        style_start_location_dataframe(location_df),
                        use_container_width=True,
                        hide_index=True,
                    )
            except Exception as exc:
                st.error(str(exc))
        else:
            st.info("Upload a workbook on the Export or Assign tab to see start location route coverage.")

    with st.container(border=True):
        _step_title(4, "Version history")
        label = st.text_input(
            "Project / city label for S3 versions",
            value=st.session_state.get("selected_project", "general"),
            key="fa_version_label",
        )

        if data and st.button(
            "Save current workbook version to S3",
            key="fa_save_version",
            type="primary",
            use_container_width=True,
        ):
            user = st.session_state.get("user", {})
            meta = save_workbook_version(
                data,
                label=label,
                original_filename=st.session_state.get("fa_workbook_name", "runcut.xlsx"),
                uploaded_by=str(user.get("email", "")),
                sheet_name=st.session_state.get("fa_sheet_name"),
            )
            if meta:
                st.success(f"Saved version (max Asn# {meta.get('max_asn')}) at {meta.get('uploaded_at')}.")
            else:
                st.warning("S3 bucket not configured (`bucket_name` in .env). Version not saved.")

        versions = list_workbook_versions(label)
        if not versions:
            st.caption("No saved versions yet for this label.")
            return

        for idx, version in enumerate(versions):
            row_col, load_col = st.columns([5, 1])
            with row_col:
                st.markdown(
                    f"""
                    <div class="fa-version-row">
                        <div>
                            <div class="fa-version-name">{html.escape(str(version.get("original_filename", "workbook")))}</div>
                            <div class="fa-version-meta">Max Asn#: {html.escape(str(version.get("max_asn", "?")))}</div>
                        </div>
                        <div class="fa-version-meta">{html.escape(str(version.get("uploaded_at", "")))}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with load_col:
                if st.button("Load", key=f"fa_load_version_{idx}", use_container_width=True):
                    loaded = load_workbook_version(version.get("xlsx_key", ""))
                    if loaded:
                        st.session_state["fa_workbook_bytes"] = loaded
                        st.session_state["fa_workbook_name"] = version.get("original_filename", "runcut.xlsx")
                        st.session_state["fa_sheet_name"] = version.get("sheet")
                        st.session_state["fa_workbook_options"] = workbook_options(
                            loaded, st.session_state.get("fa_sheet_name")
                        )
                        st.session_state["fa_rules_loaded"] = True
                        st.success("Version loaded into session.")
                        st.rerun()
                    else:
                        st.error("Could not load that version from S3.")


def _workbook_status_banner() -> None:
    data = _workbook_bytes()
    opts = st.session_state.get("fa_workbook_options") or {}
    with st.container(border=True):
        st.markdown("**Current workbook in session**")
        c1, c2, c3, c4 = st.columns(4)
        if data:
            c1.metric("File", str(st.session_state.get("fa_workbook_name", "Loaded")))
            c2.metric("Sheet", str(opts.get("sheet", "—")))
            c3.metric("Max Asn#", int(opts.get("max_assignment", 0)))
            c4.metric("Blank rows", int(opts.get("blank_rows", 0)))
        else:
            c1.metric("File", "None loaded")
            c2.metric("Sheet", "—")
            c3.metric("Max Asn#", 0)
            c4.metric("Blank rows", "—")


def _render_tab_navigation() -> str:
    if "fa_active_tab" not in st.session_state:
        st.session_state["fa_active_tab"] = "export"
    st.radio(
        "Sections",
        options=list(FA_TABS.keys()),
        format_func=lambda key: FA_TABS[key],
        horizontal=True,
        key="fa_active_tab",
        label_visibility="collapsed",
    )
    return str(st.session_state["fa_active_tab"])


def render_field_assignments_page() -> None:
    """Main entry for ?page=field_assignments."""
    _inject_page_styles()
    st.markdown('<div class="fa-page-marker"></div>', unsafe_allow_html=True)
    _render_page_header()
    _workflow_strip()
    _workbook_status_banner()

    active_tab = _render_tab_navigation()
    if active_tab == "export":
        _render_export_tab()
    elif active_tab == "assign":
        _render_assign_tab()
    else:
        _render_summary_tab()
