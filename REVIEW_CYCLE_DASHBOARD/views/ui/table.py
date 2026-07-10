"""Reference-style data tables (AgGrid, presentation only)."""

from __future__ import annotations

import hashlib

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, DataReturnMode, GridOptionsBuilder, GridUpdateMode

from views.ui.components import empty_state, section_header_compact, table_section_footer

REF_AGGRID_CSS = {
    ".ag-root-wrapper": {
        "border": "0 !important",
        "border-radius": "0 !important",
        "font-family": "Inter, ui-sans-serif, system-ui, sans-serif !important",
    },
    ".ag-header": {
        "background-color": "#F1F5F9 !important",
        "border-bottom": "1px solid #E2E8F0 !important",
    },
    ".ag-header-cell": {
        "color": "#475569 !important",
        "font-size": "10px !important",
        "font-weight": "700 !important",
        "letter-spacing": "0.08em !important",
        "text-transform": "uppercase !important",
    },
    ".ag-row": {
        "border-bottom": "1px solid #F1F5F9 !important",
        "font-size": "13px !important",
        "color": "#0F172A !important",
        "transition": "background-color 160ms ease !important",
    },
    ".ag-row-odd": {"background-color": "#F8FAFC !important"},
    ".ag-row-even": {"background-color": "#FFFFFF !important"},
    ".ag-row-hover": {"background-color": "#DBEAFE !important"},
    ".ag-cell": {
        "display": "flex !important",
        "align-items": "center !important",
        "padding-left": "16px !important",
        "padding-right": "16px !important",
    },
    ".ref-ag-project-cell": {
        "font-weight": "600 !important",
        "padding-left": "40px !important",
        "background-repeat": "no-repeat !important",
        "background-position": "16px center !important",
        "background-size": "14px 14px !important",
        "background-image": (
            "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' "
            "width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%2364748B' "
            "stroke-width='2'%3E%3Cpath d='M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z'/%3E"
            "%3Cpolyline points='14 2 14 8 20 8'/%3E%3C/svg%3E\") !important"
        ),
    },
    ".ag-paging-panel": {
        "border-top": "1px solid #E2E8F0 !important",
        "color": "#64748B !important",
        "font-size": "12px !important",
        "height": "44px !important",
        "padding": "0 16px !important",
        "justify-content": "flex-end !important",
        "background-color": "#FFFFFF !important",
    },
    ".ag-body-horizontal-scroll-viewport::-webkit-scrollbar, "
    ".ag-body-vertical-scroll-viewport::-webkit-scrollbar": {
        "width": "6px !important",
        "height": "6px !important",
    },
    ".ag-body-horizontal-scroll-viewport::-webkit-scrollbar-thumb, "
    ".ag-body-vertical-scroll-viewport::-webkit-scrollbar-thumb": {
        "background": "#CBD5E1 !important",
        "border-radius": "999px !important",
    },
}


def _table_key(base: str, df: pd.DataFrame) -> str:
    if df.empty:
        return f"{base}_empty"
    digest = hashlib.md5(
        f"{base}_{len(df)}_{'|'.join(df.columns.astype(str))}".encode(),
        usedforsecurity=False,
    ).hexdigest()[:10]
    return f"{base}_{digest}"


def _filter_dataframe(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if not query or not query.strip():
        return df
    needle = query.strip().lower()
    mask = df.astype(str).apply(
        lambda row: any(needle in cell.lower() for cell in row),
        axis=1,
    )
    return df[mask]


def _render_toolbar(
    df: pd.DataFrame,
    *,
    key: str,
    section_title: str | None,
    search_placeholder: str,
    export_name: str | None,
) -> str:
    """Toolbar row: section title (left) + search + filter (right)."""
    st.markdown('<span class="ref-table-toolbar-marker" hidden></span>', unsafe_allow_html=True)
    if section_title:
        cols = st.columns([2.4, 3.2, 1], vertical_alignment="center")
        with cols[0]:
            section_header_compact(section_title)
        search_col, filter_col = cols[1], cols[2]
    else:
        cols = st.columns([5, 1], vertical_alignment="center")
        search_col, filter_col = cols[0], cols[1]

    with search_col:
        query = st.text_input(
            "Search table",
            placeholder=search_placeholder,
            label_visibility="collapsed",
            key=f"{key}_search",
        )
    with filter_col:
        filters_on = bool(st.session_state.get(f"{key}_filters_on", False))
        label = "Filters ✓" if filters_on else "Filters"
        if st.button(label, key=f"{key}_filter_toggle", use_container_width=True):
            st.session_state[f"{key}_filters_on"] = not filters_on
            st.rerun()
    return query or ""


def render_table(
    df: pd.DataFrame,
    *,
    key: str = "ref_table",
    height: int | None = None,
    section_title: str | None = None,
    search_placeholder: str = "Search...",
    show_toolbar: bool = True,
    export_name: str | None = None,
    entity_label: str = "rows",
    page_size: int = 20,
    empty_title: str = "No data",
    empty_detail: str = "There are no records to display.",
) -> None:
    """Render a read-only reference-style data table. Data stays plain text only."""
    if df is None or df.empty:
        empty_state(empty_title, empty_detail)
        return

    query = ""
    if show_toolbar:
        query = _render_toolbar(
            df,
            key=key,
            section_title=section_title,
            search_placeholder=search_placeholder,
            export_name=export_name,
        )

    view = _filter_dataframe(df, query)
    if view.empty:
        empty_state("No matches", f'No rows match "{query}".')
        return

    gb = GridOptionsBuilder.from_dataframe(view)
    gb.configure_default_column(
        filterable=True,
        sortable=True,
        resizable=True,
        minWidth=100,
        wrapText=False,
    )

    project_col = next((c for c in view.columns if str(c).lower() == "project"), None)
    if project_col:
        gb.configure_column(project_col, cellClass="ref-ag-project-cell")

    gb.configure_grid_options(
        suppressRowClickSelection=True,
        rowHeight=48,
        headerHeight=44,
        animateRows=False,
        enableCellTextSelection=True,
        tooltipShowDelay=0,
    )
    gb.configure_pagination(
        enabled=True,
        paginationPageSize=page_size,
        paginationAutoPageSize=False,
    )

    row_count = len(view)
    grid_height = height or min(max(row_count * 48 + 104, 320), 560)

    with st.container(border=True):
        AgGrid(
            view,
            gridOptions=gb.build(),
            height=grid_height,
            theme="alpine",
            custom_css=REF_AGGRID_CSS,
            allow_unsafe_jscode=False,
            fit_columns_on_grid_load=True,
            use_container_width=True,
            key=_table_key(key, view),
            update_mode=GridUpdateMode.NO_UPDATE,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            reload_data=False,
        )

    if show_toolbar:
        end = min(page_size, row_count)
        st.caption(f"Showing 1 to {end} of {row_count} {entity_label}")


def _build_reference_grid(
    df: pd.DataFrame,
    *,
    floating_filter: bool = False,
) -> tuple[dict, int]:
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filterable=True,
        sortable=True,
        resizable=True,
        minWidth=100,
        wrapText=False,
        floatingFilter=floating_filter,
    )

    project_col = next((c for c in df.columns if str(c).lower() == "project"), None)
    if project_col:
        gb.configure_column(project_col, cellClass="ref-ag-project-cell")

    numeric_cols = [c for c in df.columns if c != project_col and pd.api.types.is_numeric_dtype(df[c])]
    for col in numeric_cols:
        gb.configure_column(col, type=["numericColumn"], cellStyle={"textAlign": "right"})

    gb.configure_grid_options(
        suppressRowClickSelection=True,
        rowHeight=48,
        headerHeight=44,
        animateRows=False,
        enableCellTextSelection=True,
        tooltipShowDelay=0,
    )
    gb.configure_pagination(enabled=False)

    row_count = len(df)
    grid_height = min(max(row_count * 48 + 56, 200), 560)
    return gb.build(), grid_height


def render_reference_table(
    df: pd.DataFrame,
    *,
    key: str = "ref_table",
    title: str = "Portfolio Table",
    entity_label: str = "projects",
    empty_title: str = "No data",
    empty_detail: str = "There are no records to display.",
    export_name: str | None = None,
) -> None:
    """Reference shell: toolbar + AgGrid body + footer (search/filter are UI-only)."""
    if df is None or df.empty:
        empty_state(empty_title, empty_detail)
        return

    total_count = len(df)
    query = _render_toolbar(
        df,
        key=key,
        section_title=title,
        search_placeholder="Search projects...",
        export_name=export_name or f"{key}.csv",
    )
    floating_filter = bool(st.session_state.get(f"{key}_filters_on", False))

    view = _filter_dataframe(df, query)
    if view.empty:
        empty_state("No matches", f'No rows match "{query or ""}".')
        return

    grid_options, grid_height = _build_reference_grid(view, floating_filter=floating_filter)

    AgGrid(
        view,
        gridOptions=grid_options,
        height=grid_height,
        theme="alpine",
        custom_css=REF_AGGRID_CSS,
        allow_unsafe_jscode=False,
        fit_columns_on_grid_load=True,
        use_container_width=True,
        key=_table_key(key, view),
        update_mode=GridUpdateMode.NO_UPDATE,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        reload_data=False,
    )
    table_section_footer(shown=len(view), total=total_count, label=entity_label)
