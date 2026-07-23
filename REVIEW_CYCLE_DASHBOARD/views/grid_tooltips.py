"""Cell-level decision-history helpers for editable grids.

History text is attached as hidden ``__tip_*`` columns, then shown on cell hover
via AgGrid ``tooltipField`` (browser tooltips). Edits still return a normal frame.
"""

from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, DataReturnMode, GridOptionsBuilder, GridUpdateMode, JsCode
from st_aggrid.shared import ColumnsAutoSizeMode

from services import history as history_svc

TIP_PREFIX = "__tip_"

# Keep columns readable; horizontal scroll appears when total width > viewport.
_DEFAULT_COL_WIDTH = 150
_GRID_HEIGHT = 460

_AGGRID_CUSTOM_CSS = {
    # streamlit-aggrid 0.3.x reserves 30px for an empty toolbar without
    # subtracting it from the grid height. That clips the bottom scrollbar.
    "#gridToolBar": {
        "display": "none !important",
        "height": "0 !important",
        "min-height": "0 !important",
        "padding": "0 !important",
    },
    ".ag-root-wrapper": {
        "border": "1px solid #d0d7de",
        "border-radius": "8px",
        "font-family": "Segoe UI, system-ui, sans-serif",
    },
    ".ag-header": {
        "background-color": "#f3f6fa",
        "border-bottom": "1px solid #d0d7de",
    },
    ".ag-header-cell": {
        "font-weight": "600",
        "font-size": "12px",
        "color": "#1f2937",
    },
    ".ag-row": {
        "border-bottom": "1px solid #eef2f6",
        "font-size": "13px",
    },
    ".ag-row-even": {"background-color": "#ffffff"},
    ".ag-row-odd": {"background-color": "#f8fafc"},
    ".ag-row-hover": {"background-color": "#eef6ff !important"},
    ".ag-cell": {
        "line-height": "36px",
    },
    # Critical: allow the grid body to scroll horizontally.
    ".ag-body-viewport": {
        "overflow-x": "auto !important",
        "overflow-y": "auto !important",
    },
    ".ag-center-cols-viewport": {
        "overflow-x": "auto !important",
    },
    ".ag-body-horizontal-scroll": {
        "height": "20px !important",
        "min-height": "20px !important",
        "max-height": "20px !important",
        "opacity": "1 !important",
        "visibility": "visible !important",
        "display": "flex !important",
        "background": "#e2e8f0 !important",
        "border-top": "1px solid #cbd5e1 !important",
    },
    ".ag-body-horizontal-scroll-viewport": {
        "overflow-x": "scroll !important",
        "height": "20px !important",
        "min-height": "20px !important",
    },
    ".ag-body-horizontal-scroll-container": {
        "height": "20px !important",
        "min-height": "20px !important",
    },
    ".ag-body-vertical-scroll": {
        "width": "14px !important",
        "opacity": "1 !important",
        "background": "#dbe3ec !important",
    },
    ".ag-body-horizontal-scroll-viewport::-webkit-scrollbar": {
        "height": "18px",
    },
    ".ag-body-horizontal-scroll-viewport::-webkit-scrollbar-button:horizontal:decrement:start": {
        "display": "block",
        "width": "20px",
        "background-color": "#e2e8f0",
        "background-repeat": "no-repeat",
        "background-position": "center",
        "background-image": (
            "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' "
            "width='18' height='18' viewBox='0 0 18 18'%3E%3Cpath "
            "d='M11 4L6 9l5 5' fill='none' stroke='%23334155' "
            "stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E\")"
        ),
    },
    ".ag-body-horizontal-scroll-viewport::-webkit-scrollbar-button:horizontal:increment:end": {
        "display": "block",
        "width": "20px",
        "background-color": "#e2e8f0",
        "background-repeat": "no-repeat",
        "background-position": "center",
        "background-image": (
            "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' "
            "width='18' height='18' viewBox='0 0 18 18'%3E%3Cpath "
            "d='M7 4l5 5-5 5' fill='none' stroke='%23334155' "
            "stroke-width='2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E\")"
        ),
    },
    ".ag-body-viewport::-webkit-scrollbar": {
        "width": "12px",
        "height": "18px",
    },
    ".ag-body-horizontal-scroll-viewport::-webkit-scrollbar-track,"
    " .ag-body-viewport::-webkit-scrollbar-track": {
        "background": "#e2e8f0",
    },
    ".ag-body-horizontal-scroll-viewport::-webkit-scrollbar-thumb,"
    " .ag-body-viewport::-webkit-scrollbar-thumb": {
        "background": "#4b5563",
        "border-radius": "8px",
        "border": "3px solid #e2e8f0",
        "min-width": "48px",
    },
}

# After paint: never size-to-fit; keep fixed widths so left/right scroll works.
_ON_FIRST_DATA_RENDERED = JsCode(
    """
    function(params) {
        // Intentionally do NOT call sizeColumnsToFit().
        // Make horizontal scrollbar usable even when the theme hides it.
        var h = document.querySelector('.ag-body-horizontal-scroll');
        if (h) {
            h.style.display = 'flex';
            h.style.height = '20px';
            h.style.minHeight = '20px';
            h.style.opacity = '1';
            h.style.visibility = 'visible';
        }
        var hv = document.querySelector('.ag-body-horizontal-scroll-viewport');
        if (hv) {
            hv.style.overflowX = 'scroll';
            hv.style.height = '20px';
            hv.style.minHeight = '20px';
        }
        var hc = document.querySelector('.ag-body-horizontal-scroll-container');
        if (hc) {
            hc.style.height = '20px';
            hc.style.minHeight = '20px';
        }
        var cv = document.querySelector('.ag-center-cols-viewport');
        if (cv) {
            cv.style.overflowX = 'auto';
        }
    }
    """
)


def tooltip_column_name(field: str) -> str:
    return f"{TIP_PREFIX}{field}"


def tip_revision_key(editor_key: str) -> str:
    return f"{editor_key}__tip_rev"


def bump_tip_revision(editor_key: str) -> int:
    """Force AgGrid remount so hover tips pick up freshly saved history."""
    key = tip_revision_key(editor_key)
    nxt = int(st.session_state.get(key, 0)) + 1
    st.session_state[key] = nxt
    return nxt


def resolve_history_project(records: pd.DataFrame | None, project_name: str | None = None) -> str | None:
    """Pick one project for history lookup, or None when rows span multiple projects."""
    if project_name:
        return project_name
    if records is None or records.empty or "PROJECT_NAME" not in records.columns:
        return None
    projects = records["PROJECT_NAME"].dropna().astype(str).unique().tolist()
    if len(projects) == 1:
        return projects[0]
    return None


def hydrate_empty_cells_from_history(
    display: pd.DataFrame,
    id_col: str,
    fields: Iterable[str],
    *,
    project_name: str | None = None,
    records: pd.DataFrame | None = None,
    actor_roles: list[str] | None = None,
) -> pd.DataFrame:
    """Fill blank editable cells from the latest DECISION_HISTORY values.

    History tooltips already show the saved value; this keeps the visible cell
    in sync when a stale records cache still has blanks.
    """
    fields = [field for field in fields if field in display.columns]
    if not fields or id_col not in display.columns or display.empty:
        return display

    single_project = resolve_history_project(records, project_name)
    if not single_project:
        return display

    ids = display[id_col].astype(str).tolist()
    latest = history_svc.load_latest_field_values(
        single_project,
        ids,
        fields,
        actor_roles=actor_roles,
    )
    if not latest:
        return display

    out = display.copy()
    for idx, row in out.iterrows():
        rid = str(row[id_col]).strip()
        values = latest.get(rid) or latest.get(rid[:-2] if rid.endswith(".0") else f"{rid}.0") or {}
        if not values:
            continue
        for field in fields:
            current = row.get(field)
            current_text = (
                ""
                if current is None or (isinstance(current, float) and pd.isna(current))
                else str(current).strip()
            )
            if current_text and current_text.lower() not in {"nan", "none", "<na>"}:
                continue
            new_text = values.get(field)
            if new_text:
                out.at[idx, field] = new_text
    return out


def attach_field_tooltips(
    display: pd.DataFrame,
    id_col: str,
    fields: Iterable[str],
    *,
    project_name: str | None = None,
    records: pd.DataFrame | None = None,
    actor_roles: list[str] | None = None,
    empty_message: str = "No decision history yet.",
) -> pd.DataFrame:
    """Attach hidden per-field tooltip columns for AgGrid cell hover."""
    fields = [field for field in fields if field in display.columns]
    if not fields or id_col not in display.columns:
        return display

    display = hydrate_empty_cells_from_history(
        display,
        id_col,
        fields,
        project_name=project_name,
        records=records,
        actor_roles=actor_roles,
    )

    single_project = resolve_history_project(records, project_name)
    if single_project:
        ids = display[id_col].astype(str).tolist()
        by_record = history_svc.load_field_history_for_records(
            single_project,
            ids,
            fields,
            actor_roles=actor_roles,
            empty_message=empty_message,
        )
        out = display.copy()
        id_series = display[id_col].astype(str)

        def _tip_for(rid: str, f: str) -> str:
            text = by_record.get(str(rid), {}).get(f)
            if text:
                return text
            if str(rid).endswith(".0"):
                text = by_record.get(str(rid)[:-2], {}).get(f)
                if text:
                    return text
            elif str(rid).isdigit():
                text = by_record.get(f"{rid}.0", {}).get(f)
                if text:
                    return text
            return empty_message

        for field in fields:
            out[tooltip_column_name(field)] = id_series.map(lambda rid, f=field: _tip_for(rid, f))
        return out

    if records is None or records.empty or "RECORD_ID" not in records.columns:
        return display

    project_by_id = records.set_index(records["RECORD_ID"].astype(str))["PROJECT_NAME"].astype(str).to_dict()
    by_project_ids: dict[str, list[str]] = {}
    for rid in display[id_col].astype(str):
        proj = project_by_id.get(rid)
        if proj:
            by_project_ids.setdefault(proj, []).append(rid)

    out = display.copy()
    for field in fields:
        out[tooltip_column_name(field)] = empty_message

    for project, ids in by_project_ids.items():
        by_record = history_svc.load_field_history_for_records(
            project,
            ids,
            fields,
            actor_roles=actor_roles,
            empty_message=empty_message,
        )
        for idx, row in display.iterrows():
            rid = str(row[id_col])
            if project_by_id.get(rid) != project:
                continue
            for field in fields:
                out.at[idx, tooltip_column_name(field)] = by_record.get(rid, {}).get(field, empty_message)
    return out


def _strip_tooltip_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=[col for col in df.columns if col.startswith(TIP_PREFIX)], errors="ignore")


def _record_id_column(display: pd.DataFrame) -> str | None:
    for col in ("elvis_id", "id"):
        if col in display.columns:
            return col
    return None


def grid_widget_key(base_key: str, df: pd.DataFrame, *, rev: int = 0) -> str:
    """Stable Streamlit widget key that changes when rows, values, or tip revision change."""
    if df.empty:
        return f"{base_key}_0_r{rev}"
    id_col = _record_id_column(df)
    if not id_col:
        return f"{base_key}_{len(df)}_r{rev}"
    # Include editable cell values so a successful save remounts with DB data
    # instead of AgGrid reusing a stale empty component model.
    value_cols = [id_col] + [
        col
        for col in (
            "Final_Usage",
            "FINAL_REVIEWER",
            "REASON FOR REMOVAL",
            "REASON FOR REMOVAL [Other]",
            "POSSIBLE ERRORS",
            "ADMIN_APPROVED",
            "2x_REVIEWED_BY",
            "2x_REVIEWED_FLAG",
        )
        if col in df.columns
    ]
    payload = df[value_cols].astype(str).fillna("").to_csv(index=False)
    digest = hashlib.md5(payload.encode(), usedforsecurity=False).hexdigest()[:12]
    return f"{base_key}_{len(df)}_{digest}_r{rev}"


def _coerce_aggrid_frame(data) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data.copy()
    return pd.DataFrame(data)


def _preferred_column_widths(columns: list[str]) -> dict[str, int]:
    widths = {
        "Elvis_Date": 120,
        "elvis_id": 100,
        "Assigned To": 120,
        "Assigned to me": 120,
        "Final_Usage": 120,
        "FINAL_REVIEWER": 140,
        "REASON FOR REMOVAL": 170,
        "REASON FOR REMOVAL [Other]": 180,
        "POSSIBLE ERRORS": 160,
        "id": 90,
        "DATE_SUBMITTED": 160,
        "DATE": 110,
        "INTERV_INIT": 110,
        "ROUTE_SURVEYEDCode": 150,
        "ADMIN_APPROVED": 130,
        "2x_REVIEWED_BY": 140,
        "2x_REVIEWED_FLAG": 140,
    }
    return {col: widths.get(col, _DEFAULT_COL_WIDTH) for col in columns}


def render_history_data_editor(
    prepared: pd.DataFrame,
    *,
    editor_key: str,
    editable_fields: set[str],
    column_config: dict[str, st.column_config.Column],
    extra_disabled: list[str] | None = None,
    selectbox_options: dict[str, list[str]] | None = None,
    checkbox_fields: set[str] | None = None,
) -> pd.DataFrame:
    """Editable grid with per-cell decision-history hover tooltips."""
    if prepared.empty:
        return prepared

    tip_cols = [col for col in prepared.columns if col.startswith(TIP_PREFIX)]
    tip_rev = int(st.session_state.get(tip_revision_key(editor_key), 0))
    if tip_cols:
        return _render_aggrid_history_editor(
            prepared,
            editor_key=editor_key,
            editable_fields=set(editable_fields or set()),
            extra_disabled=extra_disabled,
            selectbox_options=selectbox_options,
            checkbox_fields=checkbox_fields,
            tip_rev=tip_rev,
        )

    grid = _strip_tooltip_columns(prepared)
    selectbox_options = selectbox_options or {}
    checkbox_fields = checkbox_fields or set()
    config = dict(column_config or {})

    for field, options in selectbox_options.items():
        if field in grid.columns:
            config[field] = st.column_config.SelectboxColumn(options=options)

    for field in checkbox_fields:
        if field in grid.columns:
            config[field] = st.column_config.CheckboxColumn()

    disabled = [col for col in grid.columns if col not in editable_fields]
    disabled.extend(extra_disabled or [])
    for col in ("Assigned to me", "Assigned To"):
        if col in grid.columns and col not in disabled:
            disabled.append(col)

    return st.data_editor(
        grid,
        column_config=config,
        disabled=disabled,
        use_container_width=True,
        hide_index=True,
        key=grid_widget_key(editor_key, grid, rev=tip_rev),
        num_rows="fixed",
        height=_GRID_HEIGHT,
    )


def _render_aggrid_history_editor(
    prepared: pd.DataFrame,
    *,
    editor_key: str,
    editable_fields: set[str],
    extra_disabled: list[str] | None,
    selectbox_options: dict[str, list[str]] | None,
    checkbox_fields: set[str] | None,
    tip_rev: int,
) -> pd.DataFrame:
    selectbox_options = selectbox_options or {}
    checkbox_fields = checkbox_fields or set()
    blocked = set(extra_disabled or [])
    blocked.update({"Assigned to me", "Assigned To"})

    # Tip columns stay in the frame for tooltipField, but never as visible/layout columns.
    prepared = prepared.reset_index(drop=True)
    visible = _strip_tooltip_columns(prepared).copy()
    for col in visible.columns:
        if col in selectbox_options:
            visible[col] = visible[col].map(
                lambda v: ""
                if v is None or (isinstance(v, float) and pd.isna(v))
                else str(v).strip()
            )
        elif col in checkbox_fields:
            visible[col] = visible[col].fillna(False).astype(bool)
        else:
            visible[col] = visible[col].fillna("").astype(str)
            visible[col] = visible[col].replace({"nan": "", "None": "", "<NA>": ""})
    tip_only = prepared[[c for c in prepared.columns if c.startswith(TIP_PREFIX)]].copy()
    grid_df = pd.concat([visible, tip_only], axis=1)

    gb = GridOptionsBuilder.from_dataframe(grid_df)
    gb.configure_default_column(
        filterable=True,
        sortable=True,
        resizable=True,
        minWidth=_DEFAULT_COL_WIDTH,
        width=_DEFAULT_COL_WIDTH,
        flex=0,
        wrapText=False,
        floatingFilter=False,
        suppressSizeToFit=True,
    )

    preferred_widths = _preferred_column_widths(list(visible.columns))

    for col in grid_df.columns:
        if col.startswith(TIP_PREFIX):
            gb.configure_column(col, hide=True, width=1, maxWidth=1, suppressSizeToFit=True)
            continue

        tip_col = tooltip_column_name(col)
        editable = col in editable_fields and col not in blocked
        width = int(preferred_widths.get(col, _DEFAULT_COL_WIDTH))
        col_kwargs: dict = {
            "editable": editable,
            "suppressSizeToFit": True,
            "flex": 0,
            "minWidth": width,
            "width": width,
        }
        if tip_col in grid_df.columns:
            col_kwargs["tooltipField"] = tip_col

        if col in selectbox_options:
            values = ["" if v is None else str(v) for v in selectbox_options[col]]
            col_kwargs["cellEditor"] = "agSelectCellEditor"
            col_kwargs["cellEditorParams"] = {"values": values}
        elif col in checkbox_fields:
            col_kwargs["cellRenderer"] = "agCheckboxCellRenderer"
            col_kwargs["cellEditor"] = "agCheckboxCellEditor"

        gb.configure_column(col, **col_kwargs)

    gb.configure_grid_options(
        suppressRowClickSelection=True,
        enableBrowserTooltips=True,
        tooltipShowDelay=0,
        tooltipHideDelay=12000,
        rowHeight=36,
        headerHeight=40,
        animateRows=False,
        enableCellTextSelection=True,
        ensureDomOrder=True,
        stopEditingWhenCellsLoseFocus=True,
        domLayout="normal",
        suppressHorizontalScroll=False,
        alwaysShowHorizontalScroll=True,
        alwaysShowVerticalScroll=True,
        onFirstDataRendered=_ON_FIRST_DATA_RENDERED,
    )

    response = AgGrid(
        grid_df,
        gridOptions=gb.build(),
        height=_GRID_HEIGHT,
        theme="balham",
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        columns_auto_size_mode=ColumnsAutoSizeMode.NO_AUTOSIZE,
        use_container_width=True,
        key=grid_widget_key(editor_key, visible, rev=tip_rev + 100),
        update_mode=GridUpdateMode.VALUE_CHANGED,
        data_return_mode=DataReturnMode.AS_INPUT,
        reload_data=True,
        enable_enterprise_modules=False,
        custom_css=_AGGRID_CUSTOM_CSS,
    )
    edited = _strip_tooltip_columns(_coerce_aggrid_frame(response.get("data")))
    return _reconcile_aggrid_edits(visible, edited, editable_fields)


def _reconcile_aggrid_edits(
    prepared_visible: pd.DataFrame,
    edited: pd.DataFrame,
    editable_fields: set[str],
) -> pd.DataFrame:
    """Align AgGrid edits by record id; fall back to prepared data if return is empty."""
    if edited.empty:
        return prepared_visible.copy()
    id_col = _record_id_column(prepared_visible)
    if not id_col or id_col not in edited.columns:
        return edited if not edited.empty else prepared_visible.copy()

    out = prepared_visible.copy()
    edited_map = {
        str(row[id_col]).strip(): row
        for _, row in edited.iterrows()
        if str(row[id_col]).strip()
    }
    for idx, row in out.iterrows():
        rid = str(row[id_col]).strip()
        if not rid or rid not in edited_map:
            continue
        new_row = edited_map[rid]
        for field in editable_fields:
            if field in out.columns and field in edited.columns:
                out.at[idx, field] = new_row[field]
    return out


def history_grid_caption(*, review_only: bool = False) -> None:
    """Retained for callers; grid instructions are intentionally not displayed."""


def consume_save_flash(editor_key: str) -> dict | None:
    return st.session_state.pop(f"{editor_key}__save_flash", None)


def mark_saved_flash(editor_key: str, changed: int) -> None:
    """Queue a brief top-of-grid success message and force tip refresh on next paint."""
    st.session_state[f"{editor_key}__save_flash"] = {"count": int(changed)}
    bump_tip_revision(editor_key)
