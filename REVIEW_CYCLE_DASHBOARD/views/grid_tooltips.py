"""Cell-level decision-history helpers for editable grids.

History text is attached as hidden ``__tip_*`` columns, then shown on cell hover
via AgGrid ``tooltipField`` (browser tooltips). Edits still return a normal frame.
"""

from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, DataReturnMode, GridOptionsBuilder, GridUpdateMode

from services import history as history_svc

TIP_PREFIX = "__tip_"


def tooltip_column_name(field: str) -> str:
    return f"{TIP_PREFIX}{field}"


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
        for field in fields:
            out[tooltip_column_name(field)] = display[id_col].astype(str).map(
                lambda rid, f=field: by_record.get(rid, {}).get(f, empty_message)
            )
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


def grid_widget_key(base_key: str, df: pd.DataFrame) -> str:
    """Stable Streamlit widget key that changes when visible rows change."""
    if df.empty:
        return f"{base_key}_0"
    id_col = _record_id_column(df)
    if not id_col:
        return f"{base_key}_{len(df)}"
    ids = "|".join(sorted(df[id_col].astype(str).unique()))
    digest = hashlib.md5(ids.encode(), usedforsecurity=False).hexdigest()[:12]
    return f"{base_key}_{len(df)}_{digest}"


def _coerce_aggrid_frame(data) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data.copy()
    return pd.DataFrame(data)


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
    if tip_cols:
        return _render_aggrid_history_editor(
            prepared,
            editor_key=editor_key,
            editable_fields=set(editable_fields or set()),
            extra_disabled=extra_disabled,
            selectbox_options=selectbox_options,
            checkbox_fields=checkbox_fields,
        )

    # Fallback when no tip columns were attached.
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
        key=grid_widget_key(editor_key, grid),
        num_rows="fixed",
    )


def _render_aggrid_history_editor(
    prepared: pd.DataFrame,
    *,
    editor_key: str,
    editable_fields: set[str],
    extra_disabled: list[str] | None,
    selectbox_options: dict[str, list[str]] | None,
    checkbox_fields: set[str] | None,
) -> pd.DataFrame:
    selectbox_options = selectbox_options or {}
    checkbox_fields = checkbox_fields or set()
    blocked = set(extra_disabled or [])
    blocked.update({"Assigned to me", "Assigned To"})

    gb = GridOptionsBuilder.from_dataframe(prepared)
    gb.configure_default_column(
        filterable=True,
        sortable=True,
        resizable=True,
        minWidth=90,
        wrapText=False,
    )

    for col in prepared.columns:
        if col.startswith(TIP_PREFIX):
            gb.configure_column(col, hide=True)
            continue

        tip_col = tooltip_column_name(col)
        editable = col in editable_fields and col not in blocked
        col_kwargs: dict = {"editable": editable}
        if tip_col in prepared.columns:
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
        rowHeight=40,
        headerHeight=42,
        animateRows=False,
        enableCellTextSelection=True,
        stopEditingWhenCellsLoseFocus=True,
    )

    visible = _strip_tooltip_columns(prepared)
    row_count = len(visible)
    height = min(max(row_count * 40 + 96, 280), 720)

    response = AgGrid(
        prepared,
        gridOptions=gb.build(),
        height=height,
        theme="streamlit",
        allow_unsafe_jscode=False,
        fit_columns_on_grid_load=False,
        use_container_width=True,
        key=grid_widget_key(editor_key, visible),
        update_mode=GridUpdateMode.MODEL_CHANGED,
        data_return_mode=DataReturnMode.AS_INPUT,
        reload_data=False,
        enable_enterprise_modules=False,
    )
    edited = _coerce_aggrid_frame(response.get("data"))
    return _strip_tooltip_columns(edited)


def history_grid_caption(*, review_only: bool = False) -> None:
    st.caption(
        'Hover an editable cell for its timeline — e.g. '
        '"Kesar 1/20/2026 2:00:00am - Use ; Tosia 2/1/2026 9:05:00am - Remove". '
        'Empty cells show "No decision history yet."'
    )
