"""Cell-level decision-history helpers for editable grids.

Editable tables use ``st.data_editor`` (not AgGrid) so cell edits do not
trigger a full-page custom-component remount on every value change.
"""

from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd
import streamlit as st

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
    """Attach hidden per-field tooltip columns (used as column help text)."""
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
    """Editable grid via ``st.data_editor`` — avoids AgGrid full-page remounts."""
    if prepared.empty:
        return prepared

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
    if "Assigned to me" in grid.columns and "Assigned to me" not in disabled:
        disabled.append("Assigned to me")

    return st.data_editor(
        grid,
        column_config=config,
        disabled=disabled,
        use_container_width=True,
        hide_index=True,
        key=grid_widget_key(editor_key, grid),
        num_rows="fixed",
    )


def history_grid_caption(*, review_only: bool = False) -> None:
    """Reserved for optional grid hints; kept as a no-op for a cleaner workspace."""
    return
