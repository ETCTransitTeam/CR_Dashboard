"""Unassign cleaning assignments — used as an Elvis_Review tab (and legacy standalone page)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from core.streamlit_cache import bump_data_cache
from rc_auth.access import can_manage_cleaning_assignments, is_super_admin_user
from services import assignments as assign_svc
from views.ui import info_strip, page_header, section_title, set_operation_flash

DISPLAY_COLS = [
    "RECORD_ID",
    "ASSIGNED_TO",
    "ASSIGNED_AT",
    "PRIORITY",
    "TEAM",
    "PROJECT_NAME",
    "ASSIGNMENT_ID",
]


def _norm_record_id(value) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return ""
    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    try:
        as_float = float(text)
        if as_float.is_integer():
            return str(int(as_float))
    except (TypeError, ValueError):
        pass
    return text


def _actor_display_name(user: dict) -> str:
    from services.notifications import actor_display_name

    return actor_display_name(user)


def _forget_overlay_ids(record_ids: list[str]) -> None:
    overlay = st.session_state.get("cleaning_head_assignee_overlay")
    if not isinstance(overlay, dict):
        return
    for rid in record_ids:
        overlay.pop(_norm_record_id(rid), None)


def _prepare_assignments_frame(project: str, *, super_admin: bool) -> pd.DataFrame:
    """Load + normalize assignments. Uses Streamlit cache (no spinner)."""
    assignments = assign_svc.load_assignments(team="cleaning", project_name=project)
    if assignments.empty:
        return assignments

    view = assignments.copy()
    view["RECORD_ID"] = view["RECORD_ID"].map(_norm_record_id)
    if "ASSIGNED_TO" in view.columns:
        view["ASSIGNED_TO"] = view["ASSIGNED_TO"].fillna("").astype(str).str.strip()
    if "ASSIGNMENT_ID" in view.columns:
        view["ASSIGNMENT_ID"] = pd.to_numeric(view["ASSIGNMENT_ID"], errors="coerce")

    if not super_admin:
        roster_key = f"assign_mgr_cleaning_roster_{project}"
        if roster_key not in st.session_state:
            st.session_state[roster_key] = {
                n.strip().lower()
                for n in assign_svc.cleaning_assignee_options(include_privileged=False)
            }
        allowed = st.session_state[roster_key]
        view = view[view["ASSIGNED_TO"].astype(str).str.strip().str.lower().isin(allowed)]
    return view


def _summary_counts(view: pd.DataFrame) -> tuple[int, int, str]:
    """Return (row_count, unique_record_ids, by-person summary)."""
    rows = len(view)
    unique = int(view["RECORD_ID"].nunique()) if (not view.empty and "RECORD_ID" in view.columns) else 0
    if view.empty or "ASSIGNED_TO" not in view.columns:
        return rows, unique, ""
    by_person = (
        view.groupby("ASSIGNED_TO", dropna=False)["RECORD_ID"]
        .count()
        .sort_values(ascending=False)
    )
    summary = " · ".join(f"{name or '(blank)'}: {n}" for name, n in by_person.items())
    return rows, unique, summary


def _assignment_ids_for_records(view: pd.DataFrame, record_ids: list[str]) -> list[int]:
    wanted = {_norm_record_id(rid) for rid in record_ids if _norm_record_id(rid)}
    if not wanted or view.empty or "ASSIGNMENT_ID" not in view.columns:
        return []
    mask = view["RECORD_ID"].astype(str).map(_norm_record_id).isin(wanted)
    ids: list[int] = []
    for value in view.loc[mask, "ASSIGNMENT_ID"].tolist():
        try:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            ids.append(int(value))
        except (TypeError, ValueError):
            continue
    return sorted(set(ids))


def _assignee_record_counts(view: pd.DataFrame, record_ids: list[str]) -> dict[str, int]:
    """How many selected record IDs were held by each ASSIGNED_TO (unique records)."""
    wanted = {_norm_record_id(rid) for rid in record_ids if _norm_record_id(rid)}
    if not wanted or view.empty or "ASSIGNED_TO" not in view.columns:
        return {}
    subset = view[view["RECORD_ID"].astype(str).map(_norm_record_id).isin(wanted)].copy()
    if subset.empty:
        return {}
    subset["_RID"] = subset["RECORD_ID"].map(_norm_record_id)
    subset["ASSIGNED_TO"] = subset["ASSIGNED_TO"].fillna("").astype(str).str.strip()
    counts: dict[str, int] = {}
    for assignee, group in subset.groupby("ASSIGNED_TO", sort=False):
        name = str(assignee or "").strip()
        if not name:
            continue
        counts[name] = int(group["_RID"].nunique())
    return counts


def _notify_unassigned(
    project: str,
    *,
    assignee_counts: dict[str, int],
    actor: str,
) -> None:
    from services import notifications as notify_svc

    actor_label = (actor or "").strip() or "a manager"
    for assignee, count in assignee_counts.items():
        if count <= 0:
            continue
        notify_svc.notify(
            assignee,
            notify_svc.ASSIGNMENT_RELEASED,
            f"{count} cleaning record(s) were unassigned from you by {actor_label}.",
            project_name=project,
        )


def _run_unassign(
    project: str,
    assignment_ids: list[int],
    record_ids: list[str],
    actor: str,
    *,
    assignee_counts: dict[str, int] | None = None,
) -> None:
    if not assignment_ids:
        st.error(
            "Could not match those record IDs to active assignments. "
            "Refresh the page and try again."
        )
        return

    unique_records = sorted({_norm_record_id(r) for r in record_ids if _norm_record_id(r)})
    with st.spinner(
        f"Unassigning {len(unique_records)} record ID(s) "
        f"({len(assignment_ids)} assignment row(s))…"
    ):
        n = assign_svc.unassign_by_assignment_ids(
            assignment_ids,
            actor=actor or None,
            project_name=project,
            record_ids=unique_records,
            team="cleaning",
        )
        still_active = assign_svc.count_active_assignments_for_records(
            project, unique_records, team="cleaning"
        )
        _forget_overlay_ids(unique_records)
        if still_active == 0 and n > 0 and assignee_counts:
            _notify_unassigned(project, assignee_counts=assignee_counts, actor=actor)
        bump_data_cache()
        st.session_state.pop(f"assign_mgr_cleaning_roster_{project}", None)
        st.session_state.pop(f"assign_mgr_selected_{project}", None)

    ids_label = ", ".join(unique_records)
    if still_active > 0:
        st.error(
            f"Unassign did not stick for: {ids_label}. "
            f"Snowflake still has {still_active} active row(s) for them "
            f"(UPDATE reported {n} row(s)). "
            "Check warehouse role permissions on ASSIGNMENTS."
        )
        return
    if n <= 0:
        st.error(
            f"Unassign ran but Snowflake reported 0 rows changed for: {ids_label}. "
            "Those IDs may already be released — refresh and check again."
        )
        return

    set_operation_flash(
        f"Released {n} assignment row(s) for {len(unique_records)} record ID(s): {ids_label}"
    )
    st.rerun()


def render_unassign_records_panel(user: dict, project: str) -> None:
    """Elvis_Review tab body: list active cleaning assignments and unassign them."""
    if not can_manage_cleaning_assignments(user):
        st.error("You do not have permission to unassign cleaning records.")
        return

    if is_super_admin_user(user):
        st.caption(
            "All active cleaning assignments — including those held by admins or super admins."
        )
    else:
        st.caption(
            "Active cleaning-team assignments only. You can unassign cleaners’ records here."
        )

    super_admin = is_super_admin_user(user)
    base_view = _prepare_assignments_frame(project, super_admin=super_admin)

    if base_view.empty:
        st.info("No active cleaning assignments for this project.")
        return

    rows, unique, summary = _summary_counts(base_view)
    section_title("Active assignments")
    info_strip(
        f"{rows} active rows · {unique} unique record IDs"
        + (f" · {summary}" if summary else "")
    )

    dup_counts = (
        base_view["RECORD_ID"].value_counts() if "RECORD_ID" in base_view.columns else pd.Series(dtype=int)
    )
    n_dup_records = int((dup_counts > 1).sum()) if not dup_counts.empty else 0
    if n_dup_records:
        n_extra = int((dup_counts - 1).clip(lower=0).sum())
        st.warning(
            f"{n_dup_records} record ID(s) have duplicate active assignment rows "
            f"({n_extra} extra). Unassigning a record releases **all** of its active rows — "
            "that is why selecting 2 IDs can report 4 rows released."
        )

    assignees = (
        sorted(a for a in base_view["ASSIGNED_TO"].dropna().astype(str).unique() if a)
        if "ASSIGNED_TO" in base_view.columns
        else []
    )

    f1, f2 = st.columns(2)
    assignee_filter = f1.multiselect(
        "Assigned to",
        assignees,
        key=f"assign_mgr_assignees_{project}",
        help="Optional. Narrow the table and unassign list.",
    )
    record_query = f2.text_input(
        "Record ID contains",
        placeholder="Optional",
        key=f"assign_mgr_rid_q_{project}",
    )

    view = base_view
    if assignee_filter:
        view = view[view["ASSIGNED_TO"].isin(assignee_filter)]
    if record_query.strip():
        q = record_query.strip().lower()
        view = view[view["RECORD_ID"].astype(str).str.lower().str.contains(q, na=False)]

    filtered_rows, filtered_unique, filtered_summary = _summary_counts(view)
    if assignee_filter or record_query.strip():
        info_strip(
            f"{filtered_rows} matching rows · {filtered_unique} unique IDs"
            + (f" · {filtered_summary}" if filtered_summary else "")
        )

    if view.empty:
        st.info("No assignments match the current filters.")
        return

    cols = [c for c in DISPLAY_COLS if c in view.columns]
    st.dataframe(view[cols], use_container_width=True, hide_index=True, height=360)

    section_title("Unassign")
    seen: set[str] = set()
    ordered_ids: list[str] = []
    for rid in view["RECORD_ID"].astype(str).tolist():
        if rid not in seen:
            seen.add(rid)
            ordered_ids.append(rid)

    actor = _actor_display_name(user)

    with st.form(f"assign_mgr_unassign_form_{project}", clear_on_submit=True):
        selected = st.multiselect(
            "Records to unassign",
            options=ordered_ids,
            help="Pick IDs here — the page will not reload until you press Unassign selected.",
        )
        submitted = st.form_submit_button(
            "Unassign selected",
            type="primary",
            use_container_width=True,
        )

    if submitted:
        if not selected:
            st.warning("Select at least one record ID first.")
        else:
            assignment_ids = _assignment_ids_for_records(view, list(selected))
            _run_unassign(
                project,
                assignment_ids,
                list(selected),
                actor,
                assignee_counts=_assignee_record_counts(view, list(selected)),
            )

    if st.button(
        f"Unassign all {len(ordered_ids)} visible",
        type="secondary",
        use_container_width=True,
        key=f"assign_mgr_unassign_all_{project}",
    ):
        assignment_ids = _assignment_ids_for_records(view, ordered_ids)
        _run_unassign(
            project,
            assignment_ids,
            ordered_ids,
            actor,
            assignee_counts=_assignee_record_counts(view, ordered_ids),
        )


def render_assignment_manager_page(user: dict) -> None:
    """Legacy standalone page wrapper (kept for safety; prefer Elvis_Review tab)."""
    page_header("Unassign records", "Active cleaning assignments for the current project.")
    if not can_manage_cleaning_assignments(user):
        st.error("You do not have permission to manage cleaning assignments.")
        return
    from core.session_project import require_active_project

    project = require_active_project()
    if not project:
        return
    render_unassign_records_panel(user, project)
