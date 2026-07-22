from __future__ import annotations

import re
from datetime import date

import pandas as pd
import streamlit as st

from core.data_access import (
    ensure_route_codes_for_project,
    load_assignments,
    records_to_dataframe,
    records_to_elvis_review,
)
from core.s3_utils import dataframe_to_excel_bytes
from core.sync_watcher import render_sync_banner
from pipeline.elvis_review_format import has_transfer_suggestions
from pipeline.ingest import format_ingest_counts, sync_and_export
from rc_auth.access import is_cleaning_head, is_super_admin_user
from services import assignments as assignment_svc
from services import notifications as notify_svc
from views.filters import apply_record_filters, record_id_column, subset_records_for_display
from views.record_fields import render_editable_elvis_table
from views.ui import (
    empty_state,
    filter_panel,
    loading,
    page_header,
    progress_status,
    section_title,
    set_operation_flash,
    stats_bar,
    workspace_toolbar,
)

# Auto-approved by pipeline: Use + Tosia or Field approved — hidden from default cleaning queue.
_CLEANING_EXCLUDED_REVIEWERS = frozenset({"tosia", "field approved"})
_AUTO_REMOVED_REVIEWER = "test/no 5 min"


def _is_empty_final_usage(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "<na>"}


def _norm_reviewer(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    if text in {"nan", "none", "<na>"}:
        return ""
    text = re.sub(r"\s*/\s*", "/", text)
    return re.sub(r"\s+", " ", text).strip()


def _is_empty_reviewer(value) -> bool:
    return _norm_reviewer(value) == ""


def _is_removed_usage(value) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() == "remove"


def _records_with_resolved_decisions(records: pd.DataFrame) -> pd.DataFrame:
    """Align typed FINAL_USAGE / FINAL_REVIEWER with payload values used in Elvis_Review display."""
    if records.empty:
        return records
    out = records.copy()
    payloads = records_to_dataframe(records).reset_index(drop=True)
    out = out.reset_index(drop=True)
    if "FINAL_USAGE" not in out.columns:
        out["FINAL_USAGE"] = ""
    if "FINAL_REVIEWER" not in out.columns:
        out["FINAL_REVIEWER"] = ""
    if "Final_Usage" in payloads.columns:
        empty_usage = out["FINAL_USAGE"].map(_is_empty_final_usage)
        out.loc[empty_usage, "FINAL_USAGE"] = payloads.loc[empty_usage, "Final_Usage"].fillna("").astype(str)
    if "FINAL_REVIEWER" in payloads.columns:
        empty_reviewer = out["FINAL_REVIEWER"].map(_is_empty_reviewer)
        out.loc[empty_reviewer, "FINAL_REVIEWER"] = payloads.loc[empty_reviewer, "FINAL_REVIEWER"].fillna("").astype(str)
    return out


def _usage_series(records: pd.DataFrame) -> pd.Series:
    if "FINAL_USAGE" in records.columns:
        return records["FINAL_USAGE"]
    return pd.Series("", index=records.index)


def _reviewer_series(records: pd.DataFrame) -> pd.Series:
    if "FINAL_REVIEWER" in records.columns:
        return records["FINAL_REVIEWER"]
    return pd.Series("", index=records.index)


def _auto_removed_record_mask(records: pd.DataFrame) -> pd.Series:
    if records.empty:
        return pd.Series(dtype=bool)
    usage = _usage_series(records)
    reviewer = _reviewer_series(records)
    return usage.map(_is_removed_usage) & reviewer.map(lambda v: _norm_reviewer(v) == _AUTO_REMOVED_REVIEWER)


def _blank_decision_mask(records: pd.DataFrame) -> pd.Series:
    if records.empty:
        return pd.Series(dtype=bool)
    usage = _usage_series(records)
    reviewer = _reviewer_series(records)
    return usage.map(_is_empty_final_usage) & reviewer.map(_is_empty_reviewer)


def _apply_removed_filters(
    records: pd.DataFrame,
    *,
    include_removed: bool,
    only_removed: bool,
) -> pd.DataFrame:
    if records.empty:
        return records
    auto_removed = _auto_removed_record_mask(records)
    if only_removed:
        return records[auto_removed].copy()
    if not include_removed:
        return records[~auto_removed].copy()
    return records.copy()


def _cleaning_queue_records(records: pd.DataFrame, include_auto_approved: bool = False) -> pd.DataFrame:
    """Default queue: empty Final Usage OR reviewer other than Tosia / Field approved."""
    if records.empty or include_auto_approved:
        return records.copy()
    usage = _usage_series(records)
    reviewer = _reviewer_series(records)
    empty_usage = usage.map(_is_empty_final_usage)
    reviewer_norm = reviewer.map(_norm_reviewer)
    other_reviewer = ~reviewer_norm.isin(_CLEANING_EXCLUDED_REVIEWERS)
    return records[empty_usage | other_reviewer].copy()


def _filter_records_with_suggestions(records: pd.DataFrame) -> pd.DataFrame:
    if records.empty:
        return records
    payloads = records_to_dataframe(records)
    ids = {
        str(rid)
        for _, row in payloads.iterrows()
        if has_transfer_suggestions(row.to_dict())
        for rid in [row.get("elvis_id"), row.get("id")]
        if rid is not None and str(rid).strip()
    }
    return records[records["RECORD_ID"].astype(str).isin(ids)].copy()


def _count_transfer_suggestions(records: pd.DataFrame) -> int:
    if records.empty:
        return 0
    payloads = records_to_dataframe(records)
    return int(payloads.apply(lambda row: has_transfer_suggestions(row.to_dict()), axis=1).sum())


def _build_elvis_review_excel_bytes(display: pd.DataFrame) -> bytes | None:
    if display.empty:
        return None
    return dataframe_to_excel_bytes({"Elvis_Review": display})


def _render_queue_downloads(display: pd.DataFrame, project_filter: list[str]) -> None:
    with loading("Preparing the cleaning queue Excel download..."):
        excel_bytes = _build_elvis_review_excel_bytes(display)
    if not excel_bytes:
        return
    label = project_filter[0] if len(project_filter) == 1 else "multi_project"
    filename = f"cleaning_queue_{label}_{date.today():%Y%m%d}.xlsx".replace(" ", "_")
    st.download_button(
        "Download Excel (Elvis_Review)",
        data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="clean_queue_kingelvis_xlsx",
    )


def _records_for_projects(project_filter: list[str], only_new: bool = False) -> pd.DataFrame:
    from core.data_access import load_records_for_projects

    if not project_filter:
        return load_records_for_projects(None, only_new=only_new)
    return load_records_for_projects(project_filter, only_new=only_new)


def _assignments_for_user(user: dict, team: str = "cleaning") -> pd.DataFrame:
    """Load active assignments; match display name or email."""
    candidates = []
    for key in ("name", "EMAIL", "DISPLAY_NAME"):
        value = user.get(key)
        if value and str(value).strip() and str(value).strip() not in candidates:
            candidates.append(str(value).strip())
    frames = []
    for who in candidates:
        df = load_assignments(assigned_to=who, team=team)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ASSIGNMENT_ID"])


def _assigned_record_ids(user: dict, project_filter: list[str]) -> set[str]:
    assignments = _assignments_for_user(user, team="cleaning")
    if assignments.empty:
        return set()
    if project_filter and "PROJECT_NAME" in assignments.columns:
        assignments = assignments[assignments["PROJECT_NAME"].isin(project_filter)]
    return set(assignments["RECORD_ID"].astype(str).tolist())


def _norm_record_id(value) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    return text


def _cleaning_assignee_overlay() -> dict[str, str]:
    """In-session assignee overrides so metrics update immediately after assign."""
    return st.session_state.setdefault("cleaning_head_assignee_overlay", {})


def _cleaning_assignee_map(project_filter: list[str]) -> dict[str, str]:
    """Map RECORD_ID -> current cleaning ASSIGNED_TO for the given projects."""
    frames = []
    if project_filter:
        for project in project_filter:
            df = load_assignments(team="cleaning", project_name=project)
            if not df.empty:
                frames.append(df)
    else:
        df = load_assignments(team="cleaning")
        if not df.empty:
            frames.append(df)
    result: dict[str, str] = {}
    if frames:
        all_a = pd.concat(frames, ignore_index=True)
        if not all_a.empty and "RECORD_ID" in all_a.columns:
            # load_assignments is already ordered by PRIORITY / ASSIGNED_AT; keep first per record.
            all_a = all_a.drop_duplicates(subset=["RECORD_ID"], keep="first")
            for _, row in all_a.iterrows():
                assignee = str(row.get("ASSIGNED_TO") or "").strip()
                if not assignee:
                    continue
                result[_norm_record_id(row["RECORD_ID"])] = assignee
    # Prefer live DB values; keep session overlay for ids not yet visible in cache.
    for rid, assignee in _cleaning_assignee_overlay().items():
        result.setdefault(_norm_record_id(rid), assignee)
    return result


def _remember_assignments(record_ids: list[str], cleaner: str) -> None:
    overlay = _cleaning_assignee_overlay()
    for rid in record_ids:
        overlay[_norm_record_id(rid)] = cleaner


def _add_assigned_to_column(display: pd.DataFrame, assignee_map: dict[str, str]) -> pd.DataFrame:
    if display.empty:
        return display
    out = display.copy()
    id_col = record_id_column(out)
    if not id_col:
        out.insert(0, "Assigned To", "Unassigned")
        return out
    out["Assigned To"] = out[id_col].map(
        lambda rid: assignee_map.get(_norm_record_id(rid)) or "Unassigned"
    )
    cols = [c for c in out.columns if c != "Assigned To"]
    insert_at = 0
    for i, col in enumerate(cols):
        if col in ("Elvis_Date", "elvis_id", "id"):
            insert_at = i + 1
    cols.insert(insert_at, "Assigned To")
    return out[cols]


def _empty_queue_message(
    all_records: pd.DataFrame,
    include_auto_approved: bool,
    suggestions_only: bool,
    only_assigned: bool,
    *,
    include_removed: bool = True,
    only_removed: bool = False,
    only_blank_decisions: bool = False,
) -> str:
    if all_records.empty:
        return "No records in this project yet."
    hidden = len(all_records) - len(_cleaning_queue_records(all_records, include_auto_approved=False))
    auto_removed_count = int(_auto_removed_record_mask(all_records).sum())
    if only_assigned:
        return "No records assigned to you match the current filters."
    if only_blank_decisions:
        return "No records with blank Final Usage and Final Reviewer match the current filters."
    if only_removed:
        return "No Test/No 5 min removed records match the current filters."
    if auto_removed_count and not include_removed:
        return (
            f"No records shown — {auto_removed_count} Test/No 5 min removed record(s) are hidden. "
            "Enable **Show Test/No 5 min removes** above to view them."
        )
    if hidden and not include_auto_approved:
        return (
            f"No records shown — {hidden} record(s) are auto-approved (Use + Tosia/Field approved). "
            "Enable **Show auto-approved records** above to view them."
        )
    if suggestions_only:
        return "No records with transfer suggestions. Turn off that filter to see all records."
    return "No records match the current filters."


def _queue_stats(
    all_records: pd.DataFrame,
    queue_records: pd.DataFrame,
    visible_records: pd.DataFrame,
    include_auto_approved: bool,
    assigned_count: int | None = None,
    *,
    include_removed: bool = True,
    only_removed: bool = False,
    only_blank_decisions: bool = False,
) -> list[tuple[str, str]]:
    visible = len(visible_records)
    sug = _count_transfer_suggestions(visible_records)
    stats: list[tuple[str, str]] = []
    if assigned_count is not None:
        stats.append(("Assigned to you", str(assigned_count)))
    stats.append(("Showing", str(visible)))
    if only_blank_decisions:
        stats.append(("Filter", "Blank decisions"))
    elif only_removed:
        stats.append(("Filter", "Test/No 5 min"))
    elif not include_removed:
        auto_removed_hidden = int(_auto_removed_record_mask(queue_records).sum())
        if auto_removed_hidden:
            stats.append(("Hidden removes", str(auto_removed_hidden)))
    if not include_auto_approved and len(all_records) != len(queue_records):
        stats.append(("Auto-approved hidden", str(len(all_records) - len(queue_records))))
    if sug:
        stats.append(("Transfer suggestions", str(sug)))
    return stats


def render_cleaning_page(user: dict) -> None:
    from authentication.auth import is_super_admin
    from core.session_project import get_active_project, require_active_project, set_active_project

    page_header("Elvis_Review")

    role = user.get("ROLE") or user.get("role")
    actor = user.get("name") or user.get("EMAIL")
    email = str(user.get("EMAIL") or user.get("email") or "").strip()
    is_manager = role in ("admin", "manager")
    is_head = is_cleaning_head(user)
    is_cleaning_role = role == "cleaning"
    # Regular cleaners: assigned-only. Cleaning head keeps cleaner role but sees all + assign UI.
    is_regular_cleaner = is_cleaning_role and not is_head
    can_assign = is_manager or is_head
    # Pipeline fetch is a privileged action — super admins only.
    can_fetch = is_super_admin(email)

    project = require_active_project()
    if not project:
        return
    project_filter = [project]

    run_sync = False
    if can_fetch:
        section_title("Workspace controls")
        with workspace_toolbar() as (col_info, col_sync):
            col_info.markdown(
                f"**Working on** `{project}`  \n"
                "Grab the newest surveys for this project. New trips get transfer suggestions; "
                "existing suggestions stay as they are. This usually takes a few minutes."
            )
            run_sync = col_sync.button(
                "Fetch latest records",
                type="primary",
                use_container_width=True,
                help="Refresh the Elvis Review queue. Can take several minutes on larger projects.",
            )

        render_sync_banner(project)

    cache_key = f"route_codes_ensured_{project}"
    if not st.session_state.get(cache_key):
        try:
            with loading("Checking and backfilling missing route codes..."):
                ensure_route_codes_for_project(project)
            st.session_state[cache_key] = True
        except Exception as exc:
            st.warning(f"Route-code check could not complete: {exc}")

    trigger_project = st.session_state.pop("trigger_pipeline", None)
    if trigger_project:
        set_active_project(trigger_project)
        # Use immediately this run; sidebar picks it up on the next rerun.
        project = str(trigger_project)
        project_filter = [project]

    if run_sync and can_fetch:
        try:
            with progress_status(
                f"Fetching the latest Elvis Review for {project}...",
                complete_label="The Elvis Review queue is ready",
            ) as update:
                result = sync_and_export(
                    project,
                    phase="auto",
                    export=False,
                    progress=update,
                )
                update.set_total(update.total + 2)
                update.advance("Sending completion notification...")
                notify_svc.notify(
                    actor,
                    notify_svc.SYNC_COMPLETED,
                    f"Fetched latest Elvis Review records for {project}",
                    project,
                )
                from core.streamlit_cache import bump_data_cache

                update.advance("Refreshing the review queue...")
                bump_data_cache()
            st.success(f"{project}: {format_ingest_counts(result['counts'])}")
        except Exception as exc:
            st.error(f"{project} fetch failed: {exc}")

    tab_labels = ["My Queue"] + (["Assign records"] if can_assign else [])
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        section_title("Queue workspace")
        with loading("Loading the cleaning queue..."):
            assigned_ids: set[str] = set()
            if is_regular_cleaner:
                assigned_ids = _assigned_record_ids(user, project_filter)

            records = _records_for_projects(project_filter, only_new=False)
            all_records = records.copy()
            records = _records_with_resolved_decisions(records)

        with filter_panel("Queue filters", "Choose which records appear in the grid below."):
            st.markdown("**Record visibility**")
            f1, f2 = st.columns(2)
            include_auto_approved = f1.checkbox(
                "Show auto-approved (Use + Tosia / Field approved)",
                value=False,
                help="Hidden by default so cleaners focus on records needing work.",
                key="clean_show_auto_approved",
            )
            suggestions_only = f2.checkbox(
                "Only transfer suggestions",
                value=False,
                key="clean_suggestions_only",
            )

            st.markdown("**Test/No 5 min removes**")
            f3, f4, f5 = st.columns(3)
            include_removed = f3.checkbox(
                "Include removes",
                value=False,
                help="Show Remove + Test/No 5 MIN records (hidden by default).",
                key="clean_show_removed",
            )
            only_removed = f4.checkbox(
                "Only removes",
                value=False,
                help="Show only Remove + Test/No 5 MIN records.",
                key="clean_only_removed",
            )
            only_blank_decisions = f5.checkbox(
                "Only blank Usage & Reviewer",
                value=False,
                help="Both Final_Usage and FINAL_REVIEWER empty.",
                key="clean_only_blank_decisions",
            )

        records = _cleaning_queue_records(records, include_auto_approved=include_auto_approved)
        queue_records = records.copy()
        records = _apply_removed_filters(
            records,
            include_removed=include_removed,
            only_removed=only_removed,
        )
        if only_blank_decisions:
            records = records[_blank_decision_mask(records)].copy()
        if suggestions_only:
            records = _filter_records_with_suggestions(records)
        # Regular cleaners never see unassigned / other people's records.
        if is_regular_cleaner:
            records = records[records["RECORD_ID"].astype(str).isin(assigned_ids)].copy()

        if records.empty:
            st.info(
                _empty_queue_message(
                    all_records,
                    include_auto_approved,
                    suggestions_only,
                    only_assigned=is_regular_cleaner,
                    include_removed=include_removed,
                    only_removed=only_removed,
                    only_blank_decisions=only_blank_decisions,
                )
            )
        else:
            display = records_to_elvis_review(records)
            if display.empty:
                st.info("No records match the current filters.")
            else:
                if is_head:
                    with loading("Loading cleaning assignments..."):
                        assignee_map = _cleaning_assignee_map(project_filter)
                    display = _add_assigned_to_column(display, assignee_map)
                display = apply_record_filters(display, key_prefix="clean")
                visible_records = subset_records_for_display(display, records)
                stats_bar(
                    _queue_stats(
                        all_records,
                        queue_records,
                        visible_records,
                        include_auto_approved,
                        assigned_count=len(assigned_ids) if is_regular_cleaner else None,
                        include_removed=include_removed,
                        only_removed=only_removed,
                        only_blank_decisions=only_blank_decisions,
                    )
                )

                if _count_transfer_suggestions(visible_records) == 0:
                    st.info(
                        "No transfer suggestions stored yet for visible records. Run **Fetch latest records** "
                        "to backfill SUGGESTED_* fields for Elvis Review."
                    )

                _render_queue_downloads(display, project_filter)
                section_title("Elvis Review")
                display = render_editable_elvis_table(
                    display,
                    visible_records,
                    user,
                    editor_key="clean_queue_editor",
                    project_name=None,
                    history_actor_roles=["cleaning"] if is_cleaning_role else None,
                )

    if can_assign:
        with tabs[1]:
            if is_head:
                _render_cleaning_head_assign_panel(project_filter, user=user)
            else:
                _render_assign_panel(project_filter, user=user)


def _records_by_selected_ids(
    records: pd.DataFrame, selected_ids: list[str]
) -> dict[str, list[str]]:
    by_project: dict[str, list[str]] = {}
    wanted = {_norm_record_id(rid) for rid in selected_ids}
    for _, row in records.iterrows():
        rid = _norm_record_id(row["RECORD_ID"])
        if rid not in wanted:
            continue
        by_project.setdefault(row["PROJECT_NAME"], []).append(rid)
    return by_project


def _assign_to_options(user: dict | None) -> list[str]:
    """Cleaning head: cleaners only. Super admin: cleaners + admins/super admins."""
    return assignment_svc.cleaning_assignee_options(
        include_privileged=is_super_admin_user(user)
    )


def _guard_assignee(user: dict | None, assignee: str) -> bool:
    """Block assigning to people outside the caller's allowed roster."""
    allowed = {n.strip().lower() for n in _assign_to_options(user)}
    return str(assignee or "").strip().lower() in allowed


def _render_cleaning_head_assign_panel(
    project_filter: list[str], *, user: dict | None = None
) -> None:
    """Cleaning head: bulk-assign unassigned blank-decision records by count."""
    section_title("Assign records")
    st.caption(
        "Only records with blank Final Usage and blank Final Reviewer can be assigned. "
        "Choose how many unassigned ones to give a cleaner."
    )

    with loading("Loading records available for assignment..."):
        records = _records_for_projects(project_filter, only_new=False)
        records = _records_with_resolved_decisions(records)
    if records.empty:
        st.info("No records in this project yet.")
        return
    # Needs cleaning: blank Final Usage AND blank Final Reviewer.
    records = records[_blank_decision_mask(records)].copy()
    if records.empty:
        st.info("No records with blank Final Usage and Final Reviewer need assigning.")
        return

    display = records_to_elvis_review(records)
    if display.empty:
        st.info("No records available to assign.")
        return

    with loading("Loading current cleaning assignments..."):
        assignee_map = _cleaning_assignee_map(project_filter)
    queue_ids = [_norm_record_id(rid) for rid in records["RECORD_ID"].tolist()]
    unassigned_ids = [rid for rid in queue_ids if rid not in assignee_map]
    already_assigned = len(queue_ids) - len(unassigned_ids)
    stats_bar([
        ("Records to assign", str(len(unassigned_ids))),
        ("Already assigned", str(already_assigned)),
        ("Blank Usage & Reviewer", str(len(queue_ids))),
    ])

    display = _add_assigned_to_column(display, assignee_map)
    display = apply_record_filters(display, key_prefix="head_assign")
    st.dataframe(display, use_container_width=True, hide_index=True)

    cleaners = _assign_to_options(user)
    max_bulk_count = max(len(unassigned_ids), 1)
    next_bulk_count = st.session_state.pop(
        "_head_bulk_count_next",
        st.session_state.get("head_bulk_count", min(25, max_bulk_count)),
    )
    st.session_state["head_bulk_count"] = min(
        max(int(next_bulk_count or 1), 1),
        max_bulk_count,
    )
    b1, b2 = st.columns(2)
    bulk_count = b1.number_input(
        "How many to assign",
        min_value=1,
        max_value=max_bulk_count,
        key="head_bulk_count",
    )
    bulk_cleaner = b2.selectbox(
        "Assign to",
        options=cleaners or ["(no cleaning users)"],
        key="head_bulk_cleaner",
    )
    bulk_clicked = st.button(
        f"Assign next {int(bulk_count)} unassigned",
        type="primary",
        disabled=not (unassigned_ids and cleaners),
        key="head_bulk_btn",
    )

    flash = st.session_state.pop("cleaning_head_assign_flash", None)
    if flash:
        st.success(flash)

    if bulk_clicked and unassigned_ids and cleaners:
        if not _guard_assignee(user, bulk_cleaner):
            st.error("You can only assign records to cleaning team members.")
            return
        target_ids = unassigned_ids[: int(bulk_count)]
        by_project = _records_by_selected_ids(records, target_ids)
        total = 0
        assigned_ids: list[str] = []
        with progress_status(
            f"Assigning records to {bulk_cleaner}...",
            complete_label="Cleaning records assigned",
        ) as update:
            step_total = max(len(by_project) + 1, 1)
            for index, (project, ids) in enumerate(by_project.items(), start=1):
                update(index, step_total, f"Assigning {len(ids)} record(s) for {project}...")
                assignment_svc.assign_records(project, ids, bulk_cleaner, team="cleaning")
                assigned_ids.extend(ids)
                total += len(ids)
            _remember_assignments(assigned_ids, bulk_cleaner)
            update(step_total, step_total, "Sending assignment notification...")
            notify_svc.notify(
                bulk_cleaner,
                notify_svc.NEW_ASSIGNMENT,
                f"You were assigned {total} record(s) to clean.",
            )
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
        remaining = max(len(unassigned_ids) - total, 1)
        st.session_state["_head_bulk_count_next"] = min(
            int(bulk_count),
            remaining,
        )
        set_operation_flash(f"Assigned {total} record(s) to {bulk_cleaner}.")
        # Stay on Review Cycle after rerun so metrics refresh in-place.
        st.query_params["page"] = "review_cycle"
        st.rerun()


def _render_assign_panel(
    project_filter: list[str], *, user: dict | None = None
) -> None:
    section_title("Assign new records to cleaners")
    st.caption(
        "Filter the table, then choose how many of the visible records to assign "
        "(top of the list, in current order)."
    )
    with loading("Loading new records for assignment..."):
        records = _records_for_projects(project_filter, only_new=True)
    if records.empty:
        st.info("No new/unassigned records to assign.")
        return
    with loading("Checking active cleaning assignments..."):
        active = set()
        for project in project_filter:
            active |= assignment_svc.active_record_ids(project, team="cleaning")
    records = records[~records["RECORD_ID"].astype(str).isin(active)]
    display = records_to_elvis_review(records)
    if display.empty:
        st.info("All new records are already assigned.")
        return
    display = apply_record_filters(display, key_prefix="assign")
    id_col = record_id_column(display)
    st.dataframe(display, use_container_width=True, hide_index=True)

    available_ids = (
        [_norm_record_id(rid) for rid in display[id_col].astype(str).tolist()]
        if id_col and not display.empty
        else []
    )
    cleaners = _assign_to_options(user)
    max_count = max(len(available_ids), 1)
    default_count = min(25, max_count)
    c1, c2, c3 = st.columns(3)
    assign_count = c1.number_input(
        "How many to assign",
        min_value=1,
        max_value=max_count,
        value=default_count,
        help="Assigns this many records from the filtered table (top rows first).",
        key="assign_panel_count",
    )
    cleaner = c2.selectbox(
        "Assign to",
        options=cleaners or ["(no cleaning users)"],
        key="assign_panel_cleaner",
    )
    priority = c3.number_input(
        "Priority",
        min_value=1,
        max_value=1000,
        value=100,
        key="assign_panel_priority",
    )
    selected_ids = available_ids[: int(assign_count)]
    can_assign = bool(selected_ids and cleaners)
    if st.button(
        f"Assign next {int(assign_count)}",
        type="primary",
        disabled=not can_assign,
        key="assign_panel_btn",
    ):
        if not _guard_assignee(user, cleaner):
            st.error(
                "You can only assign to cleaning team members."
                if not is_super_admin_user(user)
                else "That assignee is not in your allowed list."
            )
            return
        by_project = _records_by_selected_ids(records, selected_ids)
        total = 0
        assigned_ids: list[str] = []
        with progress_status(
            f"Assigning {len(selected_ids)} record(s) to {cleaner}...",
            complete_label="Selected records assigned",
        ) as update:
            step_total = max(len(by_project) + 1, 1)
            for index, (project, ids) in enumerate(by_project.items(), start=1):
                update(index, step_total, f"Assigning {len(ids)} record(s) for {project}...")
                assignment_svc.assign_records(
                    project, ids, cleaner, team="cleaning", priority=int(priority)
                )
                assigned_ids.extend(ids)
                total += len(ids)
            _remember_assignments(assigned_ids, cleaner)
            update(step_total, step_total, "Sending assignment notification...")
            notify_svc.notify(
                cleaner,
                notify_svc.NEW_ASSIGNMENT,
                f"You were assigned {total} record(s) to clean.",
            )
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
        set_operation_flash(f"Assigned {total} record(s) to {cleaner}.")
        st.rerun()
