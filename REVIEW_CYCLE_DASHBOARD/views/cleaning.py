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
from services import assignments as assignment_svc
from services import notifications as notify_svc
from views.filters import apply_record_filters, record_id_column, subset_records_for_display
from views.record_card import render_record_card
from views.record_fields import render_editable_elvis_table
from views.ui import empty_state, filter_panel, page_header, section_title, stats_bar, workspace_toolbar

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


def _open_selected(display: pd.DataFrame, records: pd.DataFrame, user: dict, key: str) -> None:
    id_col = record_id_column(display)
    if not id_col:
        return
    options = display[id_col].astype(str).tolist()
    selected_id = st.selectbox("Open record", options=options, key=key)
    if not selected_id:
        return
    match = records[records["RECORD_ID"].astype(str) == str(selected_id)]
    if match.empty:
        return
    project = match["PROJECT_NAME"].iloc[0]
    render_record_card(project, selected_id, user, allow_admin=False)


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


def _add_assigned_to_me_column(display: pd.DataFrame, assigned_ids: set[str]) -> pd.DataFrame:
    if display.empty:
        return display
    out = display.copy()
    id_col = record_id_column(out)
    if not id_col:
        out["Assigned to me"] = "No"
        return out
    out["Assigned to me"] = out[id_col].astype(str).map(lambda rid: "Yes" if rid in assigned_ids else "No")
    return out


def _apply_assignment_display_options(
    display: pd.DataFrame,
    *,
    only_assigned: bool,
    assignments_first: bool,
    assigned_ids: set[str],
) -> pd.DataFrame:
    if display.empty:
        return display
    out = display.copy()
    id_col = record_id_column(out)
    if only_assigned and id_col and assigned_ids:
        out = out[out[id_col].astype(str).isin(assigned_ids)].copy()
    if assignments_first and "Assigned to me" in out.columns:
        out = out.assign(_sort_key=out["Assigned to me"].map(lambda v: 0 if v == "Yes" else 1))
        out = out.sort_values("_sort_key", kind="stable").drop(columns=["_sort_key"])
    return out


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
    from core.session_project import get_active_project, require_active_project, set_active_project

    page_header("Elvis_Review")

    role = user.get("ROLE") or user.get("role")
    actor = user.get("name") or user.get("EMAIL")
    project = require_active_project()
    if not project:
        return
    project_filter = [project]

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
            ensure_route_codes_for_project(project)
            st.session_state[cache_key] = True
        except Exception:
            pass

    trigger_project = st.session_state.pop("trigger_pipeline", None)
    if trigger_project:
        set_active_project(trigger_project)
        # Use immediately this run; sidebar picks it up on the next rerun.
        project = str(trigger_project)
        project_filter = [project]

    if run_sync:
        from pipeline.progress import PipelineProgress

        progress_bar = st.progress(0.0, text="Getting started…")
        progress_label = st.empty()

        def _on_progress(step: int, total: int, label: str) -> None:
            ratio = float(step) / float(total or 1)
            progress_bar.progress(min(max(ratio, 0.0), 1.0), text=label)
            progress_label.caption("Hang tight — larger projects can take a few minutes.")

        with st.spinner(
            f"Brewing a fresh Elvis Review for **{project}** — this can take a while, "
            "perfect time for coffee…"
        ):
            try:
                result = sync_and_export(
                    project,
                    phase="auto",
                    export=False,
                    progress=PipelineProgress(_on_progress, total=8),
                )
                progress_bar.progress(1.0, text="Your review queue is ready.")
                progress_label.caption("All set — new records are in the queue below.")
                st.success(f"{project}: {format_ingest_counts(result['counts'])}")
                notify_svc.notify(
                    actor,
                    notify_svc.SYNC_COMPLETED,
                    f"Fetched latest Elvis Review records for {project}",
                    project,
                )
                from core.streamlit_cache import bump_data_cache

                bump_data_cache()
            except Exception as exc:
                progress_label.caption("Something went wrong while refreshing.")
                st.error(f"{project} fetch failed: {exc}")

    is_manager = role in ("admin", "manager")
    tab_labels = ["My Queue"] + (["Assign records"] if is_manager else [])
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        section_title("Queue workspace")
        is_cleaning_role = role == "cleaning"
        assigned_ids: set[str] = set()
        if is_cleaning_role:
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

            if is_cleaning_role:
                st.markdown("**My assignments**")
                c3, c4 = st.columns(2)
                assignments_first = c3.checkbox(
                    "My assignments first",
                    value=False,
                    key="clean_assignments_first",
                )
                only_assigned = c4.checkbox(
                    "Only my assignments",
                    value=False,
                    key="clean_only_assigned",
                )
            else:
                only_assigned = False
                assignments_first = False

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
        if only_assigned and assigned_ids:
            records = records[records["RECORD_ID"].astype(str).isin(assigned_ids)].copy()

        if records.empty:
            st.info(
                _empty_queue_message(
                    all_records,
                    include_auto_approved,
                    suggestions_only,
                    only_assigned,
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
                if is_cleaning_role:
                    display = _add_assigned_to_me_column(display, assigned_ids)
                    display = _apply_assignment_display_options(
                        display,
                        only_assigned=False,
                        assignments_first=assignments_first,
                        assigned_ids=assigned_ids,
                    )
                display = apply_record_filters(display, key_prefix="clean")
                visible_records = subset_records_for_display(display, records)
                stats_bar(
                    _queue_stats(
                        all_records,
                        queue_records,
                        visible_records,
                        include_auto_approved,
                        assigned_count=len(assigned_ids) if is_cleaning_role else None,
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
                section_title("Open record details")
                _open_selected(display, visible_records, user, key="clean_open")

    if is_manager:
        with tabs[1]:
            _render_assign_panel(project_filter)


def _render_assign_panel(project_filter: list[str]) -> None:
    section_title("Assign new records to cleaners")
    records = _records_for_projects(project_filter, only_new=True)
    if records.empty:
        st.info("No new/unassigned records to assign.")
        return
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

    cleaners = assignment_svc.team_members("cleaning")
    c1, c2, c3 = st.columns(3)
    selected_ids = c1.multiselect(
        "Record IDs",
        options=display[id_col].astype(str).tolist() if id_col else [],
    )
    cleaner = c2.selectbox("Assign to", options=cleaners or ["(no cleaning users)"])
    priority = c3.number_input("Priority", min_value=1, max_value=1000, value=100)
    if st.button("Assign selected", type="primary", disabled=not (selected_ids and cleaners)):
        by_project: dict[str, list[str]] = {}
        for rid in selected_ids:
            match = records[records["RECORD_ID"].astype(str) == str(rid)]
            if match.empty:
                continue
            by_project.setdefault(match["PROJECT_NAME"].iloc[0], []).append(rid)
        total = 0
        for project, ids in by_project.items():
            assignment_svc.assign_records(project, ids, cleaner, team="cleaning", priority=int(priority))
            total += len(ids)
        notify_svc.notify(cleaner, notify_svc.NEW_ASSIGNMENT, f"You were assigned {total} record(s) to clean.")
        st.success(f"Assigned {total} record(s) to {cleaner}.")
        st.rerun()
