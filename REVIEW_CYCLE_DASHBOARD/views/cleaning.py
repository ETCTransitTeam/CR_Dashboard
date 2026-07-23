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
from rc_auth.access import (
    can_manage_cleaning_assignments,
    is_cleaning_head,
    is_super_admin_user,
)
from services import assignments as assignment_svc
from services import notifications as notify_svc
from views.assignment_manager import render_unassign_records_panel
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


def _auto_approved_mask(records: pd.DataFrame) -> pd.Series:
    """Use + Tosia / Field approved — already cleaned; never assign these."""
    if records.empty:
        return pd.Series(dtype=bool)
    usage = _usage_series(records)
    reviewer = _reviewer_series(records)
    is_use = usage.map(lambda v: str(v or "").strip().lower() == "use")
    excluded_reviewer = reviewer.map(lambda v: _norm_reviewer(v) in _CLEANING_EXCLUDED_REVIEWERS)
    return is_use & excluded_reviewer


def _assignable_for_cleaning_mask(records: pd.DataFrame) -> pd.Series:
    """Only blank Usage & Reviewer rows may be assigned (auto-approved are excluded)."""
    if records.empty:
        return pd.Series(dtype=bool)
    return _blank_decision_mask(records) & ~_auto_approved_mask(records)


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


def _user_assignee_aliases(user: dict) -> list[str]:
    """All strings that may appear in ASSIGNMENTS.ASSIGNED_TO for this user."""
    raw: list[str] = []
    for key in ("username", "name", "NAME", "DISPLAY_NAME", "EMAIL", "email"):
        value = user.get(key)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        text = str(value).strip()
        if text:
            raw.append(text)
    # Email local-part is often what COALESCE(USERNAME, EMAIL) falls back to.
    for text in list(raw):
        if "@" in text:
            local = text.split("@", 1)[0].strip()
            if local:
                raw.append(local)
    seen: set[str] = set()
    out: list[str] = []
    for text in raw:
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _assignments_for_user(user: dict, team: str = "cleaning", project_filter: list[str] | None = None) -> pd.DataFrame:
    """Load active assignments for this user (case-insensitive name/email match)."""
    aliases = {a.lower() for a in _user_assignee_aliases(user)}
    if not aliases:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    if project_filter:
        for project in project_filter:
            df = load_assignments(team=team, project_name=project)
            if not df.empty:
                frames.append(df)
    else:
        df = load_assignments(team=team)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()

    all_a = pd.concat(frames, ignore_index=True)
    if "ASSIGNED_TO" not in all_a.columns:
        return pd.DataFrame()
    mask = all_a["ASSIGNED_TO"].fillna("").astype(str).str.strip().str.lower().isin(aliases)
    matched = all_a.loc[mask].copy()
    if matched.empty:
        return matched
    if "ASSIGNMENT_ID" in matched.columns:
        matched = matched.drop_duplicates(subset=["ASSIGNMENT_ID"])
    return matched


def _assigned_record_ids(user: dict, project_filter: list[str]) -> set[str]:
    assignments = _assignments_for_user(user, team="cleaning", project_filter=project_filter)
    if assignments.empty or "RECORD_ID" not in assignments.columns:
        return set()
    return {_norm_record_id(rid) for rid in assignments["RECORD_ID"].tolist() if _norm_record_id(rid)}


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

    can_unassign = can_manage_cleaning_assignments(user)
    tab_labels = ["My Queue"]
    if can_assign:
        tab_labels.append("Assign records")
    if can_unassign:
        tab_labels.append("Unassign records")

    # st.tabs resets to the first tab on every widget rerun (e.g. changing Assign to).
    # Horizontal radio keeps the active section in session_state across reruns.
    tab_key = "elvis_review_section"
    if st.session_state.get(tab_key) not in tab_labels:
        st.session_state[tab_key] = tab_labels[0]
    active_tab = st.radio(
        "Elvis Review section",
        tab_labels,
        horizontal=True,
        key=tab_key,
        label_visibility="collapsed",
    )

    if active_tab == "My Queue":
        section_title("Queue workspace")
        with loading("Loading the cleaning queue..."):
            assigned_ids: set[str] = set()
            if is_regular_cleaner:
                assigned_ids = _assigned_record_ids(user, project_filter)

            records = _records_for_projects(project_filter, only_new=False)
            all_records = records.copy()
            records = _records_with_resolved_decisions(records)

        # Cleaners only ever see their own assignments — never the full project queue.
        if is_regular_cleaner:
            if not assigned_ids:
                st.info(
                    "No cleaning records are assigned to you for this project. "
                    "Ask a manager or cleaning head to assign work from **Assign records**."
                )
                return
            rid_series = records["RECORD_ID"].map(_norm_record_id)
            records = records[rid_series.isin(assigned_ids)].copy()
            # Drop any auto-approved rows that were wrongly assigned (already cleaned).
            leaked = int(_auto_approved_mask(records).sum()) if not records.empty else 0
            if leaked:
                records = records[~_auto_approved_mask(records)].copy()
                st.warning(
                    f"{leaked} assigned record(s) are auto-approved (Use + Tosia / Field approved) "
                    "and were hidden — they are already cleaned. Ask a manager to unassign them."
                )
            all_assigned = records.copy()

        with filter_panel("Queue filters", "Choose which records appear in the grid below."):
            st.markdown("**Record visibility**")
            if is_regular_cleaner:
                include_auto_approved = False
                suggestions_only = st.checkbox(
                    "Only transfer suggestions",
                    value=False,
                    key="clean_suggestions_only",
                )
            else:
                f1, f2 = st.columns(2)
                include_auto_approved = f1.checkbox(
                    "Show auto-approved (Use + Tosia / Field approved)",
                    value=False,
                    help="Hidden by default — those records are already cleaned and are not assignable.",
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

        if not is_regular_cleaner:
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

        if records.empty:
            if is_regular_cleaner and not all_assigned.empty:
                st.info(
                    _empty_queue_message(
                        all_assigned,
                        include_auto_approved,
                        suggestions_only,
                        only_assigned=True,
                        include_removed=include_removed,
                        only_removed=only_removed,
                        only_blank_decisions=only_blank_decisions,
                    )
                )
            else:
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
                if is_head or (not is_regular_cleaner):
                    with loading("Loading cleaning assignments..."):
                        assignee_map = _cleaning_assignee_map(project_filter)
                    display = _add_assigned_to_column(display, assignee_map)
                display = apply_record_filters(display, key_prefix="clean")
                visible_records = subset_records_for_display(display, records)
                stats_bar(
                    _queue_stats(
                        all_records if not is_regular_cleaner else all_assigned,
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
                    project_name=project_filter[0] if len(project_filter) == 1 else None,
                    history_actor_roles=["cleaning"] if is_cleaning_role else None,
                )

    elif active_tab == "Assign records" and can_assign:
        if is_head:
            _render_cleaning_head_assign_panel(project_filter, user=user)
        else:
            _render_assign_panel(project_filter, user=user)

    elif active_tab == "Unassign records" and can_unassign:
        render_unassign_records_panel(user, project)


def _records_by_selected_ids(
    records: pd.DataFrame, selected_ids: list[str]
) -> dict[str, list[str]]:
    wanted = {_norm_record_id(rid) for rid in selected_ids if _norm_record_id(rid)}
    if not wanted or records.empty:
        return {}
    view = records.copy()
    view["_RID"] = view["RECORD_ID"].map(_norm_record_id)
    view = view[view["_RID"].isin(wanted)]
    by_project: dict[str, list[str]] = {}
    for project, group in view.groupby("PROJECT_NAME", sort=False):
        # Preserve selection order when possible.
        order = {rid: i for i, rid in enumerate(selected_ids)}
        ids = sorted(
            {_norm_record_id(r) for r in group["_RID"].tolist() if _norm_record_id(r)},
            key=lambda r: order.get(r, 10**9),
        )
        if ids:
            by_project[str(project)] = ids
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


def _only_assignable_ids(records: pd.DataFrame, selected_ids: list[str]) -> list[str]:
    """Drop any selected IDs that are not blank-decision / are auto-approved."""
    wanted = {_norm_record_id(rid) for rid in selected_ids if _norm_record_id(rid)}
    if not wanted or records.empty:
        return []
    assignable = records[_assignable_for_cleaning_mask(records)].copy()
    if assignable.empty:
        return []
    ok = set(assignable["RECORD_ID"].map(_norm_record_id).tolist())
    return [rid for rid in selected_ids if _norm_record_id(rid) in ok]


def _assign_pool_counts(
    assignable_records: pd.DataFrame,
    assignee_map: dict[str, str],
) -> tuple[int, int, int, dict[str, int]]:
    """Return (pool, assigned, left, per_person counts) for assignable cleaning records."""
    pool_ids = [
        rid
        for rid in (_norm_record_id(x) for x in assignable_records["RECORD_ID"].tolist())
        if rid
    ]
    pool = len(pool_ids)
    assigned = sum(1 for rid in pool_ids if rid in assignee_map)
    left = pool - assigned
    per_person: dict[str, int] = {}
    for rid in pool_ids:
        who = str(assignee_map.get(rid) or "").strip()
        if not who:
            continue
        per_person[who] = per_person.get(who, 0) + 1
    return pool, assigned, left, per_person


def _is_unassigned_label(value) -> bool:
    text = str(value or "").strip().lower()
    return text in {"", "unassigned", "nan", "none", "<na>"}


def _visible_unassigned_ids(display: pd.DataFrame, unassigned_fallback: set[str] | list[str]) -> list[str]:
    id_col = record_id_column(display)
    if not id_col or display.empty:
        return []
    if "Assigned To" in display.columns:
        out: list[str] = []
        for rid, who in zip(display[id_col].tolist(), display["Assigned To"].tolist()):
            norm = _norm_record_id(rid)
            if norm and _is_unassigned_label(who):
                out.append(norm)
        return out
    allowed = {_norm_record_id(x) for x in unassigned_fallback if _norm_record_id(x)}
    return [
        rid
        for rid in (_norm_record_id(x) for x in display[id_col].tolist())
        if rid in allowed
    ]


def _render_assign_tracking(
    *,
    pool: int,
    assigned: int,
    left: int,
    per_person: dict[str, int],
    visible_left: int | None = None,
) -> None:
    """Progress strip for the Assign records tab."""
    items: list[tuple[str, str]] = [
        ("Assignable pool", str(pool)),
        ("Already assigned", str(assigned)),
        ("Left to assign", str(left)),
    ]
    if visible_left is not None:
        items.append(("Visible left", str(visible_left)))
    stats_bar(items)
    if left == 0 and pool > 0:
        st.success("All assignable records are currently assigned.")
    elif pool == 0:
        st.info("No assignable records in this project.")
    if per_person:
        ranked = sorted(per_person.items(), key=lambda kv: (-kv[1], kv[0].lower()))
        summary = " · ".join(f"{name}: {count}" for name, count in ranked)
        from views.ui import info_strip

        info_strip(f"Assigned by person — {summary}")


def _render_cleaning_head_assign_panel(
    project_filter: list[str], *, user: dict | None = None
) -> None:
    """Cleaning head: bulk-assign unassigned blank-decision records by count."""
    section_title("Assign records")
    st.caption(
        "Only records with blank Final Usage and blank Final Reviewer can be assigned. "
        "Auto-approved (Use + Tosia / Field approved) records are already cleaned and never appear here. "
        "Choose how many unassigned ones to give a cleaner."
    )

    with loading("Loading records available for assignment..."):
        records = _records_for_projects(project_filter, only_new=False)
        records = _records_with_resolved_decisions(records)
    if records.empty:
        st.info("No records in this project yet.")
        return
    records = records[_assignable_for_cleaning_mask(records)].copy()
    if records.empty:
        st.info(
            "No assignable records — need blank Final Usage and Final Reviewer "
            "(auto-approved records are excluded)."
        )
        return

    display = records_to_elvis_review(records)
    if display.empty:
        st.info("No records available to assign.")
        return

    with loading("Loading current cleaning assignments..."):
        assignee_map = _cleaning_assignee_map(project_filter)
    pool, already_assigned, left, per_person = _assign_pool_counts(records, assignee_map)
    queue_ids = [_norm_record_id(rid) for rid in records["RECORD_ID"].tolist()]
    unassigned_ids = [rid for rid in queue_ids if rid and rid not in assignee_map]

    display = _add_assigned_to_column(display, assignee_map)
    display = apply_record_filters(display, key_prefix="head_assign")
    visible_unassigned = _visible_unassigned_ids(display, unassigned_ids)

    _render_assign_tracking(
        pool=pool,
        assigned=already_assigned,
        left=left,
        per_person=per_person,
        visible_left=len(visible_unassigned),
    )
    st.dataframe(display, use_container_width=True, hide_index=True)

    cleaners = _assign_to_options(user)
    max_bulk_count = max(len(visible_unassigned), 1)
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
        disabled=not (visible_unassigned and cleaners),
        key="head_bulk_btn",
    )

    flash = st.session_state.pop("cleaning_head_assign_flash", None)
    if flash:
        st.success(flash)

    if bulk_clicked and visible_unassigned and cleaners:
        if not _guard_assignee(user, bulk_cleaner):
            st.error("You can only assign records to cleaning team members.")
            return
        target_ids = _only_assignable_ids(records, visible_unassigned[: int(bulk_count)])
        if not target_ids:
            st.error("None of the selected records are assignable (auto-approved / already decided).")
            return
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
            actor_name = notify_svc.actor_display_name(user)
            notify_svc.notify(
                bulk_cleaner,
                notify_svc.NEW_ASSIGNMENT,
                f"You were assigned {total} cleaning record(s) by {actor_name}.",
            )
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
        remaining = max(left - total, 1)
        st.session_state["_head_bulk_count_next"] = min(
            int(bulk_count),
            remaining,
        )
        set_operation_flash(
            f"Assigned {total} record(s) to {bulk_cleaner}. "
            f"{max(left - total, 0)} left to assign in the pool."
        )
        st.query_params["page"] = "review_cycle"
        st.rerun()


def _render_assign_panel(
    project_filter: list[str], *, user: dict | None = None
) -> None:
    section_title("Assign new records to cleaners")
    st.caption(
        "Only records with blank Final Usage and blank Final Reviewer can be assigned. "
        "Auto-approved (Use + Tosia / Field approved) records are already cleaned and never appear here. "
        "Filter the table, then choose how many visible unassigned records to assign "
        "(top of the list, in current order)."
    )
    with loading("Loading records available for assignment..."):
        records = _records_for_projects(project_filter, only_new=False)
        records = _records_with_resolved_decisions(records)
    if records.empty:
        st.info("No records in this project yet.")
        return

    records = records[_assignable_for_cleaning_mask(records)].copy()
    if records.empty:
        st.info(
            "No assignable records — need blank Final Usage and Final Reviewer "
            "(auto-approved records are excluded)."
        )
        return

    with loading("Loading current cleaning assignments..."):
        assignee_map = _cleaning_assignee_map(project_filter)
    pool, already_assigned, left, per_person = _assign_pool_counts(records, assignee_map)
    unassigned_set = {
        rid
        for rid in (_norm_record_id(x) for x in records["RECORD_ID"].tolist())
        if rid and rid not in assignee_map
    }

    display = records_to_elvis_review(records)
    if display.empty:
        st.info("No records available to assign.")
        return
    display = _add_assigned_to_column(display, assignee_map)
    display = apply_record_filters(display, key_prefix="assign")
    visible_unassigned = _visible_unassigned_ids(display, unassigned_set)

    _render_assign_tracking(
        pool=pool,
        assigned=already_assigned,
        left=left,
        per_person=per_person,
        visible_left=len(visible_unassigned),
    )
    # Table shows the full assignable pool (assigned + left) so progress is visible.
    st.dataframe(display, use_container_width=True, hide_index=True)

    if left == 0:
        return

    cleaners = _assign_to_options(user)
    max_count = max(len(visible_unassigned), 1)
    default_count = min(25, max_count)
    c1, c2, c3 = st.columns(3)
    assign_count = c1.number_input(
        "How many to assign",
        min_value=1,
        max_value=max_count,
        value=default_count,
        help="Assigns this many unassigned records from the filtered table (top rows first).",
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
    can_assign = bool(visible_unassigned and cleaners)
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
        selected_ids = _only_assignable_ids(records, visible_unassigned[: int(assign_count)])
        if not selected_ids:
            st.error("None of the selected records are assignable (auto-approved / already decided).")
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
            actor_name = notify_svc.actor_display_name(user)
            notify_svc.notify(
                cleaner,
                notify_svc.NEW_ASSIGNMENT,
                f"You were assigned {total} cleaning record(s) by {actor_name}.",
            )
        from core.streamlit_cache import bump_data_cache

        bump_data_cache()
        set_operation_flash(
            f"Assigned {total} record(s) to {cleaner}. "
            f"{max(left - total, 0)} left to assign in the pool."
        )
        st.rerun()
