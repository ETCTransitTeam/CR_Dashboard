from __future__ import annotations

import pandas as pd
import streamlit as st

from core.config import APP_CONFIG_SCHEMA, REVIEW_CYCLE_SCHEMA, fq_table
from core.data_access import ensure_route_codes_for_project
from core.projects import get_sync_state, list_projects
from core.schema import bootstrap_database, ensure_migrations, refresh_projects, repair_timestamp_columns, schema_is_ready
from core.s3_utils import storage_mode_label
from core.snowflake_conn import snowflake_auth_mode, test_connection
from core.sync_watcher import render_sync_banner
from pipeline.ingest import export_kingelvis, format_ingest_counts, sync_and_export
from pipeline.runner import build_context, cleanup_workspace, run_post_cleaning_pipeline, stage_inputs
from services import notifications as notify_svc
from services import quality
from services import sync as sync_svc
from views.ui import (
    info_strip,
    loading,
    page_header,
    progress_status,
    section_title,
    set_operation_flash,
    stats_bar,
)


def _require_project(project: str | None) -> bool:
    if project:
        return True
    st.error("Select a project first, or refresh projects from APP_CONFIG (see below).")
    return False


def _state_value(value) -> str:
    text = str(value) if value is not None else ""
    return text if text and text.lower() != "none" else "Not available"


def _render_project_sync_state(state: dict) -> None:
    status = _state_value(state.get("LAST_PIPELINE_STATUS"))
    message = _state_value(state.get("LAST_PIPELINE_MESSAGE"))
    stats_bar([
        ("Last pull", _state_value(state.get("LAST_PULL_TS"))),
        ("OD sync seen", _state_value(state.get("LAST_OD_SYNC_SEEN"))),
        ("KingElvis export", _state_value(state.get("LAST_KINGELVIS_EXPORT_TS"))),
    ])

    if status.lower() == "success":
        st.success(f"Pipeline status: {status}")
    elif status == "Not available":
        st.info("Pipeline status has not been recorded yet.")
    else:
        st.warning(f"Pipeline status: {status}")
    st.caption(f"Last message: {message}")


def _render_refresh_summary(summary: dict) -> None:
    st.success("Morning refresh completed.")
    rows = [{"Item": str(key), "Value": _state_value(value)} for key, value in summary.items()]
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render_sync_admin_page(user: dict) -> None:
    from core.config import REVIEW_CYCLE_SCHEMA

    page_header(
        "Sync & Pipeline Admin",
        f"Schema: {REVIEW_CYCLE_SCHEMA} | KingElvis: {storage_mode_label()} | Snowflake auth: {snowflake_auth_mode()}",
    )
    actor = user.get("name") or user.get("EMAIL")

    with loading("Loading available projects..."):
        projects = list_projects()
    project_names = projects["PROJECT_NAME"].tolist() if not projects.empty else []

    if not project_names:
        st.warning(
            f"No projects in `{fq_table('PROJECTS')}`. "
            f"Click **Refresh projects from APP_CONFIG** to load from `{fq_table('PROJECT_CONFIGS', APP_CONFIG_SCHEMA)}`."
        )
        if st.button("Refresh projects from APP_CONFIG", type="primary"):
            try:
                with loading("Refreshing projects from APP_CONFIG..."):
                    count = refresh_projects()
                if count == 0:
                    st.warning("Query succeeded but returned 0 active projects.")
                else:
                    set_operation_flash(f"Loaded {count} project(s).")
                    st.rerun()
            except Exception as exc:
                st.error(str(exc))

    section_title("Project status")
    from core.session_project import require_active_project

    project = require_active_project()
    with st.container(border=True):
        if project_names and project:
            st.caption(f"Active project (sidebar): **{project}**")
        elif not project_names:
            st.caption("No projects available yet.")
            project = None

        if project:
            render_sync_banner(project)
            with loading("Loading project sync status..."):
                state = get_sync_state(project)
            if state:
                _render_project_sync_state(state)
            else:
                st.info("No sync state has been recorded for this project yet.")

    st.divider()
    section_title("Weekly orchestration")
    with st.container(border=True):
        st.markdown("**Pipeline operations**")
        st.caption("Run the core weekly workflow, review checks, exports, and quality jobs for the **active sidebar project**.")
        c1, c2, c3 = st.columns(3)
        with c1:
            with st.container(border=True):
                st.markdown("**Weekly cycle**")
                st.caption("Full auto-approval + flags into master records (all pages).")
                if st.button(
                    "Run weekly cycle",
                    type="primary",
                    disabled=not project,
                    help="Full auto-approval chain including Combined Checks flags",
                    use_container_width=True,
                ):
                    if _require_project(project):
                        try:
                            with progress_status(
                                f"Running weekly cycle for {project}...",
                                complete_label="Weekly cycle complete",
                            ) as update:
                                result = sync_and_export(project, phase="auto", export=False, progress=update)
                                update(1, 1, "Sending weekly cycle notification...")
                                notify_svc.notify(actor, notify_svc.SYNC_COMPLETED, f"Weekly cycle complete for {project}", project)
                            st.success(format_ingest_counts(result["counts"]))
                        except Exception as exc:
                            st.error(str(exc))
        with c2:
            with st.container(border=True):
                st.markdown("**Review flags**")
                st.caption("Generate flags and combined checks.")
                if st.button(
                    "Generate review flags",
                    disabled=not project,
                    help="Flag chain -> FLAGS / COMBINED_CHECKS",
                    use_container_width=True,
                ):
                    if _require_project(project):
                        try:
                            with progress_status(
                                f"Generating review flags for {project}...",
                                complete_label="Review flags and quality alerts complete",
                            ) as update:
                                result = sync_and_export(project, phase="flags", export=False, progress=update)
                                update(1, 2, "Computing quality alerts...")
                                alerts = quality.compute_quality_alerts(project)
                                update(2, 2, "Notifying administrators about quality alerts...")
                                for admin in notify_svc.admins():
                                    notify_svc.notify(
                                        admin,
                                        notify_svc.DATA_QUALITY_ALERT,
                                        f"{len(alerts)} quality alert(s) for {project}",
                                        project,
                                    )
                            st.success(format_ingest_counts(result["counts"]))
                        except Exception as exc:
                            st.error(str(exc))
        with c3:
            with st.container(border=True):
                st.markdown("**Removed IDs**")
                st.caption("Create the field-team removed/deleted export.")
                if st.button("Generate removed IDs", disabled=not project, help="Removed_ids_field_team.py", use_container_width=True):
                    if _require_project(project):
                        ctx = None
                        try:
                            with progress_status(
                                f"Generating removed IDs for {project}...",
                                complete_label="Removed IDs export ready",
                            ) as update:
                                ctx = build_context(project)
                                outputs = run_post_cleaning_pipeline(ctx, progress=update)
                                update.set_total(update.total + 1)
                                update.advance("Preparing removed IDs download...")
                                removed_path = outputs.get("removed_ids_xlsx")
                                if removed_path and removed_path.exists():
                                    with open(removed_path, "rb") as handle:
                                        st.download_button(
                                            "Download Removed/Deleted Records",
                                            data=handle.read(),
                                            file_name=removed_path.name,
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            use_container_width=True,
                                        )
                                else:
                                    st.warning("Removed IDs file was not generated.")
                        except Exception as exc:
                            st.error(str(exc))
                        finally:
                            if ctx is not None:
                                cleanup_workspace(ctx)

        c4, c5, c6 = st.columns(3)
        with c4:
            with st.container(border=True):
                st.markdown("**KingElvis export**")
                st.caption("Export the current review file.")
                if st.button("Export KingElvis", disabled=not project, use_container_width=True):
                    if _require_project(project):
                        try:
                            with loading(f"Exporting KingElvis review file for {project}..."):
                                location = export_kingelvis(project)
                            st.success(f"Exported to {location}")
                        except Exception as exc:
                            st.error(str(exc))
        with c5:
            with st.container(border=True):
                st.markdown("**Quality alerts**")
                st.caption("Recompute data-quality alert rows.")
                if st.button("Recompute quality alerts", disabled=not project, use_container_width=True):
                    if _require_project(project):
                        try:
                            with progress_status(
                                f"Recomputing quality alerts for {project}...",
                                complete_label="Quality alerts recomputed",
                            ) as update:
                                update(1, 2, "Computing quality alert rows...")
                                alerts = quality.compute_quality_alerts(project)
                                update(2, 2, "Notifying administrators...")
                                for admin in notify_svc.admins():
                                    notify_svc.notify(admin, notify_svc.DATA_QUALITY_ALERT, f"{len(alerts)} quality alert(s) for {project}", project)
                            st.success(f"{len(alerts)} alert(s) computed.")
                        except Exception as exc:
                            st.error(str(exc))
        with c6:
            with st.container(border=True):
                st.markdown("**Demographics**")
                st.caption("Run configured demographic checks.")
                if st.button(
                    "Run demographic checks",
                    disabled=not project,
                    help="Configured demographic rules -> DEMOGRAPHIC_CHECKS",
                    use_container_width=True,
                ):
                    if _require_project(project):
                        try:
                            with loading(f"Running configured demographic checks for {project}..."):
                                demo_df = quality.generate_demographic_checks_from_review(project)
                            st.success(f"Ingested {len(demo_df)} demographic check row(s).")
                        except Exception as exc:
                            st.error(str(exc))

    st.divider()
    section_title("Maintenance actions")
    backfill_card, refresh_card = st.columns(2)
    with backfill_card:
        with st.container(border=True):
            st.markdown("**Route code backfill**")
            st.caption(
                "Copies route codes from the staged Elvis export into existing records when pipeline CSVs "
                "leave `ROUTE_SURVEYEDCode` empty."
            )
            if st.button(
                "Backfill route codes",
                disabled=not project,
                use_container_width=True,
                help="Backfill ROUTE_SURVEYEDCode from Elvis export",
            ):
                if _require_project(project):
                    ctx = None
                    try:
                        with progress_status(
                            f"Backfilling route codes for {project}...",
                            complete_label="Route code backfill complete",
                        ) as update:
                            update(1, 3, "Preparing pipeline workspace...")
                            ctx = build_context(project)
                            update(2, 3, "Staging the Elvis export...")
                            stage_inputs(ctx)
                            update(3, 3, "Backfilling route codes...")
                            count = ensure_route_codes_for_project(project)
                        st.success(f"Updated {count} record(s) with route codes.")
                    except Exception as exc:
                        st.error(str(exc))
                    finally:
                        if ctx is not None:
                            cleanup_workspace(ctx)

    with refresh_card:
        with st.container(border=True):
            st.markdown("**Scheduled refresh**")
            st.caption(
                "Checks OD freshness and pulls every project whose OD Collection `last_survey_date` "
                "changed since last seen."
            )
            if st.button("Run morning refresh", disabled=not project_names, use_container_width=True):
                try:
                    with progress_status(
                        "Checking OD freshness across projects...",
                        complete_label="Morning refresh complete",
                    ) as update:
                        summary = sync_svc.morning_refresh(progress=update)
                    _render_refresh_summary(summary)
                except Exception as exc:
                    st.error(str(exc))

    st.divider()
    section_title("Bootstrap")
    with st.expander("Advanced database and setup tools", expanded=False):
        st.caption("Use these tools only for setup, troubleshooting, or refreshing project configuration.")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Test Snowflake connection", use_container_width=True):
                try:
                    with loading("Testing the Snowflake connection..."):
                        connection_result = test_connection()
                    st.success(connection_result)
                except Exception as exc:
                    st.error(str(exc))
            if st.button("Initialize / migrate database schema", use_container_width=True):
                if schema_is_ready():
                    st.info(f"Schema `{REVIEW_CYCLE_SCHEMA}` already exists. Re-running is safe (uses IF NOT EXISTS).")
                try:
                    with progress_status(
                        "Initializing Review Cycle database...",
                        complete_label="Database schema is ready",
                    ) as update:
                        update(1, 2, "Bootstrapping database objects...")
                        result = bootstrap_database()
                        update(2, 2, "Applying database migrations...")
                        ensure_migrations()
                    set_operation_flash(str(result))
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        with c2:
            if st.button("Repair timestamp columns", help="Repair UPDATED_AT / CREATED_AT", use_container_width=True):
                try:
                    with loading("Repairing timestamp column types..."):
                        repaired = repair_timestamp_columns()
                    if repaired:
                        st.success(f"Repaired: {', '.join(repaired)}")
                    else:
                        st.success("All timestamp columns already have the correct type.")
                    st.session_state["timestamp_columns_repaired"] = True
                except Exception as exc:
                    st.error(str(exc))
            if st.button("Refresh projects from APP_CONFIG", key="refresh_projects_btn", use_container_width=True):
                try:
                    with loading("Refreshing projects from APP_CONFIG..."):
                        count = refresh_projects()
                    set_operation_flash(f"Loaded {count} project(s).")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
