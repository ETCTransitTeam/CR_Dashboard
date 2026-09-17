"""Super-admin Sync History page: morning toggle, alerts, run history."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import streamlit as st

from od_sync_history import (
    KEY_ALERT_EMAILS,
    KEY_DROP_ALERT_PCT,
    KEY_EMAIL_ALERTS,
    ensure_od_sync_history_tables,
    get_drop_alert_pct,
    get_setting,
    get_setting_meta,
    is_email_alerts_enabled,
    is_morning_sync_enabled,
    list_recent_runs,
    list_run_projects,
    set_morning_sync_enabled,
    set_setting,
    summary_chips,
)

_CHI = ZoneInfo("America/Chicago")


def _format_chicago(value) -> str:
    """Format a timestamp for display in America/Chicago."""
    if value is None or value == "":
        return "—"
    if isinstance(value, str):
        text = value.strip()
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                value = datetime.strptime(text[:26], fmt)
                break
            except ValueError:
                continue
        else:
            return text
    if not isinstance(value, datetime):
        return str(value)

    if value.tzinfo is None:
        # History/settings writers store Chicago wall time as TIMESTAMP_NTZ.
        value = value.replace(tzinfo=_CHI)
    else:
        value = value.astimezone(_CHI)

    hour12 = value.hour % 12 or 12
    ampm = "AM" if value.hour < 12 else "PM"
    return f"{value.strftime('%b %d, %Y')} {hour12}:{value.strftime('%M')} {ampm} CT"


def _sync_history_styles() -> None:
    st.markdown(
        """
        <style>
        .sync-hist-hero h1 {
            margin: 0 0 4px 0;
            font-size: 1.75rem;
            font-weight: 700;
            color: #0f172a;
        }
        .sync-hist-hero p {
            margin: 0 0 1.25rem 0;
            color: #64748b;
            font-size: 0.95rem;
        }
        .sync-morning-panel {
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            padding: 1.1rem 1.25rem 0.85rem 1.25rem;
            margin-bottom: 1.25rem;
            background: linear-gradient(180deg, #f8fafc 0%, #ffffff 55%);
        }
        .sync-morning-panel.is-on {
            border-color: #86efac;
            background: linear-gradient(180deg, #f0fdf4 0%, #ffffff 60%);
        }
        .sync-morning-panel.is-off {
            border-color: #fecaca;
            background: linear-gradient(180deg, #fef2f2 0%, #ffffff 60%);
        }
        .sync-morning-kicker {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: #64748b;
            margin-bottom: 0.35rem;
        }
        .sync-morning-title {
            font-size: 1.15rem;
            font-weight: 700;
            color: #0f172a;
            margin: 0 0 0.35rem 0;
        }
        .sync-morning-desc {
            margin: 0;
            color: #475569;
            font-size: 0.92rem;
            line-height: 1.45;
            max-width: 52rem;
        }
        .sync-status-pill {
            display: inline-block;
            padding: 0.35rem 0.85rem;
            border-radius: 999px;
            font-weight: 700;
            font-size: 0.85rem;
            letter-spacing: 0.02em;
        }
        .sync-status-pill.on {
            background: #dcfce7;
            color: #166534;
            border: 1px solid #86efac;
        }
        .sync-status-pill.off {
            background: #fee2e2;
            color: #991b1b;
            border: 1px solid #fca5a5;
        }
        .sync-morning-meta {
            margin-top: 0.65rem;
            color: #64748b;
            font-size: 0.82rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def sync_history_page() -> None:
    ensure_od_sync_history_tables()
    actor = (st.session_state.get("user") or {}).get("email") or "unknown"
    _sync_history_styles()

    st.markdown(
        """
        <div class="sync-hist-hero">
            <h1>Sync History</h1>
            <p>Track morning and manual OD syncs, failures, and record-drop alerts.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    morning_on = is_morning_sync_enabled()
    morning_meta = get_setting_meta("morning_sync_enabled")
    panel_class = "is-on" if morning_on else "is-off"
    status_label = "ON" if morning_on else "OFF"
    status_class = "on" if morning_on else "off"
    changed_by = morning_meta.get("updated_by") or "—"
    changed_at = _format_chicago(morning_meta.get("updated_at"))

    st.markdown(
        f"""
        <div class="sync-morning-panel {panel_class}">
            <div class="sync-morning-kicker">Scheduled job control</div>
            <div class="sync-morning-title">Daily morning OD sync</div>
            <p class="sync-morning-desc">
                Controls the <strong>5:00 AM Chicago</strong> server job that syncs all active projects.
                When <strong>OFF</strong>, that job skips syncing and only logs a skipped run.
                The <strong>Sync</strong> button on a project still works either way.
            </p>
            <div style="margin-top:0.85rem;">
                <span class="sync-status-pill {status_class}">Currently {status_label}</span>
            </div>
            <div class="sync-morning-meta">
                Last changed by <strong>{changed_by}</strong> · {changed_at}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    toggle_col, action_col = st.columns([1.4, 2])
    with toggle_col:
        enabled = st.toggle(
            "Enable morning auto-sync",
            value=morning_on,
            key="sync_hist_morning_toggle",
            help="Turns the scheduled 5:00 AM Chicago sync on or off. Does not stop a run that is already in progress.",
        )
    with action_col:
        if enabled != morning_on:
            set_morning_sync_enabled(enabled, updated_by=actor)
            if enabled:
                st.success("Morning auto-sync turned ON.")
            else:
                st.success(
                    "Morning auto-sync turned OFF. "
                    "The next scheduled run will skip until you turn it back on."
                )
            st.rerun()
        else:
            st.caption(
                "Tip: flip the switch to change the schedule. A sync already running will finish normally."
            )

    with st.expander("Alert settings", expanded=False):
        email_on = st.toggle(
            "Email alerts",
            value=is_email_alerts_enabled(),
            key="sync_hist_email_toggle",
        )
        if email_on != is_email_alerts_enabled():
            set_setting(KEY_EMAIL_ALERTS, "true" if email_on else "false", updated_by=actor)
            st.rerun()

        current_emails = get_setting(KEY_ALERT_EMAILS, "")
        emails = st.text_input(
            "Alert recipients (comma-separated; blank = super-admin list)",
            value=current_emails,
            key="sync_hist_emails",
        )
        drop_pct = st.number_input(
            "Record-drop alert threshold (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(get_drop_alert_pct()),
            step=0.5,
            help="Email when dashboard-visible records fall by at least this percent "
            "vs the last successful sync for that project. Adjustable — not fixed.",
            key="sync_hist_drop_pct",
        )
        if st.button("Save alert settings", key="sync_hist_save_alerts"):
            set_setting(KEY_ALERT_EMAILS, emails.strip(), updated_by=actor)
            set_setting(KEY_DROP_ALERT_PCT, str(drop_pct), updated_by=actor)
            st.success("Alert settings saved.")
            st.rerun()

    chips = summary_chips(7)
    c1, c2, c3, c4 = st.columns(4)
    last = chips.get("last_run") or {}
    c1.metric("Morning auto-sync", "On" if chips.get("morning_enabled") else "Off")
    c2.metric("Last run", f"{last.get('status') or '—'}")
    c3.metric("Project failures (7d)", chips.get("project_fails", 0))
    c4.metric("Drop alerts (7d)", chips.get("drop_alerts", 0))
    if last.get("started_at"):
        st.caption(f"Last run started: {_format_chicago(last.get('started_at'))}")

    days = st.selectbox("Show runs from last", options=[7, 14, 30, 90], index=2, key="sync_hist_days")
    status_filter = st.multiselect(
        "Status filter",
        options=["success", "partial", "failed", "skipped", "running"],
        default=[],
        key="sync_hist_status",
    )
    trigger_filter = st.multiselect(
        "Trigger filter",
        options=["morning", "ui", "manual_cli"],
        default=[],
        key="sync_hist_trigger",
    )
    drops_only = st.checkbox("Only runs with drop alerts", value=False, key="sync_hist_drops_only")

    if st.button("Refresh", key="sync_hist_refresh"):
        st.rerun()

    runs = list_recent_runs(limit=200, days=int(days))
    if status_filter:
        runs = [r for r in runs if (r.get("STATUS") or "") in status_filter]
    if trigger_filter:
        runs = [r for r in runs if (r.get("TRIGGER") or "") in trigger_filter]

    if not runs:
        st.info("No sync runs recorded yet for this period.")
        return

    for run in runs:
        run_id = run.get("RUN_ID")
        projects = list_run_projects(run_id)
        if drops_only and not any(p.get("ALERT_DROP") for p in projects):
            continue
        title = (
            f"{_format_chicago(run.get('STARTED_AT'))} · {run.get('TRIGGER')} · "
            f"{run.get('STATUS')} · ok={run.get('PROJECT_OK')}/{run.get('PROJECT_TOTAL')}"
        )
        with st.expander(title, expanded=False):
            st.write(
                {
                    "run_id": run_id,
                    "finished": _format_chicago(run.get("FINISHED_AT")),
                    "host": run.get("HOST"),
                    "actor": run.get("ACTOR"),
                    "notes": run.get("NOTES"),
                    "failed": run.get("PROJECT_FAILED"),
                }
            )
            if not projects:
                st.caption("No project rows.")
                continue
            for p in projects:
                drop_flag = " ⚠ drop" if p.get("ALERT_DROP") else ""
                st.markdown(
                    f"**{p.get('PROJECT_NAME')}** — `{p.get('STATUS')}`"
                    f"{drop_flag} · {p.get('DURATION_SEC') or 0:.1f}s · "
                    f"visible={p.get('DASHBOARD_VISIBLE')} "
                    f"(prev={p.get('PREV_DASHBOARD_VISIBLE')}, "
                    f"drop%={p.get('DROP_PCT')})"
                )
                if p.get("ERROR_MESSAGE"):
                    st.error(p.get("ERROR_MESSAGE"))
                if p.get("ERROR_DETAIL"):
                    with st.expander("Error detail", expanded=False):
                        st.code(p.get("ERROR_DETAIL"), language="text")
