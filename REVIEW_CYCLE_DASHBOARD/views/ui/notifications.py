"""Notification dropdown at header bell."""

from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from views.ui.loading import loading, set_operation_flash


MARK_ALL_BRIDGE_LABEL = "RCD_MARK_ALL_READ"
MARK_ALL_BRIDGE_KEY = "rcd_mark_all_bridge"

_NOTIFICATION_TYPE_LABELS = {
    "new_assignment": "New assignment",
    "assignment_released": "Unassigned",
    "review_completed": "Review completed",
    "admin_approval_required": "Admin approval",
    "sync_completed": "Sync completed",
    "data_quality_alert": "Data quality",
}


def _notification_type_label(ntype: str) -> str:
    key = str(ntype or "").strip().lower()
    if key in _NOTIFICATION_TYPE_LABELS:
        return _NOTIFICATION_TYPE_LABELS[key]
    text = key.replace("_", " ").strip()
    return text.title() if text else "Notice"


def _notification_recipient(user: dict) -> str:
    return str(user.get("name") or user.get("EMAIL") or user.get("email") or "").strip()


def _user_recipients(user: dict) -> list[str]:
    recipients: list[str] = []
    for key in ("name", "username", "EMAIL", "email", "DISPLAY_NAME"):
        value = str(user.get(key) or "").strip()
        if value and value not in recipients:
            recipients.append(value)
    return recipients


def _is_unread(value) -> bool:
    """Robust unread check for Snowflake BOOL / INT / string payloads."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, (bool, int, float)):
        return not bool(value)
    text = str(value).strip().lower()
    if text in {"", "0", "false", "n", "no", "f", "off"}:
        return True
    if text in {"1", "true", "y", "yes", "t", "on"}:
        return False
    return not bool(value)


def _list_user_notifications(user: dict, *, limit: int = 25) -> pd.DataFrame:
    """Recent notifications for every identity alias of this user."""
    from services import notifications as notify_svc

    frames: list[pd.DataFrame] = []
    for recipient in _user_recipients(user):
        try:
            frame = notify_svc.list_notifications(recipient, unread_only=False, limit=limit)
        except Exception:
            continue
        if frame is not None and not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    if "NOTIFICATION_ID" in combined.columns:
        combined = combined.drop_duplicates(subset=["NOTIFICATION_ID"], keep="first")
    if "CREATED_AT" in combined.columns:
        combined = combined.sort_values("CREATED_AT", ascending=False)
    return combined.head(limit)


def unread_count_for_user(user: dict) -> int:
    items = _list_user_notifications(user, limit=200)
    if items.empty or "IS_READ" not in items.columns:
        from services import notifications as notify_svc

        total = 0
        for recipient in _user_recipients(user):
            try:
                total += int(notify_svc.unread_count(recipient))
            except Exception:
                continue
        return total
    return int(sum(1 for _, row in items.iterrows() if _is_unread(row.get("IS_READ"))))


def handle_pending_notification_actions(user: dict) -> None:
    """Legacy query-param path (same tab only; no new-window links)."""
    flag = st.query_params.get("ref_notif_mark_all")
    if str(flag or "") != "1":
        return
    with loading("Marking notifications as read..."):
        _mark_all_read_for_user(user)
    params = {key: value for key, value in st.query_params.items() if key != "ref_notif_mark_all"}
    st.query_params.from_dict(params)
    set_operation_flash("All notifications marked as read.")
    st.rerun()


def _mark_all_read_for_user(user: dict) -> None:
    from core.streamlit_cache import bump_data_cache
    from services import notifications as notify_svc

    pending_ids: list[int] = []
    try:
        items = _list_user_notifications(user, limit=500)
        if not items.empty and "NOTIFICATION_ID" in items.columns:
            for _, row in items.iterrows():
                if _is_unread(row.get("IS_READ")):
                    try:
                        pending_ids.append(int(row["NOTIFICATION_ID"]))
                    except (TypeError, ValueError):
                        continue
    except Exception:
        pending_ids = []

    for recipient in _user_recipients(user):
        notify_svc.mark_all_read(recipient)
    for notification_id in pending_ids:
        try:
            notify_svc.mark_read(notification_id)
        except Exception:
            continue
    bump_data_cache()


def build_notification_panel_html(user: dict, version: int) -> str:
    del version
    if not _user_recipients(user):
        return '<div class="ref-notif-empty">No notifications.</div>'

    try:
        items = _list_user_notifications(user, limit=25)
    except Exception:
        return '<div class="ref-notif-empty">Could not load notifications.</div>'

    if items.empty:
        return '<div class="ref-notif-empty">No notifications.</div>'

    parts = [
        '<div class="ref-notif-panel-head">'
        "<span>Notifications</span>"
        '<button type="button" class="ref-notif-close" data-ref-notif-close '
        'aria-label="Close notifications">&times;</button>'
        "</div>"
    ]
    has_unread = any(_is_unread(row.get("IS_READ")) for _, row in items.iterrows())
    if has_unread:
        # Button only — never an <a href> (Streamlit/browser may open those in a new tab).
        parts.append(
            '<button type="button" class="ref-notif-mark-all" data-ref-notif-mark-all>'
            "Mark all as read</button>"
        )

    parts.append('<ul class="ref-notif-list">')
    for _, row in items.iterrows():
        unread = _is_unread(row.get("IS_READ"))
        cls = "ref-notif-item ref-notif-unread" if unread else "ref-notif-item"
        ntype = html.escape(_notification_type_label(str(row.get("NTYPE") or "")))
        message = html.escape(str(row.get("MESSAGE") or ""))
        bullet = "•" if unread else ""
        parts.append(
            f'<li class="{cls}">'
            f'<span class="ref-notif-bullet">{bullet}</span>'
            f"<span><strong>{ntype}:</strong> {message}</span></li>"
        )
    parts.append("</ul>")
    return "".join(parts)


def render_notification_actions(user: dict) -> None:
    """Hidden Streamlit button clicked by the header Mark-all control (same tab)."""
    if not _user_recipients(user):
        return
    st.markdown(
        f'<span id="ref-notif-actions" data-ref-mark-label="{html.escape(MARK_ALL_BRIDGE_LABEL)}"></span>',
        unsafe_allow_html=True,
    )
    if st.button(MARK_ALL_BRIDGE_LABEL, key=MARK_ALL_BRIDGE_KEY):
        with loading("Marking notifications as read..."):
            _mark_all_read_for_user(user)
        set_operation_flash("All notifications marked as read.")
        st.rerun()
