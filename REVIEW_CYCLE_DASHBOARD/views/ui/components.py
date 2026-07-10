"""Reference design presentation components (no business logic)."""

from __future__ import annotations

import html
import re
from contextlib import contextmanager
from typing import Iterator

import streamlit as st

from views.ui.icons import icon as svg_icon

ROLE_LABELS = {
    "cleaning": "Cleaning Team",
    "review": "Review Team",
    "field": "Field Team",
    "manager": "Manager",
    "admin": "Admin",
    "super_admin": "Super Admin",
}

_HEADER_CTX: dict[str, object] = {}

_METRIC_BY_LABEL: dict[str, tuple[str, str, str]] = {
    "total records": ("file-text", "blue", "All time total"),
    "cleaned (use)": ("circle-check", "orange", "Current count"),
    "reviewed": ("shield-check", "green", "Verified total"),
    "removed": ("trash-2", "purple", "Active records"),
    "pending": ("clock", "cyan", "Open items"),
    "quality score": ("star", "blue", "Project health index"),
    "removal rate": ("rotate-ccw", "orange", "Share of removed records"),
}

_DEFAULT_METRIC = ("layers", "blue", "Metric")

_SPARK_PATHS = (
    "M0,14 L4,11 L8,12 L12,8 L16,10 L20,6 L24,8",
    "M0,12 L4,9 L8,11 L12,6 L16,8 L20,4 L24,6",
    "M0,10 L4,12 L8,8 L12,10 L16,5 L20,7 L24,4",
    "M0,8 L4,11 L8,6 L12,9 L16,4 L20,6 L24,5",
    "M0,14 L4,7 L8,10 L12,8 L16,11 L20,5 L24,7",
)

_HERO_WAVES = (
    '<svg class="ref-header-wave" viewBox="0 0 1440 160" preserveAspectRatio="none" aria-hidden="true">'
    '<path d="M0,72 C180,28 360,120 540,64 C720,8 900,112 1080,56 C1260,0 1350,88 1440,48 L1440,0 L0,0 Z" fill="currentColor"/>'
    '<path d="M0,110 C220,70 440,140 660,96 C880,52 1100,130 1320,86 C1380,74 1410,90 1440,82 L1440,0 L0,0 Z" fill="currentColor" opacity="0.55"/>'
    '</svg>'
)


def set_header_context(
    *,
    user_name: str = "",
    role: str = "",
    unread: int = 0,
    user: dict | None = None,
    email: str = "",
) -> None:
    global _HEADER_CTX
    _HEADER_CTX = {
        "user_name": user_name,
        "role": role,
        "unread": unread,
        "user": user,
        "email": email,
    }


def _header_context() -> dict[str, object]:
    return _HEADER_CTX


def _render_html(markup: str) -> None:
    # Main DOM (not st.html iframe) so global CSS + Lucide apply to icons/sparklines.
    st.markdown(f'<div class="ref-ui-root">{markup}</div>', unsafe_allow_html=True)


def _metric_meta(label: str) -> tuple[str, str, str]:
    return _METRIC_BY_LABEL.get(label.strip().lower(), _DEFAULT_METRIC)


def _spark_svg(tone: str, idx: int) -> str:
    path = _SPARK_PATHS[idx % len(_SPARK_PATHS)]
    return (
        f'<svg class="ref-spark ref-spark-{tone}" viewBox="0 0 24 16" aria-hidden="true">'
        f'<path d="{path}" fill="none" stroke="currentColor" stroke-width="1.75" '
        f'stroke-linecap="round" stroke-linejoin="round"/></svg>'
    )


def _role_label(role: str) -> str:
    return ROLE_LABELS.get((role or "").lower(), (role or "User").replace("_", " ").title())


def _crumb_role(role: str) -> str:
    key = (role or "").lower()
    if key in {"admin", "super_admin"}:
        return "Admin"
    return _role_label(role)


# ── Sidebar ────────────────────────────────────────────────────────────────


def sidebar_brand() -> None:
    _render_html(
        '<header class="ref-brand">'
        '<div class="ref-brand-logo">RC</div>'
        '<div class="ref-brand-text">'
        '<div class="ref-brand-title">Review Cycle</div>'
        '<div class="ref-brand-sub">Enterprise QA Workspace</div>'
        "</div></header>"
    )


def user_card(name: str, email: str, role: str) -> None:
    """Kept for auth/other surfaces; main app shows the user in the header."""
    display = name or email or "Signed in"
    role_label = ROLE_LABELS.get((role or "").lower(), (role or "User").replace("_", " ").title())
    initial = (display or "?")[0].upper()
    email_html = f'<div class="ref-user-email">{html.escape(email)}</div>' if email else ""
    _render_html(
        f'<section class="ref-user-card">'
        f'<div class="ref-user-avatar-lg">{html.escape(initial)}</div>'
        f'<div class="ref-user-info">'
        f'<div class="ref-user-name">{html.escape(display)}</div>'
        f"{email_html}"
        f'<span class="ref-user-role">{html.escape(role_label)}</span>'
        f"</div></section>"
    )


def sidebar_nav_label() -> None:
    _render_html('<div class="ref-nav-heading">Navigation</div>')


def sidebar_account_label() -> None:
    _render_html('<div class="ref-nav-heading ref-nav-heading-account">Account</div>')


# ── Header / Hero ──────────────────────────────────────────────────────────


def app_topbar(title: str, user_name: str, role: str, *, unread: int = 0) -> None:
    set_header_context(user_name=user_name, role=role, unread=unread)


def page_header(
    title: str,
    subtitle: str | None = None,
    *,
    eyebrow: str | None = None,
    badge: str | None = None,
    user_name: str | None = None,
    role: str | None = None,
    unread: int | None = None,
) -> None:
    ctx = _header_context()
    display = user_name if user_name is not None else str(ctx.get("user_name") or "Signed in")
    role_val = role if role is not None else str(ctx.get("role") or "")
    unread_val = unread if unread is not None else int(ctx.get("unread") or 0)
    crumb_role = _crumb_role(role_val)
    initial = (display or "?")[0].upper()
    user = _HEADER_CTX.get("user")
    email = ""
    if isinstance(user, dict):
        email = str(user.get("email") or user.get("EMAIL") or ctx.get("email") or "").strip()
    elif ctx.get("email"):
        email = str(ctx.get("email") or "").strip()

    notif_badge = f'<span class="ref-bell-badge">{unread_val}</span>' if unread_val else ""
    badge_html = f'<span class="ref-hero-badge">{html.escape(badge)}</span>' if badge else ""
    subtitle_html = f'<p class="ref-hero-sub">{html.escape(subtitle)}</p>' if subtitle else ""
    eyebrow_html = f'<p class="ref-hero-eyebrow">{html.escape(eyebrow)}</p>' if eyebrow else ""
    role_label = _role_label(role_val)
    menu_email = html.escape(email) if email else "—"
    menu_role = html.escape(role_label)

    notif_panel = ""
    if isinstance(user, dict) and user:
        from views.ui.notifications import build_notification_panel_html

        try:
            notif_panel = build_notification_panel_html(user, 0)
        except Exception:
            notif_panel = '<div class="ref-notif-empty">Could not load notifications.</div>'

    empty_notif = '<div class="ref-notif-empty">No notifications.</div>'
    _render_html(
        f'<section class="ref-page-shell">'
        f'<div class="ref-app-header">'
        f'<nav class="ref-navbar-crumb" aria-label="Breadcrumb">'
        f"<span>Dashboard</span><span class=\"ref-crumb-sep\">/</span>"
        f"<span>{html.escape(crumb_role)}</span><span class=\"ref-crumb-sep\">/</span>"
        f"<span class=\"ref-crumb-current\">{html.escape(title)}</span>"
        f"</nav>"
        f'<div class="ref-navbar-tools">'
        f'<span class="ref-pill ref-pill-live">'
        f'<span class="ref-dot-live"></span>Live Workspace</span>'
        f'<span class="ref-bell-wrap" id="ref-notif-wrap">'
        f'<button type="button" class="ref-bell-btn" id="ref-notif-anchor" '
        f'aria-haspopup="true" aria-expanded="false" aria-controls="ref-notif-panel" title="Notifications">'
        f'{svg_icon("bell", "ref-icon-sm")}{notif_badge}'
        f"</button>"
        f'<div class="ref-notif-panel" id="ref-notif-panel" role="dialog" aria-label="Notifications">'
        f"{notif_panel or empty_notif}</div>"
        f"</span>"
        f'<div class="ref-user-menu" id="ref-user-menu">'
        f'<button type="button" class="ref-user-chip" id="ref-user-anchor" '
        f'aria-haspopup="true" aria-expanded="false" aria-controls="ref-user-panel" '
        f'title="{html.escape(display)}">'
        f'<span class="ref-avatar-sm">{html.escape(initial)}</span>'
        f'<span class="ref-user-chip-name">{html.escape(display)}</span>'
        f'<span class="ref-user-chip-chevron">{svg_icon("chevron-down", "ref-icon-xs")}</span>'
        f"</button>"
        f'<div class="ref-user-panel" id="ref-user-panel" role="menu" aria-label="Account">'
        f'<div class="ref-user-panel-role">{menu_role}</div>'
        f'<div class="ref-user-panel-email">{menu_email}</div>'
        f"</div>"
        f"</div>"
        f"</div></div>"
        f'<div class="ref-hero-row">'
        f'<div class="ref-hero-copy">'
        f"{eyebrow_html}"
        f'<div class="ref-hero-title-wrap">'
        f'<h1 class="ref-hero-title">{html.escape(title)}</h1>'
        f"{badge_html}"
        f"</div>"
        f"{subtitle_html}"
        f"</div>"
        f"</div></section>"
    )
    if isinstance(user, dict) and user:
        from views.ui.notifications import render_notification_actions

        render_notification_actions(user)


def page_actions() -> None:
    page_header("")


# ── Metric cards ─────────────────────────────────────────────────────────────


def _card_value_class(value: str) -> str:
    """Use a wrapping text style for long non-numeric metric values (e.g. project names)."""
    text = str(value or "").strip()
    if not text:
        return "ref-card-value"
    if re.fullmatch(r"[\d,]+(?:\.\d+)?%?", text):
        return "ref-card-value"
    if re.fullmatch(r"[\d,]+\s*/\s*[\d,]+", text):
        return "ref-card-value"
    if len(text) > 10:
        return "ref-card-value ref-card-value-text"
    return "ref-card-value"


def _summary_card_html(
    label: str, value: str, hint: str, icon_name: str, tone: str, idx: int, trend: str = "",
) -> str:
    value_cls = _card_value_class(value)
    return (
        f'<article class="ref-card ref-card-summary ref-tone-{tone}">'
        f'<div class="ref-card-icon">{svg_icon(icon_name, "ref-icon-md")}</div>'
        f'<div class="ref-card-label">{html.escape(label)}</div>'
        f'<div class="{value_cls}" title="{html.escape(value)}">{html.escape(value)}</div>'
        f'<div class="ref-card-hint">{html.escape(hint)}</div>'
        f'<div class="ref-card-spark">{_spark_svg(tone, idx)}</div>'
        f"</article>"
    )


def _analytics_card_html(
    label: str, value: str, hint: str, icon_name: str, tone: str, idx: int, trend: str = "",
) -> str:
    progress_html = ""
    ratio = re.match(r"^\s*([\d,]+)\s*/\s*([\d,]+)\s*$", value)
    if ratio:
        try:
            num = float(ratio.group(1).replace(",", ""))
            den = float(ratio.group(2).replace(",", ""))
            pct = min(100, max(0, (num / den * 100) if den else 0))
            progress_html = (
                f'<div class="ref-card-progress">'
                f'<div class="ref-card-progress-fill ref-tone-fill-{tone}" style="width:{pct:.1f}%"></div>'
                f"</div>"
            )
        except ValueError:
            progress_html = ""

    trend_html = ""
    if trend:
        low = trend.lower()
        if low.startswith(("+", "up", "↑")):
            cls, ic = "ref-card-delta ref-card-delta-up", "trending-up"
        elif low.startswith(("-", "down", "↓")):
            cls, ic = "ref-card-delta ref-card-delta-down", "trending-down"
        else:
            cls, ic = "ref-card-delta", "minus"
        trend_html = (
            f'<div class="{cls}">'
            f'{svg_icon(ic, "ref-icon-xs")}'
            f"<span>{html.escape(trend)}</span></div>"
        )

    value_cls = _card_value_class(value)
    return (
        f'<article class="ref-card ref-card-analytics ref-tone-{tone}">'
        f'<div class="ref-card-icon">{svg_icon(icon_name, "ref-icon-md")}</div>'
        f'<div class="ref-card-label">{html.escape(label)}</div>'
        f'<div class="{value_cls}" title="{html.escape(value)}">{html.escape(value)}</div>'
        f'<div class="ref-card-hint">{html.escape(hint)}</div>'
        f"{trend_html}"
        f"{progress_html}"
        f"</article>"
    )


def metric_card(
    label: str, value: str, *, hint: str = "", icon: str = "file-text",
    tone: str = "blue", trend: str = "", idx: int = 0,
) -> None:
    if trend or "/" in value:
        _render_html(_analytics_card_html(label, value, hint or "", icon, tone, idx, trend))
    else:
        _render_html(_summary_card_html(label, value, hint or "", icon, tone, idx))


def metric_row(
    items: list[tuple[str, ...]], *, columns: int | None = None, variant: str = "summary",
) -> None:
    if not items:
        return
    count = min(len(items), columns or 5)
    cards = []
    for idx, item in enumerate(items[:count]):
        label = str(item[0])
        value = str(item[1])
        subtitle = str(item[2]) if len(item) > 2 and item[2] else ""
        trend = str(item[3]) if len(item) > 3 and item[3] else ""
        icon, tone, default_hint = _metric_meta(label)
        if len(item) > 4 and item[4]:
            icon = str(item[4])
        if len(item) > 5 and item[5]:
            tone = str(item[5])
        hint = subtitle or default_hint
        if variant == "analytics":
            cards.append(_analytics_card_html(label, value, hint, icon, tone, idx, trend))
        else:
            cards.append(_summary_card_html(label, value, hint, icon, tone, idx, trend))
    grid = f"ref-grid ref-grid-{count}"
    _render_html(f'<div class="ref-grid-wrap"><div class="{grid}">{"".join(cards)}</div></div>')


def stats_bar(items: list[tuple[str, ...]]) -> None:
    metric_row(items)


# ── Section headers ──────────────────────────────────────────────────────────


def section_header(title: str, *, description: str | None = None) -> None:
    desc_html = f'<p class="ref-section-desc">{html.escape(description)}</p>' if description else ""
    _render_html(
        f'<div class="ref-section-block">'
        f'<div class="ref-section ref-section-inline">'
        f'<span class="ref-section-bar"></span>'
        f'<span class="ref-section-text">{html.escape(title)}</span>'
        f"</div>"
        f"{desc_html}"
        f"</div>"
    )


def section_header_compact(title: str) -> None:
    _render_html(
        f'<div class="ref-section ref-section-inline">'
        f'<span class="ref-section-bar"></span>'
        f'<span class="ref-section-text">{html.escape(title)}</span>'
        f"</div>"
    )


def section_title(title: str, *, description: str | None = None) -> None:
    section_header(title, description=description)


def table_section_chrome(title: str, *, row_count: int | None = None) -> None:
    _render_html(
        f'<div class="ref-table-head">'
        f'<div class="ref-section ref-section-inline">'
        f'<span class="ref-section-bar"></span>'
        f'<span class="ref-section-text">{html.escape(title)}</span>'
        f"</div>"
        f'<div class="ref-table-tools">'
        f'<label class="ref-search" aria-hidden="true">'
        f'{svg_icon("search", "ref-icon-sm")}'
        f'<span>Search projects...</span></label>'
        f'<span class="ref-tool-btn">'
        f'{svg_icon("filter", "ref-icon-sm")}Filters</span>'
        f'<span class="ref-tool-btn ref-tool-btn-icon" title="Download">'
        f'{svg_icon("download", "ref-icon-sm")}</span>'
        f"</div></div>"
    )


def table_section_footer(*, shown: int, total: int, label: str = "projects") -> None:
    _render_html(
        f'<div class="ref-table-foot">'
        f'<span class="ref-table-foot-info">Showing 1 to {shown} of {total} {html.escape(label)}</span>'
        f'<div class="ref-pagination" aria-hidden="true">'
        f'<span class="ref-page-arrow ref-page-arrow-off">'
        f'{svg_icon("chevron-left", "ref-icon-xs")}</span>'
        f'<span class="ref-page-active">1</span>'
        f'<span class="ref-page-arrow ref-page-arrow-off">'
        f'{svg_icon("chevron-right", "ref-icon-xs")}</span>'
        f"</div></div>"
    )


def drill_section_label() -> None:
    section_header("Drill Into a Project")


def form_field_label(text: str) -> None:
    _render_html(f'<label class="ref-field-label">{html.escape(text)}</label>')


# ── Surfaces ─────────────────────────────────────────────────────────────────


def dashboard_card(content_html: str = "") -> None:
    if content_html:
        _render_html(f'<div class="ref-surface">{content_html}</div>')


def badge(text: str, *, tone: str = "neutral") -> None:
    _render_html(f'<span class="ref-badge ref-badge-{tone}">{html.escape(text)}</span>')


def info_strip(text: str) -> None:
    _render_html(
        f'<div class="ref-notice">'
        f'{svg_icon("info", "ref-icon-sm")}'
        f"<span>{html.escape(text)}</span></div>"
    )


def empty_state(title: str, detail: str, icon: str = "inbox") -> None:
    icon_key = icon if icon and icon.isascii() and " " not in icon else "inbox"
    _render_html(
        f'<div class="ref-empty">'
        f'<div class="ref-empty-icon">{svg_icon(icon_key, "ref-icon-lg")}</div>'
        f'<div class="ref-empty-title">{html.escape(title)}</div>'
        f'<div class="ref-empty-copy">{html.escape(detail)}</div>'
        f"</div>"
    )


# ── Auth ─────────────────────────────────────────────────────────────────────


def auth_hero(title: str = "Review Cycle Dashboard", subtitle: str | None = None) -> None:
    _render_html(
        f'<div class="ref-auth">'
        f'<div class="ref-auth-mark">RC</div>'
        f'<div class="ref-auth-heading">{html.escape(title)}</div>'
        f'<div class="ref-auth-lead">'
        f"{html.escape(subtitle or 'A focused workspace for cleaning, review, field, and manager workflows.')}"
        f"</div></div>"
    )


@contextmanager
def auth_card() -> Iterator[None]:
    with st.container(border=True):
        yield


def auth_meta(text: str) -> None:
    st.caption(text)


@contextmanager
def filter_panel(title: str, help_text: str | None = None, *, expanded: bool = True) -> Iterator[None]:
    with st.expander(title, expanded=expanded):
        if help_text:
            st.caption(help_text)
        yield


@contextmanager
def section(title: str, expanded: bool = False) -> Iterator[None]:
    with st.expander(title, expanded=expanded):
        yield


@contextmanager
def action_row() -> Iterator[None]:
    with st.container(border=True):
        yield


@contextmanager
def page_toolbar() -> Iterator[tuple]:
    left, right = st.columns([3, 1], vertical_alignment="bottom")
    yield left, right


@contextmanager
def workspace_toolbar() -> Iterator[tuple]:
    """Elvis Review action row (fetch button). Project is selected in the sidebar."""
    st.markdown('<div class="ref-workspace-toolbar-marker" aria-hidden="true"></div>', unsafe_allow_html=True)
    with st.container(border=True):
        st.caption("Refresh the review queue for the project selected in the sidebar.")
        left, right = st.columns([2.8, 1], vertical_alignment="bottom", gap="medium")
        yield left, right


def count_badge(count: int, label: str = "records") -> None:
    st.caption(f"{count} {label}")
