"""Reference design system — centralized tokens and presentation CSS."""

from __future__ import annotations

import json

import streamlit as st
import streamlit.components.v1 as components

GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Design tokens (8px grid) ──────────────────────────────── */
:root {
    --primary: #2563EB;
    --primary-dark: #1D4ED8;
    --primary-hover: #1E40AF;
    --cyan: #06B6D4;
    --green: #22C55E;
    --green-soft: #ECFDF3;
    --orange: #F59E0B;
    --orange-soft: #FFF7ED;
    --purple: #7C3AED;
    --purple-soft: #EDE9FE;
    --danger: #DC2626;
    --danger-soft: #FEE2E2;
    --cyan-soft: #CFFAFE;
    --blue-soft: #DBEAFE;
    --bg: #F8FAFC;
    --surface: #FFFFFF;
    --surface-muted: #F8FAFC;
    --surface-header: #F1F5F9;
    --border: #E5E7EB;
    --border-subtle: #F1F5F9;
    --text: #0F172A;
    --text-muted: #64748B;
    --text-subtle: #94A3B8;
    --shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 4px 16px rgba(15, 23, 42, 0.04);
    --shadow-card: 0 1px 3px rgba(15, 23, 42, 0.05);
    --radius-sm: 6px;
    --radius-md: 10px;
    --radius-card: 12px;
    --radius-btn: 10px;
    --radius-nav: 999px;
    --radius-pill: 999px;
    --sidebar-w: 280px;
    --content-max: 1500px;
    --section-gap: 28px;
    --card-gap: 16px;
    --ease: 160ms cubic-bezier(0.4, 0, 0.2, 1);
    --focus: 0 0 0 3px rgba(37, 99, 235, 0.18);
}

html, body, [class*="css"] {
    font-family: Inter, ui-sans-serif, system-ui, -apple-system, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

.stApp {
    color: var(--text);
    background: var(--bg);
}

.main, section[data-testid="stSidebar"] { position: relative; z-index: 1; }

/* Keep Streamlit header so the native sidebar expand control stays available.
   Do NOT hide stToolbar — the expand button lives inside it. */
header[data-testid="stHeader"] {
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
}
header[data-testid="stHeader"] [data-testid="stDecoration"],
header[data-testid="stHeader"] [data-testid="stStatusWidget"],
header[data-testid="stHeader"] [data-testid="stToolbarActions"] {
    display: none !important;
}
#MainMenu, footer { visibility: hidden; }
div[data-testid="stHtml"], div[data-testid="stHtml"] > div {
    margin: 0; padding: 0; background: transparent;
}

/* Markdown UI blocks — tight vertical rhythm */
div[data-testid="stMarkdownContainer"] p:has(.ref-page-shell),
div[data-testid="stMarkdownContainer"] p:has(.ref-grid-wrap),
div[data-testid="stMarkdownContainer"] p:has(.ref-section-block),
div[data-testid="stMarkdownContainer"] p:has(.ref-table-head),
div[data-testid="stMarkdownContainer"] p:has(.ref-table-foot),
div[data-testid="stMarkdownContainer"] p:has(.ref-empty),
div[data-testid="stMarkdownContainer"] p:has(.ref-card) {
    margin: 0 !important;
    padding: 0 !important;
}
div[data-testid="stElementContainer"] {
    margin-bottom: 0.25rem;
}
div[data-testid="stElementContainer"]:has(.ref-grid-wrap),
div[data-testid="stElementContainer"]:has(.ref-table-head),
div[data-testid="stElementContainer"]:has(.ref-table-foot),
div[data-testid="stElementContainer"]:has(.ref-page-shell) {
    margin-bottom: 0 !important;
}
div[data-testid="stMarkdownContainer"]:has(#ref-notif-anchor),
div[data-testid="stMarkdownContainer"]:has(#ref-notif-anchor) p,
div[data-testid="stMarkdownContainer"]:has(#ref-notif-anchor) .ref-ui-root {
    pointer-events: auto !important;
}

/* ── Workspace controls toolbar (Elvis Review) ─────────────── */
.ref-workspace-toolbar-marker { display: none !important; }
.ref-workspace-toolbar-marker + div[data-testid="stVerticalBlockBorderWrapper"] {
    margin-bottom: 12px !important;
    border-color: var(--border) !important;
    border-radius: var(--radius-card) !important;
    background: linear-gradient(180deg, #FFFFFF 0%, #F8FAFC 100%) !important;
    box-shadow: var(--shadow-card) !important;
    padding: 4px 4px 0 !important;
}
.ref-workspace-toolbar-marker + div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stCaptionContainer"] p {
    font-size: 12px !important;
    color: var(--text-muted) !important;
    margin-bottom: 4px !important;
}
.ref-workspace-toolbar-marker + div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stSelectbox"] label p {
    font-size: 11px !important;
    font-weight: 700 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: var(--primary) !important;
}
.ref-workspace-toolbar-marker + div[data-testid="stVerticalBlockBorderWrapper"] div[data-baseweb="select"] > div {
    min-height: 42px !important;
    border-radius: var(--radius-btn) !important;
    border-color: #CBD5E1 !important;
    background: #FFFFFF !important;
    box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.04) !important;
}
.ref-workspace-toolbar-marker + div[data-testid="stVerticalBlockBorderWrapper"] .stButton > button {
    min-height: 42px !important;
    font-weight: 600 !important;
}

.block-container {
    max-width: var(--content-max);
    padding: 8px 32px 48px;
}

hr { margin: 16px 0; border: 0; border-top: 1px solid var(--border); }

.ref-icon { display: inline-block; vertical-align: middle; flex-shrink: 0; stroke-width: 2; }
.ref-icon-lg { width: 20px; height: 20px; }
.ref-icon-md { width: 18px; height: 18px; }
.ref-icon-sm { width: 16px; height: 16px; }
.ref-icon-xs { width: 14px; height: 14px; }

/* ── Sidebar ───────────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: #F8FAFC !important;
    border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"][aria-expanded="true"] {
    width: var(--sidebar-w) !important;
    min-width: var(--sidebar-w) !important;
    max-width: var(--sidebar-w) !important;
}
section[data-testid="stSidebar"][aria-expanded="false"] {
    /* Let Streamlit collapse natively — do not transform/hide the section or the
       expand control (fixed children are trapped by transform containing blocks). */
    min-width: 0 !important;
}
section[data-testid="stSidebar"] > div {
    margin: 0 !important;
    padding: 0 !important;
    border: 0 !important;
    border-radius: 0 !important;
    background: #F8FAFC !important;
    box-shadow: none !important;
}
section[data-testid="stSidebar"][aria-expanded="true"] > div {
    width: var(--sidebar-w) !important;
}
section[data-testid="stSidebar"] .block-container {
    padding: 20px 16px 20px;
    display: flex;
    flex-direction: column;
    min-height: 100vh;
    gap: 0;
}

/* Native Streamlit sidebar expand — always visible when sidebar is collapsed */
[data-testid="stExpandSidebarButton"],
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapsedControl"] {
    display: flex !important;
    visibility: visible !important;
    opacity: 1 !important;
    position: fixed !important;
    top: 0.85rem !important;
    left: 0.85rem !important;
    z-index: 100001 !important;
    width: 2.25rem !important;
    height: 2.25rem !important;
    margin: 0 !important;
    align-items: center !important;
    justify-content: center !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    background: var(--surface) !important;
    box-shadow: var(--shadow-card) !important;
    color: var(--text-muted) !important;
}
[data-testid="stExpandSidebarButton"]:hover,
[data-testid="collapsedControl"]:hover,
[data-testid="stSidebarCollapsedControl"]:hover {
    color: var(--primary) !important;
    border-color: rgba(37, 99, 235, 0.35) !important;
}
/* Leave room for the expand control when the sidebar is collapsed */
.stApp:has([data-testid="stExpandSidebarButton"]),
.stApp:has([data-testid="collapsedControl"]),
.stApp:has([data-testid="stSidebarCollapsedControl"]) {
    --ref-sidebar-toggle-pad: 3rem;
}
[data-testid="stSidebarCollapseButton"] {
    z-index: 2 !important;
    color: var(--text-muted) !important;
}
[data-testid="stSidebarCollapseButton"]:hover {
    color: var(--primary) !important;
}

.ref-brand {
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 18px; padding: 4px 4px 16px;
    border-bottom: 1px solid var(--border);
}
.ref-brand-logo {
    width: 40px; height: 40px; flex-shrink: 0;
    display: flex; align-items: center; justify-content: center;
    border-radius: 10px;
    background: var(--primary); color: #FFF;
    font-size: 13px; font-weight: 700; letter-spacing: -0.02em;
}
.ref-brand-title { font-size: 15px; font-weight: 700; color: var(--text); line-height: 1.25; }
.ref-brand-sub { margin-top: 2px; font-size: 12px; font-weight: 500; color: var(--text-muted); }

.ref-user-card {
    display: flex; align-items: center; gap: 12px;
    padding: 14px; margin-bottom: 20px;
    border: 1px solid var(--border); border-radius: var(--radius-card);
    background: var(--surface);
    box-shadow: var(--shadow);
    transition: border-color var(--ease);
}
.ref-user-card:hover { border-color: #CBD5E1; }
.ref-user-avatar-lg {
    width: 40px; height: 40px; flex-shrink: 0;
    display: flex; align-items: center; justify-content: center;
    border-radius: var(--radius-pill);
    background: var(--primary); color: #FFF;
    font-size: 14px; font-weight: 700;
}
.ref-user-name { font-size: 14px; font-weight: 600; color: var(--text); line-height: 1.3; }
.ref-user-email { margin-top: 2px; font-size: 12px; color: var(--text-muted); line-height: 1.3; }
.ref-user-role {
    display: inline-flex; margin-top: 6px; padding: 3px 10px;
    border-radius: var(--radius-pill); background: var(--blue-soft);
    color: var(--primary-dark); font-size: 11px; font-weight: 600;
}

.ref-nav-heading {
    margin: 16px 0 8px 8px;
    font-size: 11px; font-weight: 600; letter-spacing: 0.08em;
    text-transform: uppercase; color: var(--text-subtle);
}
.ref-nav-heading-account { margin-top: 8px; }
.ref-sidebar-project-heading {
    margin: 4px 0 8px 8px;
    font-size: 11px; font-weight: 600; letter-spacing: 0.08em;
    text-transform: uppercase; color: var(--text-subtle);
}
.ref-sidebar-divider {
    height: 1px;
    background: var(--border);
    margin: 16px 4px 4px;
}
section[data-testid="stSidebar"] div[data-testid="stSelectbox"] {
    margin: 0 0 10px;
    padding: 0 4px;
}
section[data-testid="stSidebar"] div[data-baseweb="select"] > div {
    min-height: 40px !important;
    border-radius: var(--radius-btn) !important;
    border-color: #CBD5E1 !important;
    background: #FFFFFF !important;
}

section[data-testid="stSidebar"] div[role="radiogroup"] {
    display: flex; flex-direction: column; gap: 2px; flex: 1;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label {
    position: relative; min-height: 42px; margin: 0 !important;
    padding: 10px 14px !important;
    border: 1px solid transparent !important;
    border-radius: 12px !important;
    color: #334155 !important;
    font-size: 14px !important; font-weight: 500 !important;
    background: transparent !important;
    cursor: pointer;
    transition: background var(--ease), color var(--ease), border-color var(--ease);
}
section[data-testid="stSidebar"] div[role="radiogroup"] label > div {
    display: flex !important; align-items: center; gap: 10px;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label input {
    position: absolute !important; opacity: 0 !important; width: 0 !important; height: 0 !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label > div > div:has(> input[type="radio"]) {
    display: none !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label [data-testid="stMarkdownContainer"] {
    display: flex !important; align-items: center !important; gap: 10px !important;
    color: inherit !important; font-weight: 500 !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label [data-testid="stMarkdownContainer"] p {
    margin: 0 !important; color: inherit !important; font-size: 13px !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label .ref-nav-icon {
    width: 16px; height: 16px; color: #64748B; stroke-width: 2;
    transition: color var(--ease);
}
section[data-testid="stSidebar"] div[role="radiogroup"] label:hover {
    background: #F1F5F9 !important;
    color: var(--text) !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label:hover .ref-nav-icon {
    color: var(--text-muted) !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {
    color: var(--primary) !important;
    background: #EFF6FF !important;
    border-color: transparent !important;
    box-shadow: none !important;
    font-weight: 600 !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked)::after {
    display: none !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) [data-testid="stMarkdownContainer"],
section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) [data-testid="stMarkdownContainer"] p {
    color: var(--primary) !important; font-weight: 600 !important;
}
section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) .ref-nav-icon {
    color: var(--primary) !important;
}

section[data-testid="stSidebar"] .stButton { margin-top: auto; padding-top: 16px; }
section[data-testid="stSidebar"] .stButton > button {
    min-height: 42px; width: 100%;
    border-radius: var(--radius-btn) !important;
    border: 1px solid var(--border) !important;
    background: var(--surface) !important;
    color: var(--text-muted) !important;
    font-size: 13px !important; font-weight: 500 !important;
    box-shadow: var(--shadow) !important;
    transition: background var(--ease), border-color var(--ease), color var(--ease);
}
section[data-testid="stSidebar"] .stButton > button::before {
    content: "";
    display: inline-block;
    width: 16px; height: 16px; margin-right: 8px;
    vertical-align: -3px;
    background: currentColor;
    -webkit-mask: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='2'%3E%3Cpath d='M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4'/%3E%3Cpolyline points='16 17 21 12 16 7'/%3E%3Cline x1='21' y1='12' x2='9' y2='12'/%3E%3C/svg%3E") center/contain no-repeat;
    mask: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='2'%3E%3Cpath d='M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4'/%3E%3Cpolyline points='16 17 21 12 16 7'/%3E%3Cline x1='21' y1='12' x2='9' y2='12'/%3E%3C/svg%3E") center/contain no-repeat;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    color: var(--text) !important;
    border-color: #CBD5E1 !important;
    background: var(--surface-muted) !important;
}

/* ── App header bar (reference) ────────────────────────────── */
.ref-page-shell { margin-bottom: var(--section-gap); position: relative; }

.ref-app-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    flex-wrap: wrap;
    min-height: 64px;
    padding: 10px 4px 14px;
    margin: 0 0 8px;
    border-bottom: 1px solid var(--border);
    background: transparent;
    overflow: visible;
    position: relative;
    z-index: 20;
}
.ref-header-zone {
    position: relative;
    padding: 0;
    margin: 0;
    overflow: visible;
}
.ref-header-wave-wrap { display: none !important; }
.ref-header-wave { display: none !important; }

.ref-topbar {
    position: relative; z-index: 1;
    display: flex; align-items: center; justify-content: space-between;
    min-height: 40px; padding: 4px 0 16px;
    gap: 16px; flex-wrap: wrap;
}
.ref-navbar-crumb {
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
    font-size: 13px; font-weight: 500; letter-spacing: 0;
    text-transform: none; color: var(--text-muted);
    padding-left: var(--ref-sidebar-toggle-pad, 0);
}
.ref-navbar-crumb .ref-crumb-current {
    color: var(--text);
    font-weight: 600;
}
.ref-crumb-sep { color: var(--text-subtle); font-weight: 400; }

.ref-navbar-tools {
    display: flex; align-items: center; gap: 10px; flex-shrink: 0; flex-wrap: wrap;
    position: relative;
    z-index: 12;
    overflow: visible;
}
.ref-pill {
    display: inline-flex; align-items: center; gap: 8px;
    height: 36px; padding: 0 14px;
    border: 1px solid transparent; border-radius: var(--radius-pill);
    background: var(--surface); font-size: 13px; font-weight: 600;
    color: var(--text-muted); white-space: nowrap;
    box-shadow: none;
}
.ref-pill-live {
    color: #15803D; background: var(--green-soft);
    border-color: rgba(22, 163, 74, 0.15);
}
.ref-pill-prod {
    color: var(--primary-dark); background: var(--surface);
    border-color: rgba(37, 99, 235, 0.35);
}
.ref-pill-user { display: none !important; }
.ref-dot-live {
    width: 7px; height: 7px; border-radius: 50%;
    background: #22C55E;
    box-shadow: 0 0 0 3px rgba(34, 197, 94, 0.18);
}
.ref-avatar-sm {
    width: 34px; height: 34px;
    display: flex; align-items: center; justify-content: center;
    border-radius: 50%; background: var(--primary); color: #FFF;
    font-size: 12px; font-weight: 700; flex-shrink: 0;
}
.ref-bell-wrap { position: relative; display: inline-flex; flex-shrink: 0; z-index: 13; }
.ref-bell-btn {
    position: relative;
    display: inline-flex; align-items: center; justify-content: center;
    width: 40px; height: 40px;
    margin: 0; padding: 0;
    border: 1px solid #E2E8F0;
    border-radius: 50%;
    background: #FFFFFF;
    color: #475569;
    box-shadow: none;
    cursor: pointer;
    transition: background var(--ease), border-color var(--ease), color var(--ease);
}
.ref-bell-btn:hover {
    background: #F8FAFC;
    border-color: #CBD5E1;
    color: var(--text);
}
.ref-bell-btn:focus-visible {
    outline: none;
    box-shadow: var(--focus);
}
.ref-bell {
    display: none !important;
}
.ref-bell-icon, .ref-bell-divider, .ref-bell-chevron { display: none !important; }
.ref-user-menu {
    position: relative;
    display: inline-flex;
    flex-shrink: 0;
    z-index: 13;
}
.ref-user-chip {
    display: inline-flex;
    align-items: center;
    gap: 10px;
    height: 40px;
    margin: 0;
    padding: 4px 12px 4px 4px;
    border: 1px solid #E2E8F0;
    border-radius: var(--radius-pill);
    background: #FFFFFF;
    color: var(--text);
    cursor: pointer;
    font: inherit;
    transition: background var(--ease), border-color var(--ease);
}
.ref-user-chip:hover {
    background: #F8FAFC;
    border-color: #CBD5E1;
}
.ref-user-chip:focus-visible {
    outline: none;
    box-shadow: var(--focus);
}
.ref-user-menu.ref-open .ref-user-chip {
    border-color: rgba(37, 99, 235, 0.35);
    background: #F8FAFC;
}
.ref-user-menu.ref-open .ref-user-chip-chevron svg {
    transform: rotate(180deg);
}
.ref-user-chip-name {
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
    white-space: nowrap;
}
.ref-user-chip-chevron {
    display: inline-flex;
    color: var(--text-subtle);
    margin-left: 2px;
    transition: transform var(--ease);
}
.ref-user-panel {
    position: absolute;
    top: calc(100% + 8px);
    right: 0;
    z-index: 100002;
    min-width: 220px;
    padding: 14px 16px;
    border: 1px solid var(--border);
    border-radius: 12px;
    background: var(--surface);
    box-shadow: 0 8px 24px rgba(15, 23, 42, 0.1);
    display: none;
}
.ref-user-panel.ref-open {
    display: flex !important;
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
    pointer-events: auto !important;
}
.ref-user-panel-role {
    display: inline-flex;
    align-items: center;
    padding: 4px 10px;
    border-radius: var(--radius-pill);
    background: var(--blue-soft);
    color: var(--primary-dark);
    font-size: 12px;
    font-weight: 600;
    line-height: 1.2;
}
.ref-user-panel-email {
    font-size: 13px;
    font-weight: 500;
    color: var(--text-muted);
    line-height: 1.35;
    word-break: break-word;
}
.ref-notif-panel {
    position: absolute;
    top: calc(100% + 8px);
    right: 0;
    z-index: 100002;
    width: min(360px, calc(100vw - 24px));
    min-width: 280px;
    padding: 14px;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--surface);
    box-shadow: 0 4px 16px rgba(15, 23, 42, 0.1);
    display: none;
}
.ref-notif-panel.ref-open {
    display: block !important;
    pointer-events: auto !important;
}
.ref-notif-backdrop {
    position: fixed;
    inset: 0;
    z-index: 100000;
    display: none;
    background: transparent;
    pointer-events: none;
}
.ref-notif-backdrop.ref-open {
    display: block;
    pointer-events: auto;
}
.ref-notif-panel-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    font-size: 14px;
    font-weight: 700;
    color: var(--text);
    margin-bottom: 10px;
}
.ref-notif-close {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 30px;
    height: 30px;
    margin: -6px -6px -6px 0;
    padding: 0;
    border: 0;
    border-radius: 50%;
    background: transparent;
    color: var(--text-muted);
    font-size: 24px;
    font-weight: 400;
    line-height: 1;
    cursor: pointer;
}
.ref-notif-close:hover {
    background: var(--surface-muted);
    color: var(--text);
}
.ref-notif-close:focus-visible {
    outline: none;
    box-shadow: var(--focus);
}
.ref-notif-mark-all {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    height: 36px;
    margin: 0 0 12px;
    padding: 0 12px;
    border: 1px solid var(--border);
    border-radius: var(--radius-pill);
    background: var(--surface);
    color: var(--primary);
    font-size: 13px;
    font-weight: 600;
    text-align: center;
    text-decoration: none !important;
    box-sizing: border-box;
    cursor: pointer;
    pointer-events: auto !important;
    position: relative;
    z-index: 100003;
    transition: background var(--ease), border-color var(--ease);
}
.ref-notif-mark-all:hover {
    background: var(--surface-muted);
    border-color: #CBD5E1;
    color: var(--primary);
    text-decoration: none !important;
}
/* Hidden Streamlit bridge for Mark all read (must stay in-DOM / clickable) */
div[data-testid="stElementContainer"]:has(#ref-notif-actions),
div[data-testid="stElementContainer"]:has(#ref-notif-actions) + div[data-testid="stElementContainer"] {
    position: absolute !important;
    width: 1px !important;
    height: 1px !important;
    margin: 0 !important;
    padding: 0 !important;
    overflow: hidden !important;
    clip: rect(0, 0, 0, 0) !important;
    border: 0 !important;
    opacity: 0 !important;
}
.ref-notif-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 8px;
}
.ref-notif-item {
    display: flex;
    gap: 6px;
    font-size: 12px;
    line-height: 1.45;
    color: var(--text-muted);
}
.ref-notif-unread { color: var(--text); }
.ref-notif-bullet {
    flex-shrink: 0;
    width: 10px;
    color: var(--text-muted);
}
.ref-notif-empty {
    font-size: 12px;
    color: var(--text-muted);
    padding: 4px 0;
}
.ref-bell:focus-visible {
    outline: none;
    box-shadow: var(--focus);
}
.ref-bell-badge {
    position: absolute; top: -2px; right: -2px;
    min-width: 18px; height: 18px; padding: 0 5px;
    display: flex; align-items: center; justify-content: center;
    border-radius: var(--radius-pill); background: var(--danger); color: #FFF;
    font-size: 10px; font-weight: 700;
    border: 2px solid #F8FAFC;
    line-height: 1;
}

.ref-hero-row {
    position: relative; z-index: 1;
    display: flex; align-items: flex-start; justify-content: flex-start;
    gap: 24px; flex-wrap: wrap;
    padding: 8px 0 4px;
}
.ref-hero-copy { flex: 1; min-width: 280px; }
.ref-hero-title {
    margin: 0; font-size: 30px; font-weight: 700;
    letter-spacing: -0.03em; line-height: 1.15; color: var(--text);
}
.ref-hero-sub {
    margin: 8px 0 0; font-size: 14px; font-weight: 400;
    color: var(--text-muted); line-height: 1.5; max-width: 680px;
}
.ref-hero-title-wrap {
    display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
}
.ref-hero-eyebrow {
    margin: 0 0 4px; font-size: 10px; font-weight: 600;
    letter-spacing: 0.1em; text-transform: uppercase; color: var(--primary);
}
.ref-hero-badge {
    display: inline-flex; align-items: center;
    padding: 4px 10px; border-radius: var(--radius-pill);
    background: var(--blue-soft); color: var(--primary-dark);
    font-size: 11px; font-weight: 600;
}

/* ── Section headers ───────────────────────────────────────── */
.ref-section-block {
    margin: 24px 0 16px;
}
.ref-section-block:first-child,
.ref-page-shell + .ref-section-block { margin-top: 8px; }

.ref-section {
    display: flex; align-items: center; gap: 8px;
    margin: var(--section-gap) 0 16px;
}
.ref-section:first-child,
.ref-page-shell + .ref-section-block,
.ref-page-shell + .ref-section,
.ref-grid-wrap + .ref-section-block,
.ref-grid-wrap + .ref-section { margin-top: 0; }
.ref-section-inline { margin: 0 !important; }

.ref-section-bar {
    width: 4px; height: 14px; border-radius: var(--radius-pill);
    background: var(--primary); flex-shrink: 0;
}
.ref-section-text {
    font-size: 11px; font-weight: 700; letter-spacing: 0.12em;
    text-transform: uppercase; color: var(--primary);
}
.ref-section-heading {
    margin: 0; font-size: 16px; font-weight: 600;
    color: var(--text); line-height: 1.3;
}
.ref-section-desc {
    margin: 4px 0 0; font-size: 13px; font-weight: 400;
    color: var(--text-muted); line-height: 1.45;
}
.ref-section-divider {
    height: 1px; background: var(--border);
    margin-top: 12px;
}
.ref-section:not(.ref-section-inline) .ref-section-divider-inline {
    display: none;
}

/* ── KPI / metric cards ────────────────────────────────────── */
.ref-grid-wrap { margin-bottom: var(--section-gap); }
.ref-grid { display: grid; gap: var(--card-gap); }
.ref-grid-5 { grid-template-columns: repeat(5, minmax(0, 1fr)); }
.ref-grid-4 { grid-template-columns: repeat(4, minmax(0, 1fr)); }
.ref-grid-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.ref-grid-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.ref-grid-1 { grid-template-columns: 1fr; }

@media (max-width: 1200px) {
    .ref-grid-5, .ref-grid-4 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .ref-grid-3 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 700px) {
    .ref-grid-5, .ref-grid-4, .ref-grid-3, .ref-grid-2 { grid-template-columns: 1fr; }
    .ref-hero-title { font-size: 28px; }
    .ref-topbar { padding: 8px 0; gap: 10px; }
    .block-container { padding: 8px 16px 32px; }
}

.ref-card {
    position: relative;
    display: flex; flex-direction: column;
    min-height: 136px;
    padding: 20px;
    border: 1px solid var(--border);
    border-radius: var(--radius-card); background: var(--surface);
    box-shadow: var(--shadow-card);
    transition: border-color var(--ease), box-shadow var(--ease);
    overflow: hidden;
}
.ref-card:hover { border-color: #D1D5DB; }
.ref-card-analytics { min-height: 156px; }

.ref-card-icon {
    width: 42px; height: 42px; flex-shrink: 0;
    display: flex; align-items: center; justify-content: center;
    border-radius: var(--radius-md);
    margin-bottom: 14px;
}
.ref-tone-blue .ref-card-icon   { background: var(--blue-soft); color: var(--primary); }
.ref-tone-orange .ref-card-icon { background: var(--orange-soft); color: var(--orange); }
.ref-tone-green .ref-card-icon  { background: var(--green-soft); color: var(--green); }
.ref-tone-purple .ref-card-icon { background: var(--purple-soft); color: var(--purple); }
.ref-card-icon svg.ref-icon {
    width: 20px; height: 20px;
}
.ref-empty-icon svg.ref-icon {
    width: 28px; height: 28px;
    color: var(--primary);
}

.ref-card-label {
    font-size: 13px; font-weight: 500; color: var(--text-muted);
    line-height: 1.3;
}
.ref-card-value {
    margin-top: 4px; font-size: 32px; font-weight: 700;
    letter-spacing: -0.03em; line-height: 1; color: var(--text);
}
.ref-card-value-text {
    font-size: 15px; font-weight: 700; letter-spacing: -0.01em;
    line-height: 1.25; word-break: break-word; overflow-wrap: anywhere;
    white-space: normal; max-width: 100%;
    padding-right: 8px;
}
.ref-card-hint {
    margin-top: 6px; font-size: 13px; font-weight: 400;
    color: var(--text-muted); line-height: 1.35;
}

.ref-card-spark {
    position: absolute; right: 18px; bottom: 18px; opacity: 0.95;
}
.ref-spark { width: 64px; height: 22px; display: block; }
.ref-spark-blue   { color: var(--primary); }
.ref-spark-orange { color: var(--orange); }
.ref-spark-green  { color: var(--green); }
.ref-spark-purple { color: var(--purple); }
.ref-spark-cyan   { color: var(--cyan); }

.ref-card-trend {
    display: inline-flex; align-items: center; gap: 3px;
    margin-top: 6px; padding: 2px 6px;
    border-radius: var(--radius-sm);
    font-size: 11px; font-weight: 600;
}
.ref-card-trend-up { background: var(--green-soft); color: var(--green); }
.ref-card-trend-down { background: var(--danger-soft); color: var(--danger); }
.ref-card-trend-neutral { background: var(--surface-muted); color: var(--text-muted); }

.ref-card-progress {
    margin-top: 14px; height: 6px; border-radius: var(--radius-pill);
    background: #E5E7EB; overflow: hidden;
}
.ref-card-progress-fill { height: 100%; border-radius: var(--radius-pill); transition: width 400ms var(--ease); }
.ref-tone-fill-blue   { background: var(--primary); }
.ref-tone-fill-orange { background: var(--orange); }
.ref-tone-fill-green  { background: var(--green); }
.ref-tone-fill-purple { background: var(--purple); }
.ref-tone-fill-cyan   { background: var(--cyan); }

.ref-card-delta {
    display: inline-flex; align-items: center; gap: 4px; margin-top: 8px;
    padding: 4px 8px; border-radius: var(--radius-sm);
    font-size: 12px; font-weight: 600;
}
.ref-card-delta-up { background: var(--green-soft); color: #15803D; }
.ref-card-delta-down { background: var(--danger-soft); color: var(--danger); }

/* ── Table toolbar (functional search / filters) ───────────── */
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] {
    margin-top: 0 !important;
    padding: 16px 20px !important;
    border: 1px solid var(--border) !important;
    border-bottom: 0 !important;
    border-radius: var(--radius-card) var(--radius-card) 0 0 !important;
    background: var(--surface) !important;
    box-shadow: var(--shadow-card) !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] div[data-testid="stTextInput"] input {
    min-height: 40px !important;
    border-radius: var(--radius-btn) !important;
    border-color: var(--border) !important;
    font-size: 14px !important;
    color: var(--text) !important;
    box-shadow: none !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] div[data-testid="stTextInput"] input::placeholder {
    color: var(--text-subtle) !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] .stButton > button,
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] .stDownloadButton > button {
    min-height: 40px !important;
    border-radius: var(--radius-btn) !important;
    border: 1px solid var(--border) !important;
    background: var(--surface) !important;
    color: var(--text-muted) !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    box-shadow: none !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] + div[data-testid="stElementContainer"] {
    margin-top: 0 !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] + div[data-testid="stElementContainer"] .ag-root-wrapper {
    border: 1px solid var(--border) !important;
    border-top: 0 !important;
    border-bottom: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    background: var(--surface) !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-toolbar-marker) + div[data-testid="stElementContainer"] + div[data-testid="stElementContainer"] .ag-paging-panel {
    display: none !important;
}

/* ── Table shell (unified card via siblings) ───────────────── */
.ref-table-head {
    display: flex; align-items: center; justify-content: space-between;
    gap: 12px; flex-wrap: wrap;
    padding: 16px 20px;
    border: 1px solid var(--border);
    border-bottom: 0;
    border-radius: var(--radius-card) var(--radius-card) 0 0;
    background: var(--surface);
    box-shadow: var(--shadow-card);
}
.ref-search {
    display: inline-flex; align-items: center; gap: 8px;
    width: 280px; max-width: 100%; height: 40px; padding: 0 14px;
    border: 1px solid var(--border); border-radius: var(--radius-btn);
    background: var(--surface); color: var(--text-subtle);
    font-size: 14px; font-weight: 400;
}
.ref-tool-btn {
    display: inline-flex; align-items: center; gap: 6px;
    height: 40px; padding: 0 14px;
    border: 1px solid var(--border); border-radius: var(--radius-btn);
    background: var(--surface); color: var(--text-muted);
    font-size: 13px; font-weight: 500;
    transition: background var(--ease), border-color var(--ease), color var(--ease);
    cursor: default;
}
.ref-tool-btn:hover {
    background: var(--surface-muted);
    border-color: #CBD5E1;
    color: var(--text);
}
.ref-tool-btn-icon {
    width: 40px; min-width: 40px; padding: 0; justify-content: center;
}

.ref-table-tools {
    display: flex; align-items: center; gap: 8px; margin-left: auto;
}

div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] {
    margin-top: 0 !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] [data-testid="stDataFrame"],
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] div[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-top: 0 !important;
    border-bottom: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    background: var(--surface) !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] {
    border: 0 !important;
    box-shadow: none !important;
    background: transparent !important;
    padding: 0 !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] [data-testid="stVerticalBlockBorderWrapper"] {
    border: 0 !important;
    box-shadow: none !important;
    background: transparent !important;
    padding: 0 !important;
    margin: 0 !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] [data-testid="glideDataEditor"] {
    border: 0 !important;
    border-radius: 0 !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] .dvn-scroller {
    border: 0 !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] .ag-root-wrapper {
    border: 1px solid var(--border) !important;
    border-top: 0 !important;
    border-bottom: 0 !important;
    border-radius: 0 !important;
    box-shadow: none !important;
    background: var(--surface) !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-head) + div[data-testid="stElementContainer"] .ag-paging-panel {
    display: none !important;
}
div[data-testid="stElementContainer"]:has(.ref-table-foot) {
    margin-top: 0 !important;
}
.ref-table-foot {
    display: flex; align-items: center; justify-content: space-between;
    padding: 14px 20px;
    border: 1px solid var(--border);
    border-top: 1px solid var(--border);
    border-radius: 0 0 var(--radius-card) var(--radius-card);
    background: var(--surface);
    box-shadow: var(--shadow-card);
    margin-bottom: var(--section-gap);
    margin-top: 0;
}
.ref-table-foot-info { font-size: 12px; font-weight: 500; color: var(--text-muted); }
.ref-pagination { display: flex; align-items: center; gap: 4px; }
.ref-page-arrow {
    display: inline-flex; align-items: center; justify-content: center;
    width: 32px; height: 32px; border: 1px solid var(--border);
    border-radius: var(--radius-btn); background: var(--surface); color: var(--text-muted);
    transition: background var(--ease), border-color var(--ease);
}
.ref-page-arrow-off { opacity: 0.4; pointer-events: none; }
.ref-page-active {
    display: inline-flex; align-items: center; justify-content: center;
    min-width: 34px; height: 34px; padding: 0 8px;
    border-radius: var(--radius-btn); background: var(--primary); color: #FFF;
    font-size: 13px; font-weight: 600;
}

/* ── Form field labels (drill-down select) ───────────────── */
div[data-testid="stSelectbox"] > label p {
    font-size: 14px !important;
    font-weight: 600 !important;
    color: var(--text) !important;
    margin-bottom: 8px !important;
}
div[data-testid="stSelectbox"] div[data-baseweb="select"] > div {
    min-height: 52px !important;
    font-size: 15px !important;
    font-weight: 500 !important;
    border-radius: var(--radius-btn) !important;
    box-shadow: var(--shadow-card) !important;
    background: var(--surface) !important;
}

/* ── Dataframe styling ─────────────────────────────────────── */
div[data-testid="stDataFrame"] {
    font-family: Inter, sans-serif !important;
    border-radius: 0 !important;
}
div[data-testid="stDataFrame"] [data-testid="glideDataEditor"] {
    font-size: 13px !important;
}
div[data-testid="stDataFrame"] [class*="header"],
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameHeader"] {
    background: var(--surface-header) !important;
    color: var(--text) !important;
    font-weight: 600 !important;
    font-size: 11px !important;
    letter-spacing: 0.04em !important;
    text-transform: uppercase !important;
    border-color: var(--border) !important;
}
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameDataCell"] {
    border-color: var(--border-subtle) !important;
    color: var(--text) !important;
    font-size: 14px !important;
    background: var(--surface) !important;
}
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameDataCell"]:not(:first-child) {
    text-align: right !important;
    justify-content: flex-end !important;
}
div[data-testid="stDataFrame"] [class*="header"] [data-testid="StyledDataFrameColHeader"]:not(:first-child),
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameHeader"] [data-testid="StyledDataFrameColHeader"]:not(:first-child) {
    text-align: right !important;
}
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameRow"]:nth-child(even) [data-testid="StyledDataFrameDataCell"] {
    background: var(--surface) !important;
}
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameRow"]:hover [data-testid="StyledDataFrameDataCell"] {
    background: #F8FAFC !important;
}
div[data-testid="stDataFrame"] [data-testid="StyledDataFrameRow"] [data-testid="StyledDataFrameDataCell"]:first-child {
    padding-left: 40px !important;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%2394A3B8' stroke-width='2'%3E%3Cpath d='M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z'/%3E%3Cpolyline points='14 2 14 8 20 8'/%3E%3C/svg%3E") !important;
    background-repeat: no-repeat !important;
    background-position: 16px center !important;
    background-size: 14px 14px !important;
    font-weight: 600 !important;
}

/* ── Forms & inputs ────────────────────────────────────────── */
div[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div {
    min-height: 40px !important;
    font-size: 14px !important; font-weight: 400 !important;
    border-radius: var(--radius-btn) !important;
    border-color: var(--border) !important;
    background: var(--surface) !important;
    transition: border-color var(--ease), box-shadow var(--ease);
}

.stButton > button,
div[data-testid="stDownloadButton"] button,
div[data-testid="stFormSubmitButton"] button {
    min-height: 40px; padding: 0 16px !important;
    border-radius: var(--radius-btn) !important;
    border: 1px solid var(--border) !important;
    font-size: 13px !important; font-weight: 600 !important;
    box-shadow: none;
    transition: background var(--ease), border-color var(--ease), color var(--ease), box-shadow var(--ease);
}
.stButton > button[kind="primary"],
div[data-testid="stFormSubmitButton"] button[kind="primary"] {
    background: var(--primary) !important;
    border-color: var(--primary) !important;
    color: #FFF !important;
}
.stButton > button[kind="primary"]:hover,
div[data-testid="stFormSubmitButton"] button[kind="primary"]:hover {
    background: var(--primary-dark) !important;
    border-color: var(--primary-dark) !important;
}
.stButton > button:hover,
div[data-testid="stDownloadButton"] button:hover,
div[data-testid="stFormSubmitButton"] button:hover {
    border-color: #CBD5E1 !important;
    background: var(--surface-muted) !important;
}

div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div,
input[type="text"], input[type="number"], input[type="password"],
textarea {
    border-radius: var(--radius-btn) !important;
    border-color: var(--border) !important;
    min-height: 40px;
    font-size: 14px !important;
    transition: border-color var(--ease), box-shadow var(--ease);
}
textarea { min-height: 80px !important; }

button:focus-visible,
input:focus,
textarea:focus,
div[data-baseweb="select"] > div:focus-within,
div[data-baseweb="input"] > div:focus-within {
    outline: none !important;
    border-color: var(--primary) !important;
    box-shadow: var(--focus) !important;
}

div[data-testid="stCheckbox"] label,
div[data-testid="stRadio"] label {
    color: var(--text-muted) !important;
    font-size: 13px !important;
}
div[data-testid="stCheckbox"] label span,
div[data-testid="stRadio"] label span {
    font-weight: 500 !important;
}

/* ── Containers & expanders ────────────────────────────────── */
div[data-testid="stVerticalBlockBorderWrapper"],
div[data-testid="stExpander"] {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-card) !important;
    background: var(--surface) !important;
    box-shadow: var(--shadow) !important;
}
div[data-testid="stExpander"] details > summary {
    padding: 14px 16px !important;
    font-size: 14px !important;
    font-weight: 600 !important;
    color: var(--text) !important;
}
div[data-testid="stExpander"] details > summary:hover {
    color: var(--primary) !important;
}

div[data-testid="stMetric"] { display: none !important; }

/* ── Badges ────────────────────────────────────────────────── */
.ref-badge {
    display: inline-flex; align-items: center;
    padding: 3px 8px; border-radius: var(--radius-pill);
    font-size: 11px; font-weight: 600; line-height: 1.2;
}
.ref-badge-neutral { background: var(--surface-muted); color: var(--text-muted); border: 1px solid var(--border); }
.ref-badge-blue { background: var(--blue-soft); color: var(--primary-dark); }
.ref-badge-green { background: var(--green-soft); color: #15803D; }
.ref-badge-orange { background: var(--orange-soft); color: #B45309; }
.ref-badge-red { background: var(--danger-soft); color: var(--danger); }
.ref-badge-purple { background: var(--purple-soft); color: var(--purple); }
.ref-badge-cyan { background: var(--cyan-soft); color: #0E7490; }

/* ── Notices & empty states ────────────────────────────────── */
.ref-notice {
    display: flex; align-items: flex-start; gap: 10px;
    padding: 12px 16px; margin: 16px 0;
    border: 1px solid var(--border); border-left: 3px solid var(--primary);
    border-radius: var(--radius-btn); background: var(--surface);
    font-size: 13px; color: var(--text-muted); line-height: 1.5;
}
.ref-notice i { color: var(--primary); margin-top: 1px; }

.ref-empty {
    text-align: center; padding: 40px 24px;
    border: 1px dashed var(--border); border-radius: var(--radius-card);
    background: var(--surface);
}
.ref-empty-icon {
    width: 44px; height: 44px; margin: 0 auto 12px;
    display: flex; align-items: center; justify-content: center;
    border-radius: 50%; background: var(--blue-soft); color: var(--primary);
}
.ref-empty-title { font-size: 15px; font-weight: 600; color: var(--text); }
.ref-empty-copy { margin-top: 4px; font-size: 13px; color: var(--text-muted); }

.ref-surface {
    padding: 16px; border: 1px solid var(--border);
    border-radius: var(--radius-card); background: var(--surface);
    box-shadow: var(--shadow);
}

/* ── Auth ──────────────────────────────────────────────────── */
.ref-auth { text-align: center; margin-bottom: 24px; }
.ref-auth-mark {
    width: 48px; height: 48px; margin: 0 auto 12px;
    display: flex; align-items: center; justify-content: center;
    border-radius: var(--radius-md); background: var(--primary); color: #FFF;
    font-size: 14px; font-weight: 700;
}
.ref-auth-heading {
    font-size: 22px; font-weight: 700; color: var(--text);
    letter-spacing: -0.02em; margin-bottom: 6px;
}
.ref-auth-lead {
    font-size: 14px; color: var(--text-muted); line-height: 1.5;
}

/* ── Captions & alerts ─────────────────────────────────────── */
div[data-testid="stCaptionContainer"] p {
    font-size: 12px !important; color: var(--text-muted) !important;
}
div[data-testid="stAlert"] {
    border-radius: var(--radius-btn) !important;
    border: 1px solid var(--border) !important;
    font-size: 13px !important;
}

/* ── Popover (notifications) ───────────────────────────────── */
div[data-testid="stPopoverBody"] {
    border: 1px solid var(--border) !important;
    border-radius: var(--radius-card) !important;
    box-shadow: 0 4px 16px rgba(15, 23, 42, 0.1) !important;
    padding: 12px !important;
}

/* ── Tabs ──────────────────────────────────────────────────── */
div[data-testid="stTabs"] button[data-baseweb="tab"] {
    height: 40px !important;
    padding: 0 16px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    color: var(--text-muted) !important;
    border-radius: var(--radius-btn) var(--radius-btn) 0 0 !important;
    transition: color var(--ease), border-color var(--ease);
}
div[data-testid="stTabs"] button[data-baseweb="tab"]:hover {
    color: var(--text) !important;
}
div[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"] {
    color: var(--primary) !important;
    font-weight: 600 !important;
    border-bottom-color: var(--primary) !important;
}
div[data-testid="stTabs"] [data-baseweb="tab-border"] {
    background: var(--border) !important;
}
div[data-testid="stTabs"] [data-baseweb="tab-panel"] {
    padding-top: 16px !important;
}

/* ── Charts ─────────────────────────────────────────────────── */
div[data-testid="stPlotlyChart"] {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--surface);
    box-shadow: var(--shadow);
    padding: 8px;
    overflow: hidden;
}

@media (max-width: 900px) {
    section[data-testid="stSidebar"][aria-expanded="true"] {
        width: 100% !important;
        min-width: 0 !important;
        max-width: 100% !important;
    }
    section[data-testid="stSidebar"][aria-expanded="true"] > div {
        width: 100% !important;
    }
}
</style>
"""

AUTH_MODE_CSS = """
<style>
section[data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none !important; }
.main .block-container { max-width: 440px; margin: 0 auto; padding-top: 56px; }
</style>
"""

PARENT_RUNTIME_JS = r"""
(function () {
    const ICONS = {
        "Project Dashboard":"layout-dashboard",
        "Elvis_Review":"clipboard-list",
        "Combined Checks":"list-checks",
        "Supervisor View Only":"eye",
        "Record History":"history",
        "Admin Approval":"shield-check",
        "Demographic Review":"users",
        "Demographic Flag Config":"settings",
        "Field Team":"map-pin",
        "Manager Analytics":"bar-chart-3",
        "Reviewer Stats":"trending-up",
        "Sync & Admin":"refresh-cw"
    };
    function loadLucide(cb) {
        if (window.lucide) { cb(); return; }
        const existing = document.getElementById("ref-lucide-cdn");
        if (existing) { existing.addEventListener("load", cb); return; }
        const s = document.createElement("script");
        s.id = "ref-lucide-cdn";
        s.src = "https://unpkg.com/lucide@0.469.0/dist/umd/lucide.min.js";
        s.onload = cb;
        document.head.appendChild(s);
    }
    function svg(name, size, cls) {
        if (!window.lucide || !window.lucide.icons || !window.lucide.icons[name]) return null;
        const d = window.lucide.icons[name];
        const s = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        s.setAttribute("viewBox", "0 0 24 24");
        s.setAttribute("width", size || 16);
        s.setAttribute("height", size || 16);
        s.setAttribute("fill", "none");
        s.setAttribute("stroke", "currentColor");
        s.setAttribute("stroke-width", "2");
        if (cls) s.setAttribute("class", cls);
        d.forEach(c => {
            const n = document.createElementNS("http://www.w3.org/2000/svg", c[0]);
            Object.keys(c[1] || {}).forEach(k => n.setAttribute(k, c[1][k]));
            if (c[2]) n.textContent = c[2];
            s.appendChild(n);
        });
        return s;
    }
    function nav() {
        document.querySelectorAll('section[data-testid="stSidebar"] div[role="radiogroup"] label').forEach(l => {
            if (l.dataset.refIconDone) return;
            const wrap = l.querySelector('[data-testid="stMarkdownContainer"]');
            if (!wrap) return;
            const text = (wrap.textContent || "").trim();
            const ic = ICONS[text];
            if (ic && !wrap.querySelector(".ref-nav-icon")) {
                const el = svg(ic, 18, "ref-nav-icon");
                if (el) wrap.insertBefore(el, wrap.firstChild);
            }
            l.dataset.refIconDone = "1";
        });
    }
    function appDocuments() {
        const docs = [];
        const seen = new Set();
        function add(doc) {
            if (!doc || seen.has(doc)) return;
            seen.add(doc);
            docs.push(doc);
        }
        add(document);
        try { add(window.parent && window.parent.document); } catch (e) {}
        docs.slice().forEach((doc) => {
            try {
                doc.querySelectorAll("iframe").forEach((frame) => {
                    try { add(frame.contentDocument); } catch (e) {}
                });
            } catch (e) {}
        });
        return docs;
    }
    function cleanupStalePortals(doc) {
        if (!doc || !doc.body) return;
        const currentNotif = doc.querySelector("#ref-notif-wrap #ref-notif-panel");
        const currentUser = doc.querySelector("#ref-user-menu #ref-user-panel");

        // Only remove overlays we detached onto document.body (safe for Streamlit/React).
        Array.from(doc.body.children).forEach((element) => {
            const id = element.id || "";
            const cls = String(element.className || "");
            const isOverlay =
                id === "ref-notif-panel" ||
                id === "ref-user-panel" ||
                id === "ref-notif-backdrop" ||
                cls.includes("ref-notif-panel") ||
                cls.includes("ref-user-panel") ||
                cls.includes("ref-notif-backdrop");
            if (!isOverlay) return;
            if (element === currentNotif || element === currentUser) return;
            element.remove();
        });

        if (!currentNotif && !currentUser) {
            doc.querySelectorAll(".ref-notif-panel.ref-open, .ref-user-panel.ref-open, .ref-bell-wrap.ref-open, .ref-user-menu.ref-open")
                .forEach((element) => element.classList.remove("ref-open"));
        }
    }
    function clickNotifMarkAll() {
        const label = "RCD_MARK_ALL_READ";
        for (const doc of appDocuments()) {
            try {
                const buttons = doc.querySelectorAll("button");
                for (const btn of buttons) {
                    const text = (btn.innerText || btn.textContent || "").replace(/\s+/g, " ").trim();
                    if (text === label) {
                        btn.click();
                        return true;
                    }
                }
            } catch (e) {}
        }
        // Same-tab fallback only — never window.open / target=_blank.
        try {
            const win = window;
            const url = new URL(win.location.href);
            url.searchParams.set("ref_notif_mark_all", "1");
            win.location.replace(url.toString());
            return true;
        } catch (e) {}
        return false;
    }
    function resolveNotifDoc(seed) {
        if (seed && seed.ownerDocument) return seed.ownerDocument;
        if (seed && seed.nodeType === 9) return seed;
        for (const doc of appDocuments()) {
            if (doc.getElementById("ref-notif-anchor")) return doc;
        }
        return document;
    }
    function positionNotifPanel(doc) {
        doc = resolveNotifDoc(doc);
        const panel = doc.getElementById("ref-notif-panel");
        if (!panel) return;
        // Keep the panel anchored to the bell wrap so logout removes it with the header.
        panel.style.position = "absolute";
        panel.style.top = "calc(100% + 8px)";
        panel.style.right = "0";
        panel.style.left = "auto";
        panel.style.zIndex = "100002";
        panel.style.pointerEvents = "auto";
    }
    function positionUserPanel(doc) {
        doc = resolveNotifDoc(doc);
        const panel = doc.getElementById("ref-user-panel");
        if (!panel) return;
        panel.style.position = "absolute";
        panel.style.top = "calc(100% + 8px)";
        panel.style.right = "0";
        panel.style.left = "auto";
        panel.style.zIndex = "100002";
        panel.style.pointerEvents = "auto";
    }
    function ensureNotifBackdrop(doc) {
        doc = resolveNotifDoc(doc);
        let backdrop = doc.getElementById("ref-notif-backdrop");
        if (!backdrop) {
            backdrop = doc.createElement("div");
            backdrop.id = "ref-notif-backdrop";
            backdrop.className = "ref-notif-backdrop";
            doc.body.appendChild(backdrop);
        }
        return backdrop;
    }
    function setUserOpen(open, doc) {
        doc = resolveNotifDoc(doc);
        const wrap = doc.getElementById("ref-user-menu");
        const anchor = doc.getElementById("ref-user-anchor");
        const panel = doc.getElementById("ref-user-panel");
        if (!anchor || !panel) return;
        if (open) {
            setNotifOpen(false, doc);
            panel.classList.add("ref-open");
            wrap?.classList.add("ref-open");
            anchor.setAttribute("aria-expanded", "true");
            positionUserPanel(doc);
        } else {
            panel.classList.remove("ref-open");
            wrap?.classList.remove("ref-open");
            anchor.setAttribute("aria-expanded", "false");
        }
    }
    function setNotifOpen(open, doc) {
        doc = resolveNotifDoc(doc);
        const wrap = doc.getElementById("ref-notif-wrap");
        const bell = doc.getElementById("ref-notif-anchor");
        const panel = doc.getElementById("ref-notif-panel");
        if (!bell || !panel) return;
        if (open) {
            setUserOpen(false, doc);
            panel.classList.add("ref-open");
            wrap?.classList.add("ref-open");
            bell.setAttribute("aria-expanded", "true");
            positionNotifPanel(doc);
            wireMarkAllButtons(doc);
        } else {
            panel.classList.remove("ref-open");
            wrap?.classList.remove("ref-open");
            bell.setAttribute("aria-expanded", "false");
        }
    }
    function closeAllMenus(doc) {
        setNotifOpen(false, doc);
        setUserOpen(false, doc);
        doc.querySelectorAll("#ref-notif-backdrop").forEach((backdrop) => backdrop.remove());
    }
    function bindNotifDocument(doc) {
        if (!doc || !doc.documentElement || doc.documentElement.dataset.refUiBound === "2") return;
        doc.documentElement.dataset.refUiBound = "2";
        doc.addEventListener("click", (e) => {
            const close = e.target.closest("[data-ref-notif-close]");
            if (close) {
                e.preventDefault();
                e.stopPropagation();
                setNotifOpen(false, doc);
                return;
            }
            const mark = e.target.closest("[data-ref-notif-mark-all]");
            if (mark) {
                e.preventDefault();
                e.stopPropagation();
                setNotifOpen(false, doc);
                clickNotifMarkAll();
                return;
            }
            const bell = e.target.closest("#ref-notif-anchor");
            if (bell) {
                e.preventDefault();
                e.stopPropagation();
                const panel = doc.getElementById("ref-notif-panel");
                setNotifOpen(!panel?.classList.contains("ref-open"), doc);
                return;
            }
            const userBtn = e.target.closest("#ref-user-anchor");
            if (userBtn) {
                e.preventDefault();
                e.stopPropagation();
                const panel = doc.getElementById("ref-user-panel");
                setUserOpen(!panel?.classList.contains("ref-open"), doc);
                return;
            }
            if (e.target.closest("#ref-user-panel")) {
                return;
            }
            if (e.target.closest("#ref-notif-backdrop")) {
                closeAllMenus(doc);
                return;
            }
            const openPanel = doc.getElementById("ref-notif-panel");
            if (openPanel?.classList.contains("ref-open") && !e.target.closest("#ref-notif-panel")) {
                setNotifOpen(false, doc);
            }
        });
        doc.addEventListener("keydown", (e) => {
            if (e.key === "Escape") closeAllMenus(doc);
        });
    }
    function wireMarkAllButtons(doc) {
        if (!doc) return;
        doc.querySelectorAll("[data-ref-notif-mark-all]").forEach((btn) => {
            if (btn.dataset.refMarkWired) return;
            btn.dataset.refMarkWired = "1";
            btn.addEventListener("click", (e) => {
                e.preventDefault();
                e.stopPropagation();
                setNotifOpen(false, doc);
                clickNotifMarkAll();
            });
            btn.addEventListener("keydown", (e) => {
                if (e.key !== "Enter" && e.key !== " ") return;
                e.preventDefault();
                e.stopPropagation();
                setNotifOpen(false, doc);
                clickNotifMarkAll();
            });
        });
    }
    function wireCloseButtons(doc) {
        if (!doc) return;
        doc.querySelectorAll("[data-ref-notif-close]").forEach((btn) => {
            if (btn.dataset.refCloseWired) return;
            btn.dataset.refCloseWired = "1";
            btn.addEventListener("click", (e) => {
                e.preventDefault();
                e.stopPropagation();
                setNotifOpen(false, doc);
            });
        });
    }
    function wireBellAnchors(doc) {
        if (!doc) return;
        doc.querySelectorAll("#ref-notif-anchor").forEach((bell) => {
            if (bell.dataset.refBellWired) return;
            bell.dataset.refBellWired = "1";
            bell.addEventListener("click", (e) => {
                e.preventDefault();
                e.stopPropagation();
                window.refToggleNotif(e);
            });
            bell.addEventListener("keydown", (e) => {
                if (e.key !== "Enter" && e.key !== " ") return;
                e.preventDefault();
                window.refToggleNotif(e);
            });
        });
    }
    function bindAllNotifDocuments() {
        appDocuments().forEach(bindNotifDocument);
        appDocuments().forEach(wireBellAnchors);
        appDocuments().forEach(wireMarkAllButtons);
        appDocuments().forEach(wireCloseButtons);
    }
    window.refMarkAllNotif = function (e) {
        if (e) {
            e.preventDefault();
            e.stopPropagation();
        }
        const doc = resolveNotifDoc(e && e.target);
        setNotifOpen(false, doc);
        clickNotifMarkAll();
        return false;
    };
    window.refToggleNotif = function (e) {
        if (e) {
            e.preventDefault();
            e.stopPropagation();
        }
        const doc = resolveNotifDoc(e && e.target);
        const panel = doc.getElementById("ref-notif-panel");
        setNotifOpen(!panel || !panel.classList.contains("ref-open"), doc);
    };
    window.refCloseNotif = function (e) {
        const doc = resolveNotifDoc(e && e.target);
        setNotifOpen(false, doc);
    };
    function init() {
        appDocuments().forEach(cleanupStalePortals);
        bindAllNotifDocuments();
        loadLucide(() => {
            if (window.lucide && window.lucide.createIcons) {
                window.lucide.createIcons({ attrs: { class: "ref-icon", "stroke-width": 2 } });
            }
            nav();
            appDocuments().forEach((doc) => positionNotifPanel(doc));
            appDocuments().forEach((doc) => positionUserPanel(doc));
        });
    }
    bindAllNotifDocuments();
    let tm;
    window.addEventListener("resize", () => { clearTimeout(tm); tm = setTimeout(init, 50); });
    window.addEventListener("scroll", () => {
        clearTimeout(tm);
        tm = setTimeout(() => {
            appDocuments().forEach((doc) => {
                const panel = doc.getElementById("ref-notif-panel");
                if (panel?.classList.contains("ref-open")) positionNotifPanel(doc);
                const userPanel = doc.getElementById("ref-user-panel");
                if (userPanel?.classList.contains("ref-open")) positionUserPanel(doc);
            });
            init();
        }, 50);
    }, true);
    new MutationObserver(() => { clearTimeout(tm); tm = setTimeout(init, 100); })
        .observe(document.body, { childList: true, subtree: true });
    init();
})();
"""


def _inject_parent_runtime() -> None:
    """Inject UI runtime into every Streamlit document (main frame + parent)."""
    bridge = (
        "<script>(function(){"
        "function run(doc){"
        "if(!doc||!doc.body)return;"
        f"var code={json.dumps(PARENT_RUNTIME_JS)};"
        "try{(new Function(code))();}catch(e){console.warn('ref-ui',e);}"
        "}"
        "function bootAll(){"
        "run(document);"
        "try{run(window.parent&&window.parent.document);}catch(e){}"
        "try{"
        "document.querySelectorAll('iframe').forEach(function(frame){"
        "try{run(frame.contentDocument);}catch(e){}"
        "});"
        "}catch(e){}"
        "}"
        "bootAll();"
        "setTimeout(bootAll,120);"
        "setTimeout(bootAll,600);"
        "try{"
        "new MutationObserver(function(){bootAll();})"
        ".observe(document.documentElement,{childList:true,subtree:true});"
        "}catch(e){}"
        "})();</script>"
    )
    components.html(bridge, height=0, width=0)


def inject_global_css(*, auth_mode: bool = False) -> None:
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
    if auth_mode:
        st.markdown(AUTH_MODE_CSS, unsafe_allow_html=True)
    else:
        _inject_parent_runtime()
