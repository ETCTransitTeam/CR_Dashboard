"""Review Cycle role-to-page access maps (embedded in OD Collection app)."""

from __future__ import annotations

CLEANING_HEAD_EMAIL = "xarin.ch2000@gmail.com"

ROLES = {
    "admin": [
        "project_dashboard",
        "cleaning",
        "review",
        "supervisor",
        "history",
        "admin",
        "demographic_config",
        "demographic",
        "field",
        "manager_dashboard",
        "reviewer_stats",
        "sync_admin",
    ],
    "manager": [
        "project_dashboard",
        "manager_dashboard",
        "reviewer_stats",
        "cleaning",
        "review",
        "supervisor",
        "history",
        "admin",
        "demographic",
        "field",
    ],
    "cleaning": ["project_dashboard", "cleaning", "history"],
    "field": ["project_dashboard", "field"],
}

ROLE_LABELS = {
    "cleaning": "Cleaning Team",
    "field": "Field Team",
    "manager": "Manager",
    "admin": "Admin",
}


def allowed_pages(role: str) -> list[str]:
    return ROLES.get(str(role or "").lower(), [])


def user_email(user: dict | None) -> str:
    if not user:
        return ""
    return str(user.get("EMAIL") or user.get("email") or "").strip().lower()


def is_cleaning_head(user: dict | None) -> bool:
    """True when the logged-in user is the hard-coded cleaning head (not a separate role)."""
    return user_email(user) == CLEANING_HEAD_EMAIL.strip().lower()
