"""Review Cycle role-to-page access maps (embedded in OD Collection app)."""

from __future__ import annotations

ROLES = {
    "admin": [
        "project_dashboard",
        "cleaning",
        "review",
        "supervisor",
        "history",
        "admin",
        "demographic",
        "demographic_config",
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
