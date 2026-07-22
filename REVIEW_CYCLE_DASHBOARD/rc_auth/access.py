"""Review Cycle role-to-page access maps (embedded in OD Collection app)."""

from __future__ import annotations

CLEANING_HEAD_EMAIL = "xarin.ch2000@gmail.com"

# Extra emails that can manage cleaning assignments (view + unassign), besides super admins.
ASSIGNMENT_MANAGER_EMAILS = {
    CLEANING_HEAD_EMAIL.strip().lower(),
}

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


def is_super_admin_user(user: dict | None) -> bool:
    """True for platform super admins (not the cleaning-head special email alone)."""
    if not user:
        return False
    if user.get("is_super_admin") or user.get("IS_SUPER_ADMIN"):
        return True
    email = user_email(user)
    if not email:
        return False
    try:
        from authentication.auth import is_super_admin

        return bool(is_super_admin(email))
    except Exception:
        return False


def can_manage_cleaning_assignments(user: dict | None) -> bool:
    """Super admins + special allowlist (e.g. xarin) can view/unassign cleaning assignments."""
    email = user_email(user)
    if not email:
        return False
    if email in ASSIGNMENT_MANAGER_EMAILS:
        return True
    return is_super_admin_user(user)
