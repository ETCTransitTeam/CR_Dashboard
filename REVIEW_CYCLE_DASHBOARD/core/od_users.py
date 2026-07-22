"""Read staff directory from OD Collection user.user_table."""

from __future__ import annotations

from core.config import env
from core.snowflake_conn import fetch_df


def _od_user_table() -> str:
    database = env("SNOWFLAKE_DATABASE")
    if not database:
        raise RuntimeError("SNOWFLAKE_DATABASE is not set")
    return f"{database}.user.user_table"


def _super_admin_emails() -> list[str]:
    try:
        from authentication.auth import SUPER_ADMIN_EMAILS

        return list(SUPER_ADMIN_EMAILS)
    except ImportError:
        return []


def _is_super_admin(email: str) -> bool:
    try:
        from authentication.auth import is_super_admin

        return is_super_admin(email)
    except ImportError:
        return email.lower() in {e.lower() for e in _super_admin_emails()}


def od_users_by_role(role: str) -> list[str]:
    """Active OD users with the given role; returns display names (username or email)."""
    role_u = str(role or "").upper()
    df = fetch_df(
        f"""
        SELECT COALESCE(USERNAME, EMAIL) AS NAME
        FROM {_od_user_table()}
        WHERE UPPER(TRIM(ROLE)) = %s AND IS_ACTIVE = TRUE
        ORDER BY NAME
        """,
        (role_u,),
        schema="user",
    )
    if df.empty:
        return []
    return df["NAME"].dropna().astype(str).tolist()


def super_admin_display_names() -> list[str]:
    """The three super admins, resolved to usernames when present in user_table."""
    emails = _super_admin_emails()
    if not emails:
        return []
    placeholders = ", ".join(["%s"] * len(emails))
    df = fetch_df(
        f"""
        SELECT EMAIL, COALESCE(USERNAME, EMAIL) AS NAME
        FROM {_od_user_table()}
        WHERE LOWER(TRIM(EMAIL)) IN ({placeholders})
        ORDER BY NAME
        """,
        tuple(e.lower() for e in emails),
        schema="user",
    )
    if not df.empty:
        return df["NAME"].dropna().astype(str).tolist()
    return [e.split("@")[0] for e in emails]


def privileged_assignee_names() -> set[str]:
    """Display names for super admins + OD ADMIN users."""
    names = super_admin_display_names() + od_users_by_role("ADMIN")
    return {n.strip() for n in names if str(n).strip()}


def _dedupe_names(names: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        key = str(name).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(str(name).strip())
    return ordered


def cleaning_assignee_options(*, include_privileged: bool = False) -> list[str]:
    """People who can receive cleaning assignments.

    - Default / cleaning head: CLEANING role only (never admin or super admin).
    - Super admin: cleaning staff plus admins / super admins.
    """
    cleaners = od_users_by_role("CLEANING")
    privileged = privileged_assignee_names()
    privileged_lower = {n.lower() for n in privileged}

    # Always strip admins/SAs out of the cleaning-only roster.
    cleaning_only = [n for n in cleaners if n.strip().lower() not in privileged_lower]

    if not include_privileged:
        return _dedupe_names(cleaning_only)

    return _dedupe_names(cleaning_only + sorted(privileged))


def team_members(team: str) -> list[str]:
    """Workflow team roster sourced from OD user_table."""
    team_key = str(team or "").lower()
    if team_key == "cleaning":
        # Cleaning-only by default; call cleaning_assignee_options(include_privileged=True)
        # when a super admin needs the expanded roster.
        return cleaning_assignee_options(include_privileged=False)
    if team_key == "field":
        return od_users_by_role("USER")
    if team_key == "review":
        return super_admin_display_names()
    return od_users_by_role(team_key.upper())


def admin_recipients() -> list[str]:
    """Super admins plus OD ADMIN users (for notifications)."""
    return _dedupe_names(super_admin_display_names() + od_users_by_role("ADMIN"))
