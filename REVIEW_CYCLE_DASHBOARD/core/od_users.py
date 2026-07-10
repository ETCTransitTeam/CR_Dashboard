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


def team_members(team: str) -> list[str]:
    """Workflow team roster sourced from OD user_table."""
    team_key = str(team or "").lower()
    if team_key == "cleaning":
        return od_users_by_role("CLEANING")
    if team_key == "field":
        return od_users_by_role("USER")
    if team_key == "review":
        return super_admin_display_names()
    return od_users_by_role(team_key.upper())


def admin_recipients() -> list[str]:
    """Super admins plus OD ADMIN users (for notifications)."""
    names = super_admin_display_names() + od_users_by_role("ADMIN")
    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        key = name.lower()
        if key not in seen:
            seen.add(key)
            ordered.append(name)
    return ordered
