"""FastAPI dependencies for the auth-aware routes.

The idea: route handlers declare ``user = Depends(get_current_user())`` or
``_ = Depends(require_csrf)`` and we do the heavy lifting once here.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Callable

from fastapi import Depends, HTTPException, Request, status

from app.auth import csrf, sessions
from app.auth.db import connect
from app.config import Settings, get_settings


def get_users_db_path(settings: Settings = Depends(get_settings)) -> Path:
    return settings.users_db_path


def get_session_secret(settings: Settings = Depends(get_settings)) -> str:
    return settings.session_secret


def _load_user(conn: sqlite3.Connection, user_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT id, username, email, created_at, last_login_at "
        "FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def get_current_user(required: bool = False) -> Callable[..., dict[str, Any] | None]:
    """Factory: returns a dependency that resolves the session cookie to a user row.

    ``required=True`` raises ``401`` when the caller is anonymous; ``False``
    returns ``None`` (used by ``/listings`` where anonymous browsing is OK).
    """

    def _dep(
        request: Request,
        users_db_path: Path = Depends(get_users_db_path),
    ) -> dict[str, Any] | None:
        raw = request.cookies.get(sessions.SESSION_COOKIE_NAME)
        session = sessions.resolve_session(users_db_path, raw)
        if session is None:
            if required:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="authentication required",
                )
            return None
        with connect(users_db_path) as conn:
            user = _load_user(conn, session.user_id)
        if user is None:
            # Session row exists but the user doesn't (e.g. DB was wiped).
            # Treat as anonymous and revoke so the client stops sending it.
            sessions.revoke_session(users_db_path, raw)
            if required:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="authentication required",
                )
            return None
        user["_session_token"] = raw  # for logout handlers
        return user

    return _dep


def require_csrf(
    request: Request,
    session_secret: str = Depends(get_session_secret),
) -> None:
    cookie_value = request.cookies.get(csrf.CSRF_COOKIE_NAME)
    header_value = request.headers.get(csrf.CSRF_HEADER_NAME)
    if not csrf.validate_csrf(
        secret=session_secret,
        cookie_value=cookie_value,
        header_value=header_value,
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF check failed",
        )
