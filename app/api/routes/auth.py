"""User authentication routes.

* ``POST /auth/register`` - create an account (username+email+password).
* ``POST /auth/login``    - verify credentials, set session + rotate cookies.
* ``POST /auth/logout``   - revoke the current session.
* ``GET  /auth/me``       - return the current user row or null.
* ``GET  /auth/csrf``     - issue a fresh CSRF token (cookie + body).
* ``POST /auth/change-password`` - requires CSRF + current password.
* ``POST /auth/delete-account``  - requires CSRF + password; cascades.
"""
from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.deps import get_current_user, get_session_secret, get_users_db_path, require_csrf
from app.auth import csrf, passwords, ratelimit, sessions
from app.auth.db import connect
from app.models.schemas import (
    ChangePasswordRequest,
    CsrfResponse,
    DeleteAccountRequest,
    LoginRequest,
    RegisterRequest,
    UserPublic,
)


router = APIRouter(prefix="/auth", tags=["auth"])


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _client_ip(request: Request) -> str:
    client = request.client
    return client.host if client else "unknown"


def _set_session_cookie(
    response: Response, *, raw_token: str, secure: bool
) -> None:
    kwargs = sessions.cookie_kwargs(
        max_age_seconds=int(sessions.SLIDING_LIFETIME.total_seconds()),
        secure=secure,
    )
    response.set_cookie(value=raw_token, **kwargs)


def _clear_session_cookie(response: Response, *, secure: bool) -> None:
    response.delete_cookie(
        key=sessions.SESSION_COOKIE_NAME,
        path="/",
        samesite="strict",
        secure=secure,
        httponly=True,
    )


def _set_csrf_cookie(response: Response, *, secret: str, secure: bool) -> str:
    token = csrf.issue_csrf_token(secret)
    kwargs = csrf.csrf_cookie_kwargs(secure=secure)
    response.set_cookie(value=token, **kwargs)
    return token


def _user_public(user_row: dict[str, Any]) -> UserPublic:
    return UserPublic(
        id=int(user_row["id"]),
        username=user_row["username"],
        email=user_row["email"],
        created_at=user_row["created_at"],
        last_login_at=user_row.get("last_login_at"),
    )


def _lookup_user_by_name(
    conn: sqlite3.Connection, username: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT id, username, email, password_hash, created_at, last_login_at "
        "FROM users WHERE username = ?",
        (username,),
    ).fetchone()
    return dict(row) if row else None


@router.get("/csrf", response_model=CsrfResponse)
def csrf_token(
    response: Response,
    session_secret: str = Depends(get_session_secret),
) -> CsrfResponse:
    """Issue a fresh CSRF cookie + body token.

    Clients call this on page load and before every state-changing request
    (cheap: it's just an itsdangerous sign).
    """
    from app.config import get_settings
    secure = get_settings().cookie_secure
    token = _set_csrf_cookie(response, secret=session_secret, secure=secure)
    return CsrfResponse(csrf_token=token)


@router.get("/me", response_model=UserPublic | None)
def me(user: dict[str, Any] | None = Depends(get_current_user(required=False))):
    if user is None:
        return None
    return _user_public(user)


@router.post("/register", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
def register(
    payload: RegisterRequest,
    response: Response,
    request: Request,
    users_db_path: Path = Depends(get_users_db_path),
    session_secret: str = Depends(get_session_secret),
) -> UserPublic:
    """Create account and start a session in one turn.

    Keeps the demo UX smooth - a fresh user doesn't have to log in
    immediately after registering. The session is brand new so rotation
    invariants still hold.
    """
    password_hash = passwords.hash_password(payload.password)
    now = _now_iso()
    with connect(users_db_path) as conn:
        try:
            cursor = conn.execute(
                "INSERT INTO users (username, email, password_hash, created_at, last_login_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (payload.username, payload.email, password_hash, now, now),
            )
            user_id = int(cursor.lastrowid)
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="username or email already taken",
            )

    raw_token, _ = sessions.create_session(users_db_path, user_id)
    from app.config import get_settings
    secure = get_settings().cookie_secure
    _set_session_cookie(response, raw_token=raw_token, secure=secure)
    _set_csrf_cookie(response, secret=session_secret, secure=secure)
    return UserPublic(
        id=user_id,
        username=payload.username,
        email=payload.email,
        created_at=now,
        last_login_at=now,
    )


@router.post("/login", response_model=UserPublic)
def login(
    payload: LoginRequest,
    response: Response,
    request: Request,
    users_db_path: Path = Depends(get_users_db_path),
    session_secret: str = Depends(get_session_secret),
) -> UserPublic:
    ip = _client_ip(request)

    if not ratelimit.check_allowed(payload.username, ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="too many login attempts; try again later",
        )

    now_iso = _now_iso()
    with connect(users_db_path) as conn:
        user_row = _lookup_user_by_name(conn, payload.username)
        ok = user_row is not None and passwords.verify_password(
            user_row["password_hash"], payload.password
        )
        conn.execute(
            "INSERT INTO user_login_attempts (username, ip, success, created_at) "
            "VALUES (?, ?, ?, ?)",
            (payload.username, ip, 1 if ok else 0, now_iso),
        )
        if ok:
            conn.execute(
                "UPDATE users SET last_login_at = ? WHERE id = ?",
                (now_iso, user_row["id"]),
            )
        conn.commit()

    if not ok:
        ratelimit.record_failure(payload.username, ip)
        # Constant-time-ish: we only branch on ok after hashing always ran.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        )

    ratelimit.record_success(payload.username)
    raw_token, _ = sessions.create_session(users_db_path, int(user_row["id"]))
    from app.config import get_settings
    secure = get_settings().cookie_secure
    _set_session_cookie(response, raw_token=raw_token, secure=secure)
    _set_csrf_cookie(response, secret=session_secret, secure=secure)
    user_row["last_login_at"] = now_iso
    return _user_public(user_row)


@router.post("/logout")
def logout(
    response: Response,
    user: dict[str, Any] | None = Depends(get_current_user(required=False)),
    _csrf: None = Depends(require_csrf),
    users_db_path: Path = Depends(get_users_db_path),
) -> dict[str, Any]:
    from app.config import get_settings
    secure = get_settings().cookie_secure
    if user is not None:
        sessions.revoke_session(users_db_path, user.get("_session_token"))
    _clear_session_cookie(response, secure=secure)
    return {"ok": True}


@router.post("/change-password", response_model=UserPublic)
def change_password(
    payload: ChangePasswordRequest,
    response: Response,
    user: dict[str, Any] = Depends(get_current_user(required=True)),
    _csrf: None = Depends(require_csrf),
    users_db_path: Path = Depends(get_users_db_path),
    session_secret: str = Depends(get_session_secret),
) -> UserPublic:
    with connect(users_db_path) as conn:
        row = conn.execute(
            "SELECT password_hash FROM users WHERE id = ?",
            (user["id"],),
        ).fetchone()
        if row is None or not passwords.verify_password(
            row["password_hash"], payload.current_password
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="current password does not match",
            )
        new_hash = passwords.hash_password(payload.new_password)
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (new_hash, user["id"]),
        )
        # Revoke every live session; client will get a fresh one below.
        sessions.rotate_user_sessions(conn, int(user["id"]))
        conn.commit()

    raw_token, _ = sessions.create_session(users_db_path, int(user["id"]))
    from app.config import get_settings
    secure = get_settings().cookie_secure
    _set_session_cookie(response, raw_token=raw_token, secure=secure)
    _set_csrf_cookie(response, secret=session_secret, secure=secure)
    return _user_public(user)


@router.post("/delete-account")
def delete_account(
    payload: DeleteAccountRequest,
    response: Response,
    user: dict[str, Any] = Depends(get_current_user(required=True)),
    _csrf: None = Depends(require_csrf),
    users_db_path: Path = Depends(get_users_db_path),
) -> dict[str, Any]:
    with connect(users_db_path) as conn:
        row = conn.execute(
            "SELECT password_hash FROM users WHERE id = ?",
            (user["id"],),
        ).fetchone()
        if row is None or not passwords.verify_password(
            row["password_hash"], payload.password
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="password does not match",
            )
        # ON DELETE CASCADE handles sessions + interactions.
        conn.execute("DELETE FROM users WHERE id = ?", (user["id"],))
        conn.commit()

    from app.config import get_settings
    secure = get_settings().cookie_secure
    _clear_session_cookie(response, secure=secure)
    return {"ok": True}
