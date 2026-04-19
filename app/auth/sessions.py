"""Opaque server-side sessions with sliding + absolute expiry.

The raw 256-bit token only ever lives in the client's HttpOnly cookie and on
the wire. What we store server-side is a SHA-256 hash so a read leak of the
users DB cannot forge live sessions.

Expiry:
* **sliding**: every successful ``resolve_session`` touches ``last_seen_at``
  and pushes ``expires_at`` 30 days out, so active users aren't kicked out.
* **absolute**: ``created_at + 90 days`` is a hard ceiling; sessions older
  than that are treated as revoked regardless of activity.

Rotation on login: ``rotate_user_sessions`` revokes every existing live session
for a user when they log in anew, so a stolen token can't co-exist with the
user's fresh session silently.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import secrets
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from app.auth.db import connect


SESSION_COOKIE_NAME = "session"
SLIDING_LIFETIME = dt.timedelta(days=30)
ABSOLUTE_LIFETIME = dt.timedelta(days=90)
_TOKEN_BYTES = 32  # 256 bits


@dataclass(slots=True)
class Session:
    user_id: int
    token_hash: str
    created_at: dt.datetime
    last_seen_at: dt.datetime
    expires_at: dt.datetime


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _generate_token() -> str:
    return secrets.token_urlsafe(_TOKEN_BYTES)


def _parse_dt(value: str | None) -> dt.datetime | None:
    if value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def rotate_user_sessions(conn: sqlite3.Connection, user_id: int) -> None:
    """Revoke every live session for ``user_id`` (called on login + password change)."""
    conn.execute(
        "UPDATE user_sessions SET revoked = 1 WHERE user_id = ? AND revoked = 0",
        (user_id,),
    )


def create_session(users_db_path: Path, user_id: int) -> tuple[str, Session]:
    """Return ``(raw_token, session)``. The raw token is what goes in the cookie."""
    now = _now()
    raw = _generate_token()
    token_hash = _hash_token(raw)
    expires_at = now + SLIDING_LIFETIME
    with connect(users_db_path) as conn:
        rotate_user_sessions(conn, user_id)
        conn.execute(
            """
            INSERT INTO user_sessions
                (token_hash, user_id, created_at, last_seen_at, expires_at, revoked)
            VALUES (?, ?, ?, ?, ?, 0)
            """,
            (
                token_hash,
                user_id,
                now.isoformat(),
                now.isoformat(),
                expires_at.isoformat(),
            ),
        )
        conn.commit()
    return raw, Session(
        user_id=user_id,
        token_hash=token_hash,
        created_at=now,
        last_seen_at=now,
        expires_at=expires_at,
    )


def resolve_session(users_db_path: Path, raw_token: str | None) -> Session | None:
    """Return the live session for ``raw_token`` or ``None``.

    Touches ``last_seen_at`` and extends ``expires_at`` (sliding). Rejects
    sessions that are revoked, past ``expires_at``, or past the absolute
    ``created_at + ABSOLUTE_LIFETIME`` ceiling.
    """
    if not raw_token:
        return None
    token_hash = _hash_token(raw_token)
    now = _now()
    with connect(users_db_path) as conn:
        row = conn.execute(
            """
            SELECT token_hash, user_id, created_at, last_seen_at, expires_at, revoked
            FROM user_sessions
            WHERE token_hash = ?
            """,
            (token_hash,),
        ).fetchone()
        if row is None:
            return None
        if int(row["revoked"]) == 1:
            return None

        created_at = _parse_dt(row["created_at"])
        expires_at = _parse_dt(row["expires_at"])
        if created_at is None or expires_at is None:
            return None
        if now >= expires_at:
            return None
        if now >= created_at + ABSOLUTE_LIFETIME:
            return None

        new_last_seen = now
        new_expires = min(now + SLIDING_LIFETIME, created_at + ABSOLUTE_LIFETIME)
        conn.execute(
            "UPDATE user_sessions SET last_seen_at = ?, expires_at = ? "
            "WHERE token_hash = ?",
            (new_last_seen.isoformat(), new_expires.isoformat(), token_hash),
        )
        conn.commit()
        return Session(
            user_id=int(row["user_id"]),
            token_hash=token_hash,
            created_at=created_at,
            last_seen_at=new_last_seen,
            expires_at=new_expires,
        )


def revoke_session(users_db_path: Path, raw_token: str | None) -> None:
    """Revoke the session identified by ``raw_token`` (idempotent)."""
    if not raw_token:
        return
    token_hash = _hash_token(raw_token)
    with connect(users_db_path) as conn:
        conn.execute(
            "UPDATE user_sessions SET revoked = 1 WHERE token_hash = ?",
            (token_hash,),
        )
        conn.commit()


def cookie_kwargs(
    *,
    max_age_seconds: int,
    secure: bool,
) -> dict:
    """The fastapi ``response.set_cookie`` kwargs we want for the session cookie."""
    return dict(
        key=SESSION_COOKIE_NAME,
        httponly=True,
        samesite="strict",
        secure=secure,
        path="/",
        max_age=max_age_seconds,
    )
