"""Security invariants: CSRF, rate-limit, session expiry, password hashing."""
from __future__ import annotations

import datetime as dt

import pytest
from fastapi.testclient import TestClient

from app.auth import passwords, ratelimit, sessions


@pytest.fixture()
def client() -> TestClient:
    from app.main import app
    with TestClient(app) as c:
        yield c


# ---------- passwords --------------------------------------------------------


def test_password_roundtrip() -> None:
    h = passwords.hash_password("hunter222")
    assert passwords.verify_password(h, "hunter222") is True
    assert passwords.verify_password(h, "nottheone") is False


def test_password_hash_is_argon2id() -> None:
    h = passwords.hash_password("hunter222")
    # argon2-cffi encodes the algorithm in the hash string.
    assert h.startswith("$argon2id$")


def test_password_distinct_hashes_for_same_password() -> None:
    # Argon2 uses a fresh random salt per hash, so two hashes of the same
    # password never collide. This is what prevents rainbow tables.
    h1 = passwords.hash_password("hunter222")
    h2 = passwords.hash_password("hunter222")
    assert h1 != h2
    assert passwords.verify_password(h1, "hunter222")
    assert passwords.verify_password(h2, "hunter222")


# ---------- CSRF -------------------------------------------------------------


def _register(client: TestClient, username: str = "csrftest") -> None:
    r = client.post(
        "/auth/register",
        json={"username": username, "email": f"{username}@x.co", "password": "hunter222"},
    )
    assert r.status_code == 201, r.text


def test_csrf_rejects_missing_token(client: TestClient) -> None:
    _register(client)
    r = client.post("/auth/logout")
    assert r.status_code == 403


def test_csrf_rejects_tampered_token(client: TestClient) -> None:
    _register(client)
    r = client.post("/auth/logout", headers={"X-CSRF-Token": "forged-nonsense"})
    assert r.status_code == 403


def test_csrf_rejects_mismatched_cookie_and_header(client: TestClient) -> None:
    _register(client)
    # Valid token - but pretend the attacker only has the header, not the cookie.
    good = client.get("/auth/csrf").json()["csrf_token"]
    client.cookies.delete("csrf_token")
    r = client.post("/auth/logout", headers={"X-CSRF-Token": good})
    assert r.status_code == 403


def test_interactions_require_csrf(client: TestClient) -> None:
    _register(client, "intuser")
    # Grab a real listing_id
    from app.config import get_settings
    import sqlite3
    with sqlite3.connect(get_settings().db_path) as db:
        row = db.execute("SELECT listing_id FROM listings LIMIT 1").fetchone()
    assert row is not None
    lid = row[0]

    # No CSRF header -> 403
    r = client.post(
        "/me/interactions", json={"listing_id": lid, "kind": "save"}
    )
    assert r.status_code == 403


# ---------- rate limiting ----------------------------------------------------


def test_login_ratelimit_blocks_after_10_failures(client: TestClient) -> None:
    _register(client, "ratelimituser")
    client.cookies.clear()
    # 10 failed attempts
    for _ in range(10):
        r = client.post(
            "/auth/login",
            json={"username": "ratelimituser", "password": "wrong222x"},
        )
        assert r.status_code == 401
    # 11th should be rate-limited (429) regardless of correct password
    r = client.post(
        "/auth/login",
        json={"username": "ratelimituser", "password": "hunter222"},
    )
    assert r.status_code == 429


def test_successful_login_clears_username_counter(client: TestClient) -> None:
    _register(client, "clearuser")
    client.cookies.clear()
    # 9 fails - still under the 10 threshold
    for _ in range(9):
        client.post(
            "/auth/login",
            json={"username": "clearuser", "password": "wrong111x"},
        )
    # Successful login clears the counter
    r = client.post(
        "/auth/login",
        json={"username": "clearuser", "password": "hunter222"},
    )
    assert r.status_code == 200
    client.cookies.clear()
    # Fresh 10 fails should again need to accumulate before 429
    for _ in range(10):
        r = client.post(
            "/auth/login",
            json={"username": "clearuser", "password": "wrong333x"},
        )
    assert r.status_code == 401  # tenth one was 401, not 429


# ---------- session flags ----------------------------------------------------


def test_session_cookie_is_httponly_samesite_strict(client: TestClient) -> None:
    _register(client, "cookie_user")
    # Look at the Set-Cookie header on the /auth/me request chain
    r = client.post(
        "/auth/login",
        json={"username": "cookie_user", "password": "hunter222"},
    )
    # TestClient surfaces raw Set-Cookie headers
    set_cookie_headers = [
        v for (k, v) in r.headers.raw if k.decode().lower() == "set-cookie"
    ]
    session_headers = [h for h in set_cookie_headers if b"session=" in h]
    assert session_headers, "no session cookie set on login"
    blob = session_headers[0].decode().lower()
    assert "httponly" in blob
    assert "samesite=strict" in blob


# ---------- session expiry + revocation -------------------------------------


def test_session_expiry_rejects_expired_token(tmp_path, monkeypatch) -> None:
    """A session past its expires_at can no longer be resolved even if
    the cookie hash still matches a row in the table."""
    users_db = tmp_path / "users.db"
    from app.auth.db import bootstrap_users_db
    bootstrap_users_db(users_db)
    # Insert a user directly
    from app.auth.db import connect
    with connect(users_db) as conn:
        conn.execute(
            "INSERT INTO users (username, email, password_hash, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("a", "a@b.co", "hash", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
    raw, sess = sessions.create_session(users_db, user_id=1)
    # Forcibly move this session's expires_at into the past
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat()
    with connect(users_db) as conn:
        conn.execute(
            "UPDATE user_sessions SET expires_at = ? WHERE token_hash = ?",
            (past, sess.token_hash),
        )
        conn.commit()
    assert sessions.resolve_session(users_db, raw) is None


def test_session_rotation_on_new_login(tmp_path) -> None:
    """A fresh ``create_session`` revokes the user's prior live session."""
    users_db = tmp_path / "users.db"
    from app.auth.db import bootstrap_users_db, connect
    bootstrap_users_db(users_db)
    with connect(users_db) as conn:
        conn.execute(
            "INSERT INTO users (username, email, password_hash, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("a", "a@b.co", "hash", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
    raw_first, _ = sessions.create_session(users_db, user_id=1)
    raw_second, _ = sessions.create_session(users_db, user_id=1)
    # The first token should now resolve to None (its row is revoked).
    assert sessions.resolve_session(users_db, raw_first) is None
    # The second token is the only live session.
    assert sessions.resolve_session(users_db, raw_second) is not None
