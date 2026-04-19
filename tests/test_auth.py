"""Happy-path auth behaviour: register, login, logout, /me, change-password."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client() -> TestClient:
    from app.main import app
    with TestClient(app) as c:
        yield c


def _csrf(client: TestClient) -> str:
    return client.get("/auth/csrf").json()["csrf_token"]


def test_register_sets_session_and_me(client: TestClient) -> None:
    r = client.post(
        "/auth/register",
        json={"username": "alice", "email": "a@b.co", "password": "hunter222"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["username"] == "alice"
    assert body["email"] == "a@b.co"
    assert "session" in client.cookies  # session cookie set

    me = client.get("/auth/me").json()
    assert me["username"] == "alice"


def test_register_rejects_weak_password(client: TestClient) -> None:
    r = client.post(
        "/auth/register",
        json={"username": "weak", "email": "w@b.co", "password": "onlyletters"},
    )
    assert r.status_code == 422  # pydantic validation


def test_register_rejects_short_username(client: TestClient) -> None:
    r = client.post(
        "/auth/register",
        json={"username": "a", "email": "a@b.co", "password": "hunter222"},
    )
    assert r.status_code == 422


def test_duplicate_register_conflicts(client: TestClient) -> None:
    payload = {"username": "bob", "email": "b@c.co", "password": "hunter222"}
    assert client.post("/auth/register", json=payload).status_code == 201
    client.cookies.clear()
    r = client.post("/auth/register", json=payload)
    assert r.status_code == 409


def test_login_then_logout(client: TestClient) -> None:
    client.post(
        "/auth/register",
        json={"username": "carol", "email": "c@d.co", "password": "hunter222"},
    )
    client.cookies.clear()

    r = client.post(
        "/auth/login",
        json={"username": "carol", "password": "hunter222"},
    )
    assert r.status_code == 200
    assert client.get("/auth/me").json()["username"] == "carol"

    tok = _csrf(client)
    r = client.post("/auth/logout", headers={"X-CSRF-Token": tok})
    assert r.status_code == 200
    assert client.get("/auth/me").json() is None


def test_login_wrong_password_returns_401(client: TestClient) -> None:
    client.post(
        "/auth/register",
        json={"username": "dave", "email": "d@e.co", "password": "hunter222"},
    )
    client.cookies.clear()
    r = client.post(
        "/auth/login",
        json={"username": "dave", "password": "wrong2222"},
    )
    assert r.status_code == 401
    assert r.json()["detail"] == "invalid credentials"


def test_login_unknown_user_returns_401_without_leaking_existence(
    client: TestClient,
) -> None:
    r = client.post(
        "/auth/login",
        json={"username": "ghost", "password": "hunter222"},
    )
    assert r.status_code == 401
    assert r.json()["detail"] == "invalid credentials"


def test_change_password_rotates_sessions(client: TestClient) -> None:
    client.post(
        "/auth/register",
        json={"username": "eve", "email": "e@f.co", "password": "hunter222"},
    )
    tok = _csrf(client)
    r = client.post(
        "/auth/change-password",
        json={"current_password": "hunter222", "new_password": "newPass12"},
        headers={"X-CSRF-Token": tok},
    )
    assert r.status_code == 200

    # Old password no longer works
    client.cookies.clear()
    r = client.post(
        "/auth/login",
        json={"username": "eve", "password": "hunter222"},
    )
    assert r.status_code == 401

    # New password works
    r = client.post(
        "/auth/login",
        json={"username": "eve", "password": "newPass12"},
    )
    assert r.status_code == 200


def test_delete_account_cascades(client: TestClient) -> None:
    client.post(
        "/auth/register",
        json={"username": "frank", "email": "f@g.co", "password": "hunter222"},
    )
    tok = _csrf(client)
    r = client.post(
        "/auth/delete-account",
        json={"password": "hunter222"},
        headers={"X-CSRF-Token": tok},
    )
    assert r.status_code == 200
    assert client.get("/auth/me").json() is None

    # Re-registering with same username/email now succeeds (account gone).
    client.cookies.clear()
    r = client.post(
        "/auth/register",
        json={"username": "frank", "email": "f@g.co", "password": "hunter222"},
    )
    assert r.status_code == 201
