"""Stateless double-submit CSRF tokens.

The client gets a signed token via ``GET /auth/csrf`` that we also set as a
non-HttpOnly cookie (so JS can read it). State-changing routes require the
same token echoed in ``X-CSRF-Token``. Because the token is signed with the
same ``LISTINGS_SESSION_SECRET`` we use for the session store, we catch
forgeries without keeping CSRF state on the server.

Why double-submit: the session cookie is HttpOnly + SameSite=Strict, so a
cross-origin POST can't carry it. But defence-in-depth matters - older
browsers, buggy proxies, sub-domain hijacks etc. motivate the second check.
"""
from __future__ import annotations

import hmac
import secrets

from itsdangerous import BadSignature, URLSafeTimedSerializer


CSRF_COOKIE_NAME = "csrf_token"
CSRF_HEADER_NAME = "X-CSRF-Token"
_CSRF_SALT = "csrf"
_CSRF_MAX_AGE_S = 60 * 60 * 24  # 24h, fresh token fetched lazily by the client


def _serializer(secret: str) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret_key=secret, salt=_CSRF_SALT)


def issue_csrf_token(secret: str) -> str:
    """Fresh signed CSRF token. Each call produces a new nonce."""
    nonce = secrets.token_urlsafe(16)
    return _serializer(secret).dumps(nonce)


def validate_csrf(
    *,
    secret: str,
    cookie_value: str | None,
    header_value: str | None,
) -> bool:
    """Return True iff both values are present, signed correctly, and equal."""
    if not cookie_value or not header_value:
        return False
    if not hmac.compare_digest(cookie_value, header_value):
        return False
    try:
        _serializer(secret).loads(cookie_value, max_age=_CSRF_MAX_AGE_S)
    except BadSignature:
        return False
    return True


def csrf_cookie_kwargs(*, secure: bool) -> dict:
    """Kwargs for the CSRF cookie: readable by JS, but SameSite=Strict."""
    return dict(
        key=CSRF_COOKIE_NAME,
        httponly=False,
        samesite="strict",
        secure=secure,
        path="/",
        max_age=_CSRF_MAX_AGE_S,
    )
