"""Argon2id password hashing wrapper.

Tuned to ~100 ms / hash on a laptop (time_cost=3, memory_cost=64 MiB,
parallelism=4). ``argon2-cffi`` already uses constant-time comparison, a
random 16-byte salt per hash, and encodes all parameters inside the returned
string so we can bump costs later without losing old hashes.
"""
from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError


# Singleton: argon2-cffi's PasswordHasher is thread-safe and cheap to reuse.
_HASHER = PasswordHasher(
    time_cost=3,
    memory_cost=64 * 1024,  # 64 MiB
    parallelism=4,
    hash_len=32,
    salt_len=16,
)


def hash_password(plain: str) -> str:
    """Return an argon2id-encoded hash string for ``plain``."""
    if not isinstance(plain, str) or not plain:
        raise ValueError("password must be a non-empty string")
    return _HASHER.hash(plain)


def verify_password(stored_hash: str, plain: str) -> bool:
    """Constant-time verify. Returns False on any mismatch or malformed hash."""
    if not stored_hash or not isinstance(plain, str):
        return False
    try:
        return _HASHER.verify(stored_hash, plain)
    except (VerifyMismatchError, InvalidHashError):
        return False


def needs_rehash(stored_hash: str) -> bool:
    """True if the hash was produced with stale params and should be upgraded."""
    try:
        return _HASHER.check_needs_rehash(stored_hash)
    except InvalidHashError:
        return True
