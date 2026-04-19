"""In-memory sliding-window rate limiter for the login route.

Two independent counters:
* per-username: 10 failed attempts in 5 min -> next login returns 429
* per-IP: 20 failed attempts in 5 min -> next login returns 429

Only failed attempts count. A successful login clears the username counter
so a user who finally remembers their password isn't locked out by their own
earlier typos. Single-process only - this is a datathon demo, not a
multi-worker deployment. If we ever scale out we move this to SQLite /
Redis, but in-memory is the correct first step.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass


WINDOW_SECONDS = 5 * 60
MAX_FAILURES_PER_USERNAME = 10
MAX_FAILURES_PER_IP = 20


@dataclass
class _Buckets:
    by_username: dict[str, deque[float]]
    by_ip: dict[str, deque[float]]


_STATE = _Buckets(
    by_username=defaultdict(deque),
    by_ip=defaultdict(deque),
)
_LOCK = threading.Lock()


def _prune(bucket: deque[float], now: float) -> None:
    cutoff = now - WINDOW_SECONDS
    while bucket and bucket[0] < cutoff:
        bucket.popleft()


def check_allowed(username: str, ip: str) -> bool:
    """Return True if we should process the login attempt, False to send 429."""
    now = time.monotonic()
    with _LOCK:
        u = _STATE.by_username.get(username)
        if u is not None:
            _prune(u, now)
            if len(u) >= MAX_FAILURES_PER_USERNAME:
                return False
        p = _STATE.by_ip.get(ip)
        if p is not None:
            _prune(p, now)
            if len(p) >= MAX_FAILURES_PER_IP:
                return False
    return True


def record_failure(username: str, ip: str) -> None:
    now = time.monotonic()
    with _LOCK:
        _STATE.by_username[username].append(now)
        _STATE.by_ip[ip].append(now)


def record_success(username: str) -> None:
    """Clear the per-username counter; leave the per-IP counter alone (abuse-resilient)."""
    with _LOCK:
        _STATE.by_username.pop(username, None)


def reset_for_tests() -> None:
    """Test hook: drop every bucket. Not called in production code paths."""
    with _LOCK:
        _STATE.by_username.clear()
        _STATE.by_ip.clear()
