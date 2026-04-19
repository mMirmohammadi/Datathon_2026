"""User system: accounts, sessions, CSRF, rate-limit, interactions.

All state lives in ``data/users.db`` - the teammate-shipped ``data/listings.db``
is never touched by anything in this package.
"""
