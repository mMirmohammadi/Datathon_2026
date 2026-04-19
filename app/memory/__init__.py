"""Memory-based personalization.

Turns a user's interaction history into extra rankings that plug into the
existing n-way RRF fusion in :mod:`app.harness.search_service`. No existing
channel is replaced - memory is additive and opt-in.
"""
