"""In-memory expiring token stores for upload/merge workflows."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from threading import RLock


class ExpiringTokenStore:
    """Thread-safe in-memory token store with TTL eviction."""

    def __init__(self, ttl: timedelta):
        self._ttl = ttl
        self._lock = RLock()
        self._items: dict[str, dict] = {}

    def _is_expired(self, created_at: datetime, now: datetime) -> bool:
        return now - created_at > self._ttl

    def purge(self) -> None:
        with self._lock:
            now = datetime.now(timezone.utc)
            expired = [token for token, payload in self._items.items() if self._is_expired(payload["created_at"], now)]
            for token in expired:
                self._items.pop(token, None)

    def put(self, payload: dict) -> str:
        with self._lock:
            now = datetime.now(timezone.utc)
            expired = [token for token, item in self._items.items() if self._is_expired(item["created_at"], now)]
            for token in expired:
                self._items.pop(token, None)
            token = str(uuid.uuid4())
            self._items[token] = {"created_at": now, "payload": payload}
            return token

    def get(self, token: str, *, pop: bool = False) -> dict:
        with self._lock:
            item = self._items.get(token)
            if not item:
                raise KeyError("Unknown token")
            now = datetime.now(timezone.utc)
            if self._is_expired(item["created_at"], now):
                self._items.pop(token, None)
                raise KeyError("Expired token")
            if pop:
                self._items.pop(token, None)
            return item["payload"]

    @property
    def ttl(self) -> timedelta:
        return self._ttl

    @property
    def items(self) -> dict[str, dict]:
        """Compatibility accessor for tests and diagnostics."""
        return self._items

    @property
    def lock(self) -> RLock:
        """Compatibility accessor for tests and diagnostics."""
        return self._lock
