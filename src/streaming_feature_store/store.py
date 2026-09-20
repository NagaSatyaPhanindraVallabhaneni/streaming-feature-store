"""Pluggable online stores for serving feature vectors.

The engine computes features; the online store is the serving layer the API
reads from. Two backends:

- InMemoryStore: default, fast, lost on restart.
- SQLiteStore: persists the latest feature vector per entity to a SQLite file,
  so served vectors survive a restart (the engine's window state does not —
  that is an intentional simplification, see README).
"""

from __future__ import annotations

import json
import sqlite3
import time
from abc import ABC, abstractmethod


class OnlineStore(ABC):
    name = "base"

    @abstractmethod
    def put(self, entity_id: str, features: dict) -> None:
        """Store the latest feature vector for an entity."""

    @abstractmethod
    def get(self, entity_id: str) -> dict | None:
        """Return the latest feature vector, or None if unknown."""

    @abstractmethod
    def delete(self, entity_id: str) -> None:
        """Remove an entity's feature vector."""

    def close(self) -> None:
        """Release resources. No-op for stores that hold none."""


class InMemoryStore(OnlineStore):
    name = "memory"

    def __init__(self) -> None:
        self._data: dict[str, dict] = {}

    def put(self, entity_id: str, features: dict) -> None:
        self._data[entity_id] = {"features": features, "updated_at": time.time()}

    def get(self, entity_id: str) -> dict | None:
        entry = self._data.get(entity_id)
        return entry["features"] if entry is not None else None

    def delete(self, entity_id: str) -> None:
        self._data.pop(entity_id, None)


class SQLiteStore(OnlineStore):
    name = "sqlite"

    def __init__(self, path: str) -> None:
        self.path = path
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS features (
                    entity_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def put(self, entity_id: str, features: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO features (entity_id, payload, updated_at) VALUES (?, ?, ?)",
                (entity_id, json.dumps(features), time.time()),
            )

    def get(self, entity_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM features WHERE entity_id = ?", (entity_id,)
            ).fetchone()
        return json.loads(row[0]) if row is not None else None

    def delete(self, entity_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM features WHERE entity_id = ?", (entity_id,))
