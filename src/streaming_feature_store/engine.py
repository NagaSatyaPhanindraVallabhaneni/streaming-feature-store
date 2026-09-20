"""Streaming aggregation engine.

Keeps per-entity state for tumbling time windows, in memory:

- Each feature definition gets one bucket per (entity, window_start).
- Buckets accumulate (count, sum); avg is derived as sum / count.
- A global watermark (max event ts seen minus allowed lateness) decides
  whether an out-of-order event is still applied or dropped as too late.
- Buckets older than the state TTL can be evicted with evict_expired().

This is intentionally single-node and in-memory: the interesting part for a
portfolio is the windowing / watermark / TTL math, which is fully tested.
"""

from __future__ import annotations

import math
import time

from .definitions import FeatureDefinition


def _matches(event: dict, filters: dict) -> bool:
    return all(event.get(key) == value for key, value in filters.items())


class FeatureEngine:
    def __init__(
        self,
        definitions: tuple[FeatureDefinition, ...] = (),
        allowed_lateness_seconds: float = 300.0,
        state_ttl_seconds: float | None = None,
    ) -> None:
        self.definitions: dict[str, FeatureDefinition] = {}
        for definition in definitions:
            self.definitions[definition.name] = definition
        self.allowed_lateness_seconds = float(allowed_lateness_seconds)
        self.state_ttl_seconds = float(state_ttl_seconds) if state_ttl_seconds is not None else None
        # (entity_id, definition_name, window_start) -> [count, sum]
        self._buckets: dict[tuple[str, str, int], list[float]] = {}
        self.max_ts_seen: float | None = None
        self.events_ingested = 0
        self.late_dropped = 0

    # ------------------------------------------------------------------ defs
    def add_definition(self, definition: FeatureDefinition) -> None:
        """Register (or replace) a feature definition.

        Replacing a definition drops its existing buckets, because the old
        buckets were computed under different window/agg settings.
        """
        if definition.name in self.definitions:
            self._buckets = {
                key: bucket for key, bucket in self._buckets.items() if key[1] != definition.name
            }
        self.definitions[definition.name] = definition

    def remove_definition(self, name: str) -> bool:
        if name not in self.definitions:
            return False
        del self.definitions[name]
        self._buckets = {key: b for key, b in self._buckets.items() if key[1] != name}
        return True

    # ---------------------------------------------------------------- ingest
    def ingest(self, event: dict) -> str:
        """Ingest one event dict. Returns 'accepted', 'late_applied' or 'late_dropped'."""
        entity_id = event["entity_id"]
        ts = float(event["ts"])

        if self.max_ts_seen is None or ts > self.max_ts_seen:
            self.max_ts_seen = ts
        watermark = self.max_ts_seen - self.allowed_lateness_seconds
        if ts < watermark:
            self.late_dropped += 1
            return "late_dropped"
        status = "late_applied" if ts < self.max_ts_seen else "accepted"

        for definition in self.definitions.values():
            if not _matches(event, definition.filters):
                continue
            if definition.agg == "count":
                value = 1.0
            else:
                raw = event.get(definition.field)
                if raw is None:
                    # Null/missing field: the event contributes nothing to this feature.
                    continue
                value = float(raw)
            window_start = math.floor(ts / definition.window_seconds) * definition.window_seconds
            key = (entity_id, definition.name, window_start)
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = [0.0, 0.0]
                self._buckets[key] = bucket
            bucket[0] += 1.0
            bucket[1] += value

        self.events_ingested += 1
        return status

    def backfill(self, events: list[dict]) -> dict:
        """Recompute features from a batch of historical events.

        Events are sorted by timestamp first, so a historical batch replays
        in order and is not penalized by the watermark.
        """
        ordered = sorted(events, key=lambda e: float(e["ts"]))
        summary = {"accepted": 0, "late_applied": 0, "late_dropped": 0, "entities": []}
        entities: set[str] = set()
        for event in ordered:
            summary[self.ingest(event)] += 1
            entities.add(event["entity_id"])
        summary["entities"] = sorted(entities)
        return summary

    # ----------------------------------------------------------------- serve
    def latest_features(self, entity_id: str) -> dict:
        """Latest computed feature vector for an entity.

        For each definition this returns the value from the most recent window
        that has data, plus the window bounds and the raw count.
        """
        result: dict = {}
        for definition in self.definitions.values():
            best_start: int | None = None
            for entity, name, window_start in self._buckets:
                if entity == entity_id and name == definition.name:
                    if best_start is None or window_start > best_start:
                        best_start = window_start
            if best_start is None:
                continue
            count, total = self._buckets[(entity_id, definition.name, best_start)]
            if definition.agg == "count":
                value = count
            elif definition.agg == "sum":
                value = total
            else:  # avg
                value = total / count
            result[definition.name] = {
                "value": value,
                "count": count,
                "window_start": best_start,
                "window_end": best_start + definition.window_seconds,
            }
        return result

    # ------------------------------------------------------------------ misc
    def evict_expired(self, now: float | None = None) -> int:
        """Drop buckets whose window ended before (now - TTL). Returns buckets removed."""
        now = time.time() if now is None else float(now)
        ttl = self.state_ttl_seconds
        if ttl is None:
            ttl = max((d.window_seconds for d in self.definitions.values()), default=3600) * 2
        cutoff = now - ttl
        expired = [
            key
            for key in self._buckets
            if key[2] + self.definitions[key[1]].window_seconds <= cutoff
        ]
        for key in expired:
            del self._buckets[key]
        return len(expired)

    def stats(self) -> dict:
        watermark = (
            self.max_ts_seen - self.allowed_lateness_seconds
            if self.max_ts_seen is not None
            else None
        )
        return {
            "events_ingested": self.events_ingested,
            "late_dropped": self.late_dropped,
            "entities": len({entity for (entity, _, _) in self._buckets}),
            "definitions": len(self.definitions),
            "buckets": len(self._buckets),
            "max_ts_seen": self.max_ts_seen,
            "watermark": watermark,
            "allowed_lateness_seconds": self.allowed_lateness_seconds,
        }

    def reset(self) -> None:
        self._buckets.clear()
        self.max_ts_seen = None
        self.events_ingested = 0
        self.late_dropped = 0
