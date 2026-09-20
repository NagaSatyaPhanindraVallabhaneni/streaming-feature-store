"""FastAPI service: feature definitions, event ingestion, online feature serving.

Architecture (write-through):
    POST /events  -> engine.ingest() -> store.put(entity, latest_features())
    GET /features/{entity_id} -> served from the online store.

The store is pluggable via env vars:
    STORE_BACKEND=memory (default) | sqlite
    SQLITE_PATH=feature_store.db (default, used when backend=sqlite)
    ALLOWED_LATENESS_SECONDS=300 (default)
    STATE_TTL_SECONDS= (default: 2x the largest window)
"""

from __future__ import annotations

import os
import time
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .definitions import definition_from_dict
from .engine import FeatureEngine
from .store import InMemoryStore, OnlineStore, SQLiteStore

START_TIME = time.time()


def _build_engine() -> FeatureEngine:
    ttl = os.getenv("STATE_TTL_SECONDS")
    return FeatureEngine(
        allowed_lateness_seconds=float(os.getenv("ALLOWED_LATENESS_SECONDS", "300")),
        state_ttl_seconds=float(ttl) if ttl else None,
    )


def _build_store() -> OnlineStore:
    backend = os.getenv("STORE_BACKEND", "memory").lower()
    if backend == "sqlite":
        return SQLiteStore(os.getenv("SQLITE_PATH", "feature_store.db"))
    if backend == "memory":
        return InMemoryStore()
    raise RuntimeError(f"Unknown STORE_BACKEND {backend!r}: expected 'memory' or 'sqlite'")


engine = _build_engine()
store = _build_store()

app = FastAPI(title="streaming-feature-store", version="0.1.0")


def reset_state() -> None:
    """Rebuild the engine and store. Used by tests; not part of the public API."""
    global engine, store
    engine = _build_engine()
    store = _build_store()


# ------------------------------------------------------------------ models
class EventIn(BaseModel):
    model_config = {"extra": "allow"}  # arbitrary event fields (e.g. amount, items)

    entity_id: str
    event_type: str
    ts: float  # epoch seconds


class DefinitionIn(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    agg: Literal["count", "sum", "avg"]
    window: str  # e.g. "30s", "5m", "1h", "1d"
    field: str | None = None
    filters: dict = Field(default_factory=dict)
    description: str = ""


class BackfillIn(BaseModel):
    events: list[EventIn]


# ----------------------------------------------------------------- helpers
def _refresh_served(entity_id: str) -> dict:
    """Write the entity's latest feature vector through to the online store."""
    features = engine.latest_features(entity_id)
    if features:
        store.put(entity_id, features)
    return features


# ------------------------------------------------------------------ routes
@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "streaming-feature-store",
        "uptime_seconds": round(time.time() - START_TIME, 3),
    }


@app.post("/definitions", status_code=201)
def register_definition(defn: DefinitionIn) -> dict:
    try:
        definition = definition_from_dict(defn.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    engine.add_definition(definition)
    return definition.to_dict()


@app.get("/definitions")
def list_definitions() -> dict:
    return {"definitions": [d.to_dict() for d in engine.definitions.values()]}


@app.post("/events")
def ingest_event(event: EventIn) -> dict:
    payload = event.model_dump()
    status = engine.ingest(payload)
    features = _refresh_served(payload["entity_id"])
    return {"status": status, "entity_id": payload["entity_id"], "features": features}


@app.post("/backfill")
def backfill(body: BackfillIn) -> dict:
    summary = engine.backfill([e.model_dump() for e in body.events])
    for entity_id in summary["entities"]:
        _refresh_served(entity_id)
    return summary


@app.get("/features/{entity_id}")
def get_features(entity_id: str) -> dict:
    features = store.get(entity_id)
    if features is None:
        raise HTTPException(status_code=404, detail=f"No features for entity {entity_id!r}")
    return {"entity_id": entity_id, "features": features}


@app.get("/stats")
def stats() -> dict:
    engine.evict_expired()
    data = engine.stats()
    data["store_backend"] = store.name
    data["uptime_seconds"] = round(time.time() - START_TIME, 3)
    return data
