"""Tests for the FastAPI service."""

import pytest
from fastapi.testclient import TestClient

from streaming_feature_store import service


@pytest.fixture(autouse=True)
def fresh_state():
    service.reset_state()
    yield
    service.reset_state()


@pytest.fixture()
def client():
    with TestClient(service.app) as c:
        yield c


def register(client, **overrides):
    payload = {
        "name": "purchase_sum_1h",
        "agg": "sum",
        "window": "1h",
        "field": "amount",
        "filters": {"event_type": "purchase"},
    }
    payload.update(overrides)
    return client.post("/definitions", json=payload)


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_register_definition(client):
    response = register(client)
    assert response.status_code == 201
    body = response.json()
    assert body["window_seconds"] == 3600
    assert body["filters"] == {"event_type": "purchase"}

    listed = client.get("/definitions").json()["definitions"]
    assert [d["name"] for d in listed] == ["purchase_sum_1h"]


def test_register_definition_invalid_window(client):
    assert register(client, window="10x").status_code == 422


def test_register_definition_sum_without_field(client):
    assert register(client, field=None).status_code == 422


def test_register_definition_duplicate_name_overwrites(client):
    assert register(client).status_code == 201
    assert register(client, window="1d").status_code == 201
    listed = client.get("/definitions").json()["definitions"]
    assert len(listed) == 1
    assert listed[0]["window"] == "1d"


def test_ingest_then_serve_features(client):
    register(client)
    response = client.post(
        "/events",
        json={"entity_id": "u1", "event_type": "purchase", "ts": 100.0, "amount": 42.5},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert body["features"]["purchase_sum_1h"]["value"] == pytest.approx(42.5)

    served = client.get("/features/u1").json()
    assert served["entity_id"] == "u1"
    assert served["features"]["purchase_sum_1h"]["value"] == pytest.approx(42.5)


def test_features_unknown_entity_404(client):
    assert client.get("/features/ghost").status_code == 404


def test_late_events_reported_by_status(client):
    register(client, name="cnt_1h", agg="count", window="1h", field=None, filters={})
    assert (
        client.post(
            "/events", json={"entity_id": "u1", "event_type": "click", "ts": 2000.0}
        ).json()["status"]
        == "accepted"
    )
    # watermark is now 1700 (lateness 300): ts=1900 is late but applied...
    assert (
        client.post(
            "/events", json={"entity_id": "u1", "event_type": "click", "ts": 1900.0}
        ).json()["status"]
        == "late_applied"
    )
    # ...and ts=1000 is too late and dropped.
    assert (
        client.post(
            "/events", json={"entity_id": "u1", "event_type": "click", "ts": 1000.0}
        ).json()["status"]
        == "late_dropped"
    )
    served = client.get("/features/u1").json()["features"]
    assert served["cnt_1h"]["value"] == 2  # dropped event did not count


def test_backfill_recomputes_history(client):
    register(client)
    response = client.post(
        "/backfill",
        json={
            "events": [
                {"entity_id": "u1", "event_type": "purchase", "ts": 300.0, "amount": 30.0},
                {"entity_id": "u1", "event_type": "purchase", "ts": 100.0, "amount": 10.0},
                {"entity_id": "u2", "event_type": "purchase", "ts": 200.0, "amount": 20.0},
            ]
        },
    )
    assert response.status_code == 200
    summary = response.json()
    assert summary["accepted"] == 3
    assert summary["entities"] == ["u1", "u2"]
    assert client.get("/features/u1").json()["features"]["purchase_sum_1h"][
        "value"
    ] == pytest.approx(40.0)


def test_new_definition_needs_new_events_before_serving(client):
    register(client, name="cnt_1h", agg="count", window="1h", field=None, filters={})
    client.post("/events", json={"entity_id": "u1", "event_type": "click", "ts": 100.0})
    register(client, name="cnt_1d", agg="count", window="1d", field=None, filters={})
    # The served vector only refreshes on ingest: cnt_1d is not there yet.
    assert set(client.get("/features/u1").json()["features"]) == {"cnt_1h"}
    client.post("/events", json={"entity_id": "u1", "event_type": "click", "ts": 200.0})
    assert set(client.get("/features/u1").json()["features"]) == {"cnt_1h", "cnt_1d"}


def test_stats(client):
    register(client, name="cnt_1h", agg="count", window="1h", field=None, filters={})
    client.post("/events", json={"entity_id": "u1", "event_type": "click", "ts": 100.0})
    stats = client.get("/stats").json()
    assert stats["events_ingested"] == 1
    assert stats["definitions"] == 1
    assert stats["store_backend"] == "memory"
