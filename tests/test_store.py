"""Tests for the pluggable online stores."""

from streaming_feature_store.store import InMemoryStore, SQLiteStore


def test_memory_put_get_roundtrip():
    store = InMemoryStore()
    features = {"purchase_sum_1h": {"value": 42.5}}
    store.put("u1", features)
    assert store.get("u1") == features


def test_memory_get_missing_returns_none():
    assert InMemoryStore().get("ghost") is None


def test_memory_delete():
    store = InMemoryStore()
    store.put("u1", {"x": 1})
    store.delete("u1")
    assert store.get("u1") is None
    store.delete("u1")  # deleting twice is fine


def test_memory_overwrite():
    store = InMemoryStore()
    store.put("u1", {"x": 1})
    store.put("u1", {"x": 2})
    assert store.get("u1") == {"x": 2}


def test_sqlite_put_get_roundtrip(tmp_path):
    store = SQLiteStore(str(tmp_path / "features.db"))
    features = {"purchase_sum_1h": {"value": 42.5, "count": 3}}
    store.put("u1", features)
    assert store.get("u1") == features
    assert store.get("ghost") is None


def test_sqlite_persists_across_instances(tmp_path):
    path = str(tmp_path / "features.db")
    SQLiteStore(path).put("u1", {"x": 7})
    reopened = SQLiteStore(path)
    assert reopened.get("u1") == {"x": 7}


def test_sqlite_delete(tmp_path):
    store = SQLiteStore(str(tmp_path / "features.db"))
    store.put("u1", {"x": 1})
    store.delete("u1")
    assert store.get("u1") is None
