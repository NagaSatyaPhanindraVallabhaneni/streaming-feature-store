"""Tests for the streaming aggregation engine: windowing math, watermarks, TTL."""

import pytest

from streaming_feature_store.definitions import FeatureDefinition
from streaming_feature_store.engine import FeatureEngine


def make_engine(**kwargs):
    return FeatureEngine(
        definitions=(
            FeatureDefinition(name="cnt_1h", agg="count", window="1h"),
            FeatureDefinition(
                name="sum_1h",
                agg="sum",
                window="1h",
                field="amount",
                filters={"event_type": "purchase"},
            ),
            FeatureDefinition(
                name="avg_1h",
                agg="avg",
                window="1h",
                field="amount",
                filters={"event_type": "purchase"},
            ),
        ),
        **kwargs,
    )


def event(entity, ts, event_type="click", amount=None):
    e = {"entity_id": entity, "event_type": event_type, "ts": ts}
    if amount is not None:
        e["amount"] = amount
    return e


def test_count_exact_values():
    engine = make_engine()
    engine.ingest(event("u1", 100))
    engine.ingest(event("u1", 200))
    feats = engine.latest_features("u1")
    assert feats["cnt_1h"]["value"] == 2
    assert feats["cnt_1h"]["window_start"] == 0
    assert feats["cnt_1h"]["window_end"] == 3600

    engine.ingest(event("u1", 3700))
    feats = engine.latest_features("u1")
    assert feats["cnt_1h"]["value"] == 1
    assert feats["cnt_1h"]["window_start"] == 3600


def test_sum_exact_values():
    engine = make_engine()
    engine.ingest(event("u1", 100, "purchase", amount=10.0))
    engine.ingest(event("u1", 200, "purchase", amount=20.0))
    assert engine.latest_features("u1")["sum_1h"]["value"] == pytest.approx(30.0)


def test_avg_exact_values():
    engine = make_engine()
    engine.ingest(event("u1", 100, "purchase", amount=10.0))
    engine.ingest(event("u1", 200, "purchase", amount=20.0))
    assert engine.latest_features("u1")["avg_1h"]["value"] == pytest.approx(15.0)


def test_window_boundary_event_starts_new_window():
    engine = make_engine()
    engine.ingest(event("u1", 3599))
    engine.ingest(event("u1", 3600))  # exactly on the boundary -> new window
    feats = engine.latest_features("u1")
    assert feats["cnt_1h"]["window_start"] == 3600
    assert feats["cnt_1h"]["value"] == 1


def test_filters_only_matching_events_counted():
    engine = make_engine()
    engine.ingest(event("u1", 100, "click"))
    engine.ingest(event("u1", 200, "page_view"))
    engine.ingest(event("u1", 300, "purchase", amount=5.0))
    feats = engine.latest_features("u1")
    assert feats["cnt_1h"]["value"] == 3  # no filter on cnt_1h
    assert feats["sum_1h"]["value"] == pytest.approx(5.0)
    assert feats["sum_1h"]["count"] == 1


def test_missing_field_event_skipped_for_sum_but_counted():
    engine = make_engine()
    engine.ingest(event("u1", 100, "purchase"))  # no amount
    feats = engine.latest_features("u1")
    assert "sum_1h" not in feats  # no bucket created
    assert "avg_1h" not in feats
    assert feats["cnt_1h"]["value"] == 1


def test_entities_are_isolated():
    engine = make_engine()
    engine.ingest(event("u1", 100, "purchase", amount=10.0))
    engine.ingest(event("u2", 100, "purchase", amount=99.0))
    assert engine.latest_features("u1")["sum_1h"]["value"] == pytest.approx(10.0)
    assert engine.latest_features("u2")["sum_1h"]["value"] == pytest.approx(99.0)


def test_late_event_within_lateness_is_applied():
    engine = make_engine(allowed_lateness_seconds=300)
    assert engine.ingest(event("u1", 1000)) == "accepted"
    assert engine.ingest(event("u1", 2000)) == "accepted"  # watermark now 1700
    assert engine.ingest(event("u1", 1900)) == "late_applied"
    assert engine.latest_features("u1")["cnt_1h"]["value"] == 3
    assert engine.late_dropped == 0


def test_late_event_beyond_lateness_is_dropped():
    engine = make_engine(allowed_lateness_seconds=300)
    engine.ingest(event("u1", 1000))
    engine.ingest(event("u1", 2000))  # watermark now 1700
    assert engine.ingest(event("u1", 1500)) == "late_dropped"
    assert engine.late_dropped == 1
    assert engine.events_ingested == 2
    # The dropped event did not touch the buckets.
    assert engine.latest_features("u1")["cnt_1h"]["value"] == 2


def test_event_exactly_at_watermark_is_accepted():
    engine = make_engine(allowed_lateness_seconds=300)
    engine.ingest(event("u1", 2000))  # watermark = 1700
    assert engine.ingest(event("u1", 1700)) == "late_applied"  # not < watermark
    assert engine.late_dropped == 0


def test_ttl_eviction_removes_only_stale_buckets():
    engine = make_engine(state_ttl_seconds=100)
    engine.ingest(event("u1", 100, "purchase", amount=10.0))  # window [0, 3600)
    engine.ingest(event("u1", 4000, "purchase", amount=20.0))  # window [3600, 7200)
    evicted = engine.evict_expired(now=5000.0)
    # cutoff = 4900: window [0,3600) ends at 3600 <= 4900 -> evicted (3 buckets);
    # window [3600,7200) ends at 7200 -> kept.
    assert evicted == 3
    assert engine.stats()["buckets"] == 3


def test_ttl_default_is_twice_largest_window():
    engine = make_engine()  # no explicit TTL; largest window is 1h
    engine.ingest(event("u1", 100))  # one bucket: cnt_1h, window [0, 3600)
    # default TTL = 7200; at now=10000, cutoff=2800 < window end 3600 -> kept
    assert engine.evict_expired(now=10000.0) == 0
    # at now=20000, cutoff=12800 >= 3600 -> evicted
    assert engine.evict_expired(now=20000.0) == 1


def test_backfill_recomputes_from_unsorted_history():
    engine = make_engine()
    summary = engine.backfill(
        [
            event("u1", 300, "purchase", amount=30.0),
            event("u1", 100, "purchase", amount=10.0),
            event("u2", 200, "purchase", amount=20.0),
            event("u1", 200, "purchase", amount=20.0),
        ]
    )
    assert summary["accepted"] == 4
    assert summary["late_dropped"] == 0
    assert summary["entities"] == ["u1", "u2"]
    assert engine.latest_features("u1")["sum_1h"]["value"] == pytest.approx(60.0)
    assert engine.latest_features("u1")["avg_1h"]["value"] == pytest.approx(20.0)
    assert engine.latest_features("u2")["sum_1h"]["value"] == pytest.approx(20.0)


def test_latest_features_unknown_entity_is_empty():
    assert make_engine().latest_features("ghost") == {}


def test_latest_features_picks_newest_window():
    engine = make_engine()
    engine.ingest(event("u1", 100, "purchase", amount=10.0))
    engine.ingest(event("u1", 3700, "purchase", amount=99.0))
    feats = engine.latest_features("u1")
    assert feats["sum_1h"]["window_start"] == 3600
    assert feats["sum_1h"]["value"] == pytest.approx(99.0)


def test_stats_reports_counters():
    engine = make_engine(allowed_lateness_seconds=300)
    engine.ingest(event("u1", 1000))
    engine.ingest(event("u2", 2000))
    engine.ingest(event("u1", 1500 - 1000))  # ts=500 < watermark 1700 -> dropped
    stats = engine.stats()
    assert stats["events_ingested"] == 2
    assert stats["late_dropped"] == 1
    assert stats["entities"] == 2
    assert stats["definitions"] == 3
    assert stats["watermark"] == pytest.approx(1700.0)


def test_replacing_definition_drops_old_buckets():
    engine = make_engine()
    engine.ingest(event("u1", 100))
    assert engine.latest_features("u1")["cnt_1h"]["window_end"] == 3600
    engine.add_definition(FeatureDefinition(name="cnt_1h", agg="count", window="1d"))
    engine.ingest(event("u1", 200))
    feats = engine.latest_features("u1")
    assert feats["cnt_1h"]["window_end"] == 86400
    assert feats["cnt_1h"]["value"] == 1  # old 1h buckets are gone


def test_remove_definition():
    engine = make_engine()
    assert engine.remove_definition("cnt_1h") is True
    assert engine.remove_definition("cnt_1h") is False
    engine.ingest(event("u1", 100))
    assert "cnt_1h" not in engine.latest_features("u1")
