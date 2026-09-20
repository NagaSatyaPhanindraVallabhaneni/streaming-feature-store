"""CLI demo: ingest a synthetic stream, print per-entity feature vectors.

No server needed — drives the FeatureEngine directly.
Run with:  PYTHONPATH=src python scripts/demo.py [--num-events N] [--seed S]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make both the package (src/) and this script's siblings importable
# regardless of the working directory the demo is launched from.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from generate_events import generate_events  # noqa: E402

from streaming_feature_store.definitions import FeatureDefinition  # noqa: E402
from streaming_feature_store.engine import FeatureEngine  # noqa: E402


def build_engine() -> FeatureEngine:
    return FeatureEngine(
        definitions=(
            FeatureDefinition(
                name="purchase_count_1h",
                agg="count",
                window="1h",
                filters={"event_type": "purchase"},
                description="Purchases per user, 1h tumbling window",
            ),
            FeatureDefinition(
                name="purchase_sum_1h",
                agg="sum",
                window="1h",
                field="amount",
                filters={"event_type": "purchase"},
                description="Purchase revenue per user, 1h window",
            ),
            FeatureDefinition(
                name="purchase_avg_1h",
                agg="avg",
                window="1h",
                field="amount",
                filters={"event_type": "purchase"},
                description="Average order value per user, 1h window",
            ),
            FeatureDefinition(
                name="clicks_5m",
                agg="count",
                window="5m",
                filters={"event_type": "click"},
                description="Clicks per user, 5m window",
            ),
            FeatureDefinition(
                name="events_1d",
                agg="count",
                window="1d",
                description="All events per user, 1d window",
            ),
        ),
        allowed_lateness_seconds=300.0,
        state_ttl_seconds=2 * 86400,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming feature store demo (no server).")
    parser.add_argument("--num-events", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    engine = build_engine()
    events = generate_events(seed=args.seed, num_events=args.num_events)

    statuses = {"accepted": 0, "late_applied": 0, "late_dropped": 0}
    purchases = 0
    for event in events:
        statuses[engine.ingest(event)] += 1
        if event["event_type"] == "purchase":
            purchases += 1

    print(
        f"Ingested {len(events)} synthetic events ({purchases} purchases) across a 6h clickstream."
    )
    print(
        f"  accepted={statuses['accepted']}  "
        f"late_applied={statuses['late_applied']}  "
        f"late_dropped={statuses['late_dropped']}"
    )
    print(f"  watermark={engine.stats()['watermark']:.0f}  buckets={engine.stats()['buckets']}")

    # Sample entities: the three users with the most purchases.
    per_user_purchases: dict[str, int] = {}
    for event in events:
        if event["event_type"] == "purchase":
            per_user_purchases[event["entity_id"]] = (
                per_user_purchases.get(event["entity_id"], 0) + 1
            )
    sample = sorted(per_user_purchases, key=per_user_purchases.get, reverse=True)[:3]

    print("\nLatest feature vectors for the 3 most active purchasers:")
    for entity_id in sample:
        features = engine.latest_features(entity_id)
        print(f"\n  {entity_id}  ({per_user_purchases[entity_id]} purchases total)")
        for name, feat in features.items():
            print(
                f"    {name:20s} value={feat['value']:.2f}  "
                f"(n={feat['count']:.0f}, window {feat['window_start']:.0f}"
                f"→{feat['window_end']:.0f})"
            )

    evicted = engine.evict_expired(now=max(e["ts"] for e in events))
    print(
        f"\nTTL eviction at stream end: {evicted} stale buckets removed, "
        f"{engine.stats()['buckets']} remain."
    )


if __name__ == "__main__":
    main()
