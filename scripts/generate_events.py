"""Seeded synthetic event generator: user clickstream + purchase events.

Produces JSON lines on stdout (or writes to a file). Mostly time-ordered with
a small fraction of out-of-order / very-late events so the engine's watermark
logic has something to chew on.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time


def generate_events(
    seed: int = 7,
    num_events: int = 5000,
    num_entities: int = 200,
    hours: float = 6.0,
    start_ts: float | None = None,
) -> list[dict]:
    if start_ts is None:
        # Default to "now minus <hours>", so TTL/watermark behavior is realistic.
        start_ts = time.time() - hours * 3600.0
    rng = random.Random(seed)
    events: list[dict] = []
    for _ in range(num_events):
        entity_id = f"user_{rng.randint(1, num_entities):04d}"
        roll = rng.random()
        if roll < 0.55:
            event_type = "page_view"
        elif roll < 0.80:
            event_type = "click"
        elif roll < 0.93:
            event_type = "add_to_cart"
        else:
            event_type = "purchase"
        ts = start_ts + rng.random() * hours * 3600.0
        event = {
            "entity_id": entity_id,
            "event_type": event_type,
            "ts": round(ts, 3),
        }
        if event_type == "purchase":
            event["amount"] = round(rng.uniform(5.0, 500.0), 2)
            event["items"] = rng.randint(1, 5)
        events.append(event)

    events.sort(key=lambda e: e["ts"])

    # Simulate out-of-order arrival: move ~2% of events to later positions
    # (their timestamps stay old, so they arrive "late").
    nudge = max(1, num_events // 50)
    for _ in range(nudge):
        idx = rng.randrange(len(events))
        event = events.pop(idx)
        # A couple of these are *very* late (hours behind) and will be dropped.
        new_idx = min(len(events), idx + rng.choice([50, 100, 200, 5000]))
        events.insert(new_idx, event)
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic clickstream/purchase events.")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--num-events", type=int, default=5000)
    parser.add_argument("--num-entities", type=int, default=200)
    parser.add_argument("--hours", type=float, default=6.0)
    parser.add_argument("--output", default="-", help="Output file, or - for stdout")
    args = parser.parse_args()

    events = generate_events(
        seed=args.seed,
        num_events=args.num_events,
        num_entities=args.num_entities,
        hours=args.hours,
    )
    out = sys.stdout if args.output == "-" else open(args.output, "w")
    with out:
        for event in events:
            out.write(json.dumps(event) + "\n")
    print(f"Wrote {len(events)} events (seed={args.seed})", file=sys.stderr)


if __name__ == "__main__":
    main()
