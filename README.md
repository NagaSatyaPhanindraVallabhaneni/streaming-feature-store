# streaming-feature-store

A tiny, real streaming feature store: register feature definitions (windowed
aggregations like `count` / `sum` / `avg` over tumbling windows), ingest
timestamped events, and serve each entity's latest feature vector over HTTP.

This is a portfolio project. It is genuinely implemented and tested — 59
pytest tests, all passing — but it is intentionally single-node and in-memory
where a production system would use Kafka/Flink/a distributed store. See
[What's intentionally simplified](#whats-intentionally-simplified).

## How it works

```
                        ┌─────────────────────────────────┐
                        │        FeatureEngine (in-mem)     │
                        │                                 │
  POST /events ────────►│  per-entity buckets keyed by    │
  POST /backfill ──────►│  (entity, feature, window_start)│
                        │  watermark  = max_ts - lateness │
                        │  TTL eviction of stale buckets  │
                        └───────────────┬─────────────────┘
                                        │ write-through
                                        ▼
                        ┌─────────────────────────────────┐
                        │   OnlineStore (serving layer)   │
                        │   InMemory  |  SQLite (opt-in)  │
                        └───────────────┬─────────────────┘
                                        │
                        GET /features/{entity_id} ◄────────┘
```

- **Tumbling windows**: an event with timestamp `ts` belongs to window
  `floor(ts / window) * window`. Windows are per feature definition, so `1h`
  revenue and `5m` click counts coexist.
- **Watermark / late events**: `watermark = max event ts seen − allowed lateness`
  (default 300s). An event older than the watermark is dropped as too late and
  counted in `late_dropped`; an out-of-order event inside the lateness budget
  is still applied (`late_applied`).
- **TTL expiry**: buckets whose window ended before `now − TTL` are evicted by
  `evict_expired()` (default TTL = 2× the largest window; the API calls it on
  `/stats`).
- **Backfill**: `POST /backfill` replays a historical batch in timestamp order
  so it isn't penalized by the watermark.
- **Serving**: the API serves feature vectors from the online store, which is
  updated write-through on every ingest/backfill. Registering a new definition
  does not rewrite already-served vectors until new events arrive.

## Quickstart

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# CLI demo (no server): ingest 3000 synthetic events, print feature vectors
PYTHONPATH=src .venv/bin/python scripts/demo.py

# API server
PYTHONPATH=src .venv/bin/uvicorn streaming_feature_store.service:app --port 8000

# ...or with Docker (SQLite online store persisted to ./data)
docker compose up --build   # -> http://localhost:8000
```

Run the checks:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q   # 59 passed
.venv/bin/ruff check src scripts tests
.venv/bin/ruff format --check src scripts tests
```

## API examples

Register a feature definition:

```bash
curl -X POST localhost:8000/definitions -H 'Content-Type: application/json' -d '{
  "name": "purchase_sum_1h",
  "agg": "sum",
  "window": "1h",
  "field": "amount",
  "filters": {"event_type": "purchase"},
  "description": "Hourly purchase revenue per user"
}'
# -> {"name":"purchase_sum_1h","agg":"sum","window":"1h","window_seconds":3600,
#     "field":"amount","filters":{"event_type":"purchase"},"description":"Hourly purchase revenue per user"}
```

Ingest an event and serve features:

```bash
curl -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "entity_id": "u1", "event_type": "purchase", "ts": 1700000100, "amount": 49.99
}'
# -> {"status":"accepted","entity_id":"u1",
#     "features":{"purchase_sum_1h":{"value":49.99,"count":1.0,
#       "window_start":1699999200,"window_end":1700002800}}}

curl localhost:8000/features/u1
curl localhost:8000/stats
```

Backfill a historical batch (replayed in timestamp order):

```bash
curl -X POST localhost:8000/backfill -H 'Content-Type: application/json' -d '{
  "events": [
    {"entity_id":"u2","event_type":"purchase","ts":1700000200,"amount":10.0},
    {"entity_id":"u2","event_type":"purchase","ts":1700000150,"amount":20.0}
  ]
}'
# -> {"accepted":2,"late_applied":0,"late_dropped":0,"entities":["u2"]}
```

Supported windows: `30s`, `5m`, `1h`, `1d` (any positive integer + unit).
Supported aggregations: `count`, `sum`, `avg`. Definitions accept optional
equality `filters`, e.g. `{"event_type": "purchase"}`.

Environment knobs: `ALLOWED_LATENESS_SECONDS` (default 300),
`STATE_TTL_SECONDS` (default 2× largest window), `STORE_BACKEND=memory|sqlite`
(default `memory`), `SQLITE_PATH` (default `feature_store.db`).

## Sample demo output

Real output of `scripts/demo.py` (seed 7, 3000 synthetic clickstream events,
222 of them purchases, 6-hour span):

```
Ingested 3000 synthetic events (222 purchases) across a 6h clickstream.
  accepted=2940  late_applied=1  late_dropped=59
  watermark=1789894962  buckets=1484

Latest feature vectors for the 3 most active purchasers:

  user_0144  (4 purchases total)
    purchase_count_1h    value=1.00  (n=1, window 1789891200→1789894800)
    purchase_sum_1h      value=94.79  (n=1, window 1789891200→1789894800)
    purchase_avg_1h      value=94.79  (n=1, window 1789891200→1789894800)
    clicks_5m            value=1.00  (n=1, window 1789884900→1789885200)
    events_1d            value=11.00  (n=11, window 1789862400→1789948800)
  ...
TTL eviction at stream end: 0 stale buckets removed, 1484 remain.
```

The generator deliberately injects out-of-order and very-late events, which is
why 59 events were dropped by the watermark while 1 late-but-acceptable event
was still applied. Nothing here is a benchmark — it's one deterministic
synthetic run (`--seed 7`).

## Project layout

```
src/streaming_feature_store/
  definitions.py   # window parsing ("5m"->300s), FeatureDefinition validation
  engine.py        # tumbling-window aggregation, watermark, TTL, backfill
  store.py         # pluggable online stores: in-memory + SQLite
  service.py       # FastAPI: /events /features/{id} /definitions /backfill /health /stats
scripts/
  generate_events.py  # seeded synthetic clickstream/purchase generator
  demo.py             # CLI demo driving the engine (no server needed)
tests/                # 59 pytest tests: windowing math, late events, TTL, API, stores
```

## What's intentionally simplified

- **Single node, in-memory engine.** A production feature store would run the
  aggregation on Flink/Spark Streaming with checkpointed state. Here the
  interesting, testable part — windowing math, watermarks, TTL — is real;
  the distributed part is not.
- **Global watermark, not per-key.** Fine for a demo; production systems track
  watermarks per partition/key.
- **Serving reads the last written vector.** The SQLite backend persists served
  vectors across restarts, but the engine's window state itself is not
  checkpointed — after a restart, history before the restart is gone until
  new events (or a backfill) arrive.
- **No auth, no schema registry, no exactly-once.** Event `ts` is epoch
  seconds; out-of-order handling is best-effort within the lateness budget.
