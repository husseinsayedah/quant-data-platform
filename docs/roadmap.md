# Roadmap

Milestones are ordered by what they teach, not by difficulty.

## M1 - Ingest, store, query (done)
- Binance trades + top-of-book over websocket, reconnect with backoff, sequence tracking
- Hive-partitioned Parquet lake, DuckDB catalog, OHLCV bars, quality reports
- FastAPI with JSON and Arrow IPC, Python client, Prometheus metrics, Grafana dashboard
- Tests, CI, Docker Compose, reproducible benchmarks

## M2 - Run it for real
- Run the compose stack on a small VM for two weeks, keep the Grafana screenshots
- Record a real capture (`mdp capture`) and check it into `tests/fixtures/` as a parser fixture
- Compare measured latency against Binance's `E` vs `T` timestamps to separate exchange-side and network delay
- Add a nightly job that compares our bars with Binance's historical klines endpoint (reconciliation)

## M3 - Level-2 order book
- Subscribe to `@depth` deltas, synchronise with the REST snapshot (the classic
  "lastUpdateId / U / u" dance), detect sequence breaks, resnapshot
- Store book deltas; rebuild the book at any timestamp; compute imbalance features at bar close

## M4 - Hot path in Rust
- A PyO3 extension that parses frames and runs the sequence check without the GIL
- Benchmark against orjson path in `bench/RESULTS.md`; target 5-10x
- Keep the Python path as the reference implementation (property test: both agree on every frame)

## M5 - Storage at scale
- int64 tick prices with per-symbol tick size (exactness)
- Compaction job: merge hourly files into daily files after the day closes
- Evaluate ClickHouse as a serving layer for multi-symbol scans; keep Parquet as the source of truth

## M6 - Platform
- Backfill from Binance's public historical data dumps to seed years of history
- Schema evolution test: add a column, prove old files still read
- Auth on the API, per-user rate limits, request logging to the lake itself
