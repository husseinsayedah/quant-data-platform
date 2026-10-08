# mdp: a market data platform for quantitative research

[![ci](https://github.com/husseinsayedah/quant-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/husseinsayedah/quant-data-platform/actions)
![python](https://img.shields.io/badge/python-3.11%2B-blue)
![license](https://img.shields.io/badge/license-MIT-green)

Real-time exchange data in, query-ready research datasets out. One repository
that covers the full path a trading firm's data team owns: **ingestion with loss
detection, a columnar lake, batch jobs, quality checks, a research API, and
monitoring**, with every stage benchmarked.

```
 Binance WS ──> BinanceClient ──> asyncio.Queue ──> ParquetWriter ──> data/raw/   (Parquet, hive-partitioned)
                 orjson parse                        columnar buffers       │
                 sequence tracking                   threaded flush         ▼
                 Prometheus metrics                                   DuckDB Catalog
                                                                       │        │
                                                   bars job ◄──────────┘        └──────► quality job
                                                   data/bars/                            data/quality/*.json
                                                        │
                                                FastAPI (JSON | Arrow IPC) ──> MDPClient ──> pandas ──> research/
```

## Why this exists

Quant research is only as good as the data under it. The hard parts are not
the strategies, they are the unglamorous guarantees: *did we lose anything,
how late is it, can we rebuild it, and can a researcher get it in two lines of
Python*. This project is a small but complete answer to those four questions.

## What it does

| Stage | What | Where |
|---|---|---|
| Ingest | Binance spot `trade` + `bookTicker` streams for N symbols over one websocket. Reconnect with capped exponential backoff and jitter. Per-symbol `trade_id` sequence tracking classifies every message as contiguous, **gap** (n ids missing) or **duplicate** (dropped). | `src/mdp/ingest/binance.py` |
| Store | Columnar buffers flushed on row count *or* idle time, serialised on a worker thread so the reader never blocks. Hive layout `symbol=/date=/hour=` in zstd Parquet, readable by DuckDB, Polars, Spark with no catalog service. | `src/mdp/ingest/writer.py` |
| Query | DuckDB over the lake with partition pruning. Typed empty results for unknown symbols, UTC everywhere. | `src/mdp/storage/catalog.py` |
| Batch | OHLCV + VWAP + buy volume bars at any interval, computed in SQL, written back partitioned and **idempotent** per `(symbol, date)`. | `src/mdp/batch/bars.py` |
| Quality | Gaps, duplicates, non-monotonic timestamps, price outliers, latency p50/p99, longest silence, clock-skew warning. JSON reports served by the API. | `src/mdp/batch/quality.py` |
| Serve | FastAPI: `/trades`, `/book_ticker`, `/bars`, `/quality/latest`, `/symbols`, `/metrics`. Every dataset as JSON **or Arrow IPC**. | `src/mdp/api/app.py` |
| Consume | `MDPClient(...).bars("BTCUSDT", "1m", start, end)` returns a DataFrame. Example SMA-crossover evaluation shows the researcher's view. | `src/mdp/api/client.py`, `research/backtest.py` |
| Observe | 14 Prometheus metrics (connection, throughput, gaps, queue depth, flush time, end-to-end latency histogram, API latency). Provisioned Grafana dashboard. | `src/mdp/monitor/metrics.py`, `deploy/` |

## Measured

From `bench/RESULTS.md`, reproducible with `python bench/run_bench.py`
(Windows 10 laptop, 8 logical CPUs, Python 3.13, pyarrow 24, DuckDB 1.5).

| What | Result |
|---|---|
| Parse Binance frames (orjson) | ~80k msgs/s per core, 2.1x the standard library |
| Parse + sequence check + metrics | ~85k msgs/s (metrics overhead is in the noise) |
| Writer, per-row append + Parquet flush, 1M trades | **425k rows/s** |
| Flush of a 100k-row buffer | **124 ms** (was 1,690 ms before optimisation, see below) |
| Storage | 20.4 bytes/trade in zstd Parquet vs 164 bytes/frame on the wire: **8x** |
| Build 1-minute bars from 1M trades | 0.21 s, ~5M trades/s |
| Quality checks over 1M trades | 0.26 s |
| 5-minute range query (60k rows) | p50 26 ms, p99 37 ms |
| Full scan of 1M trades into Arrow | 240 ms |
| Aggregate over 1M trades | 15 ms |
| `/bars` response, Arrow vs JSON | 8.2 KB vs 18.7 KB, 6 ms vs 10 ms |

Live, against Binance for 60 s on two symbols: 22,709 messages, 0 gaps, 0
duplicates, 0 reconnects. The quality job flagged a **negative median latency
(-705 ms)**: this laptop's clock was behind the exchange clock. That is the
clock-skew failure mode from `docs/design.md` showing up in practice, and the
reason the report carries a `warnings` field.

**One profiling story.** The first benchmark showed a 100k-row flush taking
1.7 s. Profiling each stage showed 1.49 s inside Arrow's `strftime` on
timezone-aware timestamps, used to derive the `date` partition key. A buffer
only ever spans a handful of hours, so the fix buckets timestamps with integer
division and formats each distinct hour once: 1,490 ms became 5 ms, and
end-to-end writer throughput went from 45k to 425k rows/s. The explanation is
kept as a comment at the call site.

## Quickstart

```bash
pip install -e ".[dev]"
pytest                                  # 34 tests, offline, ~30 s

# Offline demo: synthetic data -> bars -> quality -> API -> research
mdp synth --symbol BTCUSDT --n 1000000
mdp bars --interval 1m
mdp quality
mdp serve &                             # http://localhost:8000/docs
python research/backtest.py --fast 10 --slow 60

# Live
mdp ingest --symbols btcusdt,ethusdt    # Ctrl-C flushes and exits cleanly
```

Full stack with Prometheus and Grafana:

```bash
docker compose up -d --build            # Grafana on :3000 (admin/admin), API on :8000
```

Researcher's two lines:

```python
from mdp.api.client import MDPClient

bars = MDPClient("http://localhost:8000").bars("BTCUSDT", "1m")  # pandas DataFrame, UTC index
```

## Design in one minute

* **Parquet + DuckDB, no database server.** Market data is immutable and
  time-ordered; a columnar file format gives pruning and compression for free
  and is readable by every tool. The directory layout *is* the catalog.
* **Loss is recorded, never hidden.** Sequence state survives reconnects so a
  reconnect that lost messages shows up as a gap, online (Prometheus) and
  offline (quality job), and the two can be reconciled.
* **Two timestamps per event.** Exchange time and local receive time; their
  difference is the latency distribution and the clock-skew detector.
* **The reader never blocks.** Buffer, swap atomically, serialise on a thread.
  On shutdown the queue is drained and flushed; nothing that reached the
  process is lost.
* **Batch jobs are idempotent.** A re-run after late data or a bug fix
  converges to the same state.
* **Arrow over the wire.** Exact types, no parsing, fewer bytes.

Trade-offs, rejected alternatives and failure modes: [`docs/design.md`](docs/design.md).
Operations: [`docs/runbook.md`](docs/runbook.md). What comes next: [`docs/roadmap.md`](docs/roadmap.md).

## Layout

```
src/mdp/
  ingest/    binance.py (ws client, parser, SequenceTracker)  writer.py (Parquet)  replay.py (synthetic, capture)
  storage/   catalog.py (DuckDB over the lake)
  batch/     bars.py (OHLCV job)  quality.py (checks + reports)
  api/       app.py (FastAPI)  client.py (researcher client)
  monitor/   metrics.py (Prometheus)
  cli.py     mdp ingest | synth | bars | quality | serve | capture
tests/       34 tests, all offline: parser, sequence, writer, catalog, bars vs pandas reference, quality, API
bench/       run_bench.py -> RESULTS.md
research/    backtest.py (example consumer)
deploy/      prometheus.yml, Grafana provisioning + dashboard
docs/        design.md, runbook.md, roadmap.md
```

## Status

M1 complete (see roadmap). Next: run it for two weeks on a VM, reconcile bars
against the exchange's historical klines, level-2 order book, and a Rust parse
path.
