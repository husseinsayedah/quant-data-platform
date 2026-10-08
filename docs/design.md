# Design

This document explains the decisions behind `mdp`, the alternatives that were
considered, and the ones that were rejected. It is deliberately short; the
code is the reference.

## Goal

A single researcher-facing platform that turns a live exchange feed into
query-ready datasets, with the properties a trading firm cares about:
**nothing silently lost, everything measurable, re-runnable from scratch.**

```
 Binance WS ──> BinanceClient ──> asyncio.Queue ──> ParquetWriter ──> data/raw/  (Parquet, hive)
                 parse (orjson)                      columnar buffers        │
                 SequenceTracker                     threaded flush          ▼
                 Prometheus metrics                                  DuckDB Catalog
                                                                      │        │
                                                        bars job ◄────┘        └────► quality job
                                                        data/bars/                    data/quality/*.json
                                                             │
                                                     FastAPI  (JSON | Arrow IPC)
                                                             │
                                                     MDPClient -> pandas  -> research/backtest.py
```

## Decisions

### 1. Parquet files on disk, no database server

**Chosen:** append-only Parquet, hive-partitioned by `symbol/date/hour`,
queried with DuckDB in-process.

**Why:** market data is immutable and time-ordered. A columnar file format
with row-group statistics gives partition pruning and predicate pushdown for
free, compresses well (see `bench/RESULTS.md`), and is readable by every tool
a researcher might use (DuckDB, Polars, Spark, pandas). There is no service to
operate, no schema migration, and a corrupted file affects one hour of one
symbol.

**Rejected:** PostgreSQL/Timescale (row store, write amplification, a server
to run for what is a bulk-append workload); ClickHouse (excellent fit, but
operationally heavier than the problem needs today, see roadmap); a
key-value store (no analytical queries).

### 2. The directory layout is the catalog

There is no metadata service. `Catalog.symbols()` lists directories. DuckDB
infers partition columns from the path. This is one less thing to keep
consistent, at the cost of slower listing when the lake has millions of files;
that is the point at which a manifest (Iceberg/Delta) earns its place.

### 3. Loss detection via exchange sequence numbers

Binance assigns a per-symbol monotonically increasing `trade_id`. The client
tracks the last id per symbol and classifies every message as contiguous, a
gap (n ids missing) or a duplicate/out-of-order. Gaps are **recorded, not
hidden**: the counter is exported to Prometheus and the batch quality job
recomputes the same statistic from the stored data, so online and offline
views can be reconciled. The tracker state survives reconnects on purpose, so
a reconnect that lost messages shows up as a gap.

### 4. Timestamps

Two timestamps per event: `ts_event` (exchange) and `ts_recv` (local). The
difference is the end-to-end latency distribution, exported as a histogram.
Both are stored as `timestamp[us, UTC]`. Bar boundaries are computed in
integer microseconds, so no code path depends on a session time zone.

### 5. Writer: buffer, swap, flush on a thread

The reader must never block on disk. Events are buffered in Python lists per
column (cheaper than per-row objects), the buffer is swapped out atomically
and serialised by `asyncio.to_thread`. Flush triggers on row count or idle
time, whichever comes first, so quiet markets still produce files promptly and
busy markets produce large row groups. On shutdown the queue is drained and
flushed synchronously: no event that reached the process is lost.

### 6. Float64 prices

Research consumers want floats. Decimal exactness matters for accounting, not
for signals. Storing prices as int64 ticks with a per-symbol tick size is the
right design for a production book; it is listed in the roadmap and the schema
module is the single place that would change.

### 7. Idempotent batch jobs

`mdp bars` rewrites whole `(symbol, date)` partitions
(`existing_data_behavior="delete_matching"`). A re-run after late data or a
bug fix converges to the same state. No job keeps state outside the lake.

### 8. Arrow over the wire

The API serves every dataset as JSON or Arrow IPC. The Python client uses
Arrow: no parsing, exact types preserved (UTC timestamps, int64 ids), and far
fewer bytes than JSON (numbers in `bench/RESULTS.md`).

## Failure modes and how they are handled

| Failure | Behaviour |
|---|---|
| Websocket drops | Reconnect with capped exponential backoff and jitter; `mdp_ws_reconnects_total` increments; sequence state kept so the loss is visible as a gap |
| Exchange sends a duplicate / replays | Dropped online, counted in `mdp_trade_duplicates_total`; offline quality job double-checks |
| Process killed | Buffered rows up to `flush_seconds` are lost and show up as a gap in the next quality run. Acceptable for a research lake; a WAL is the fix if it is not |
| Disk slow | Queue grows (`mdp_queue_depth`); bounded at 200k so memory is capped; reader back-pressures |
| Bad tick | Not filtered at ingest (raw is raw). Flagged by the quality job's `n_price_outliers` |
| Clock skew on the host | Latency histogram goes negative or huge; clamped at 0 online, visible in the quality report |
| Bars job crashes mid-run | Partition is rewritten atomically next run |

## What I would do differently at 100x the volume

* Replace the Python parse path with a Rust extension (roadmap M4). Parsing is
  the hot path; everything after it is columnar.
* One writer process per symbol group, to use more than one core.
* A manifest (Iceberg) once file counts make directory listing slow.
* ClickHouse or a Parquet-backed Trino cluster when queries need to fan out
  across machines.
