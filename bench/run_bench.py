"""Reproducible benchmarks for the whole pipeline.

    python bench/run_bench.py --n 1_000_000

Everything runs on synthetic data in a temp directory, so results are
comparable across machines. Writes bench/RESULTS.md.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
from fastapi.testclient import TestClient

from mdp.api.app import create_app
from mdp.batch.bars import run_bars_job
from mdp.batch.quality import run_quality
from mdp.ingest.binance import BinanceClient, parse_message
from mdp.ingest.replay import synthetic_frames, synthetic_trades
from mdp.ingest.writer import ParquetWriter
from mdp.schemas import Stream, Trade
from mdp.storage.catalog import Catalog

ROOT = Path(__file__).resolve().parent


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


def timeit(fn, reps: int) -> list[float]:
    out = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t0) * 1e3)
    return out


def stdlib_parse(raw: bytes, ts: int) -> Trade | None:
    """Same logic as parse_message but using json from the standard library."""
    d = json.loads(raw)["data"]
    if d.get("e") != "trade":
        return None
    return Trade(d["s"], d["t"], float(d["p"]), float(d["q"]), bool(d["m"]), d["T"] * 1000, ts)


def bench_parse(n: int) -> dict:
    frames = synthetic_frames(n)
    raw_bytes = sum(len(f) for f in frames)

    t0 = time.perf_counter()
    for f in frames:
        parse_message(f, 0)
    dt_orjson = time.perf_counter() - t0

    t0 = time.perf_counter()
    for f in frames:
        stdlib_parse(f, 0)
    dt_json = time.perf_counter() - t0

    client = BinanceClient("wss://unused", asyncio.Queue())
    t0 = time.perf_counter()
    for f in frames:
        client.handle_frame(f, 0)
    dt_handle = time.perf_counter() - t0

    return {
        "frames": n,
        "avg_frame_bytes": raw_bytes / n,
        "parse_orjson_msgs_per_s": n / dt_orjson,
        "parse_stdlib_msgs_per_s": n / dt_json,
        "handle_frame_msgs_per_s": n / dt_handle,  # parse + sequence check + metrics
        "raw_bytes": raw_bytes,
    }


def bench_write(trades: list[Trade], data_dir: Path) -> dict:
    w = ParquetWriter(data_dir / "raw", flush_rows=100_000)
    t0 = time.perf_counter()
    for tr in trades:
        due = w.append(tr)
        if due:
            w.flush(due)
    w.flush_all()
    dt = time.perf_counter() - t0
    files = list((data_dir / "raw").rglob("*.parquet"))
    size = sum(f.stat().st_size for f in files)
    # serialisation cost alone (no per-row Python append): table build + write of one buffer
    from mdp.ingest.writer import ColumnBuffer
    from mdp.schemas import Stream

    buf = ColumnBuffer(Stream.TRADES)
    for tr in trades[:100_000]:
        buf.append(tr)
    t0 = time.perf_counter()
    w.write_buffer(buf)
    dt_flush = time.perf_counter() - t0
    return {
        "rows": len(trades),
        "append_and_write_rows_per_s": len(trades) / dt,
        "flush_100k_ms": dt_flush * 1e3,
        "parquet_files": len(files),
        "parquet_bytes": size,
        "bytes_per_row": size / len(trades),
    }


def bench_batch(cat: Catalog) -> dict:
    r = run_bars_job(cat, "BTCUSDT", "1m")
    q0 = time.perf_counter()
    rep = run_quality(cat, "BTCUSDT")
    dq = time.perf_counter() - q0
    return {
        "bars_1m_seconds": r.seconds,
        "bars_trades_per_s": r.n_trades / r.seconds,
        "n_bars": r.n_bars,
        "quality_seconds": dq,
        "quality_trades_per_s": rep.n_trades / dq,
    }


def bench_query(cat: Catalog, trades: list[Trade], reps: int = 30) -> dict:
    start = datetime.fromtimestamp(trades[0].ts_event_us / 1e6, tz=UTC) + timedelta(minutes=30)
    end = start + timedelta(minutes=5)
    rng = timeit(lambda: cat.trades("BTCUSDT", start, end), reps)
    n_rng = cat.trades("BTCUSDT", start, end).num_rows
    full = timeit(lambda: cat.trades("BTCUSDT"), 5)
    bars = timeit(lambda: cat.bars("BTCUSDT", "1m"), reps)
    scan = cat._scan(cat.raw_dir(Stream.TRADES), "BTCUSDT")
    agg = timeit(lambda: cat.sql(f"SELECT count(*), sum(qty), max(price) FROM {scan}"), reps)
    return {
        "range_5min_rows": n_rng,
        "range_5min_p50_ms": pct(rng, 0.5),
        "range_5min_p99_ms": pct(rng, 0.99),
        "full_scan_rows": len(trades),
        "full_scan_p50_ms": statistics.median(full),
        "bars_read_p50_ms": pct(bars, 0.5),
        "agg_full_table_p50_ms": pct(agg, 0.5),
    }


def bench_api(data_dir: Path, reps: int = 20) -> dict:
    with TestClient(create_app(data_dir)) as c:
        p = {"symbol": "BTCUSDT", "interval": "1m"}
        js = timeit(lambda: c.get("/bars", params=p), reps)
        ar = timeit(lambda: c.get("/bars", params={**p, "format": "arrow"}), reps)
        js_bytes = len(c.get("/bars", params=p).content)
        ar_bytes = len(c.get("/bars", params={**p, "format": "arrow"}).content)
        tr = timeit(
            lambda: c.get(
                "/trades", params={"symbol": "BTCUSDT", "limit": 10_000, "format": "arrow"}
            ),
            reps,
        )
    return {
        "bars_json_p50_ms": pct(js, 0.5),
        "bars_arrow_p50_ms": pct(ar, 0.5),
        "bars_json_bytes": js_bytes,
        "bars_arrow_bytes": ar_bytes,
        "trades_10k_arrow_p50_ms": pct(tr, 0.5),
    }


def fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.1f}" if abs(v) >= 10 else f"{v:,.3f}"
    return f"{v:,}" if isinstance(v, int) else str(v)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1_000_000, help="synthetic trades for write/query")
    ap.add_argument("--parse-n", type=int, default=300_000)
    ap.add_argument("--out", type=Path, default=ROOT / "RESULTS.md")
    args = ap.parse_args()

    print(f"generating {args.n:,} synthetic trades ...", file=sys.stderr)
    t0 = time.perf_counter()
    trades = list(synthetic_trades("BTCUSDT", args.n, rate_per_sec=200, seed=0))
    gen_s = time.perf_counter() - t0

    sections: dict[str, dict] = {}
    print("parse ...", file=sys.stderr)
    sections["Parsing (synthetic Binance frames)"] = bench_parse(args.parse_n)
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        print("write ...", file=sys.stderr)
        sections["Parquet writer"] = bench_write(trades, data_dir)
        cat = Catalog(data_dir)
        print("batch ...", file=sys.stderr)
        sections["Batch jobs (DuckDB over Parquet)"] = bench_batch(cat)
        print("query ...", file=sys.stderr)
        sections["Queries (DuckDB, cold process, warm OS cache)"] = bench_query(cat, trades)
        cat.close()
        print("api ...", file=sys.stderr)
        sections["API (in-process ASGI test client)"] = bench_api(data_dir)

    env = {
        "date": datetime.now(tz=UTC).strftime("%Y-%m-%d"),
        "machine": (
            f"{platform.system()} {platform.release()}, "
            f"{os.cpu_count()} logical CPUs, {platform.machine()}"
        ),
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "duckdb": duckdb.__version__,
        "synthetic_trades": args.n,
        "generation_seconds": round(gen_s, 1),
    }
    lines = ["# Benchmark results", "", "Generated by `python bench/run_bench.py`.", ""]
    lines += [f"- **{k}**: {v}" for k, v in env.items()] + [""]
    for title, res in sections.items():
        lines += [f"## {title}", "", "| metric | value |", "|---|---|"]
        lines += [f"| {k} | {fmt(v)} |" for k, v in res.items()]
        lines.append("")
    args.out.write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
