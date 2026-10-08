from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mdp.ingest.writer import ParquetWriter
from mdp.schemas import BookTicker, Stream, Trade
from mdp.storage.catalog import Catalog, interval_to_us
from tests.conftest import N_TRADES


def test_hive_layout_and_partition_columns_not_in_file(lake: Path):
    files = sorted((lake / "raw" / "trades").rglob("*.parquet"))
    assert files, "no parquet written"
    rel = files[0].relative_to(lake / "raw" / "trades").as_posix()
    assert rel.startswith("symbol=BTCUSDT/date=2025-10-08/hour=")
    schema = pq.read_schema(files[0])
    assert "symbol" not in schema.names and "date" not in schema.names
    assert schema.field("ts_event").type == pa.timestamp("us", tz="UTC")
    assert sum(pq.read_metadata(f).num_rows for f in files) == N_TRADES


def test_catalog_roundtrip_count_order_and_hive_columns(catalog: Catalog, trades: list[Trade]):
    assert catalog.symbols() == ["BTCUSDT"]
    assert catalog.dates("BTCUSDT") == ["2025-10-08"]
    t = catalog.trades("BTCUSDT")
    assert t.num_rows == N_TRADES
    ids = t["trade_id"].to_pylist()
    assert ids == sorted(ids) and ids[0] == 1
    assert set(t["symbol"].to_pylist()) == {"BTCUSDT"}
    assert t["hour"].to_pylist()[0] == 0
    # values survive the trip
    first = t.slice(0, 1).to_pylist()[0]
    assert first["price"] == trades[0].price and first["qty"] == trades[0].qty
    assert first["ts_event"] == datetime.fromtimestamp(trades[0].ts_event_us / 1e6, tz=UTC)


def test_catalog_time_range_filter_and_limit(catalog: Catalog, trades: list[Trade]):
    start = datetime.fromtimestamp(trades[0].ts_event_us / 1e6, tz=UTC) + timedelta(minutes=1)
    end = start + timedelta(minutes=2)
    expected = [
        tr for tr in trades if start.timestamp() * 1e6 <= tr.ts_event_us < end.timestamp() * 1e6
    ]
    t = catalog.trades("BTCUSDT", start, end)
    assert t.num_rows == len(expected) > 0
    assert catalog.trades("BTCUSDT", start, end, limit=7).num_rows == 7
    # naive datetimes are interpreted as UTC
    assert catalog.trades(
        "BTCUSDT", start.replace(tzinfo=None), end.replace(tzinfo=None)
    ).num_rows == len(expected)


def test_catalog_unknown_symbol_returns_typed_empty_table(catalog: Catalog):
    t = catalog.trades("NOPE")
    assert t.num_rows == 0 and "trade_id" in t.schema.names
    assert catalog.bars("BTCUSDT", "1m").num_rows == 0
    assert catalog.book_ticker("BTCUSDT").num_rows == 0


def test_book_ticker_roundtrip(tmp_path: Path):
    w = ParquetWriter(tmp_path / "raw", flush_rows=100)
    base = 1_759_881_600_000_000
    for i in range(250):
        w.append(BookTicker("ETHUSDT", 1000 + i, 3000.0, 1.0, 3000.1, 2.0, base + i * 1_000_000))
    w.flush_all()
    c = Catalog(tmp_path)
    t = c.book_ticker("ETHUSDT")
    assert t.num_rows == 250
    assert t["update_id"].to_pylist() == list(range(1000, 1250))
    assert c.symbols(Stream.BOOK_TICKER) == ["ETHUSDT"]
    assert c.symbols(Stream.TRADES) == []


def test_interval_parsing():
    assert interval_to_us("1s") == 1_000_000
    assert interval_to_us("5m") == 300_000_000
    assert interval_to_us("1h") == 3_600_000_000
    for bad in ("1x", "m", "", "1.5m"):
        try:
            interval_to_us(bad)
        except ValueError:
            continue
        raise AssertionError(bad)


async def test_writer_run_flushes_on_cancel_and_on_timer(tmp_path: Path, trades: list[Trade]):
    w = ParquetWriter(tmp_path / "raw", flush_rows=1_000_000, flush_seconds=0.2)
    q: asyncio.Queue = asyncio.Queue()
    for tr in trades[:500]:
        q.put_nowait(tr)
    task = asyncio.create_task(w.run(q))
    await asyncio.sleep(0.6)  # timer flush should have fired
    assert w.rows_written == 500
    for tr in trades[500:600]:
        q.put_nowait(tr)
    await asyncio.sleep(0)  # let the loop pick up the queue, then cancel: drain path
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert w.rows_written == 600
    assert Catalog(tmp_path).trades("BTCUSDT").num_rows == 600
