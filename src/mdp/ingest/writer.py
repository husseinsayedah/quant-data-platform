"""Buffered, partitioned Parquet writer.

Events are accumulated per stream in columnar buffers (one Python list per
column) and flushed either when the row threshold is hit or when the writer
loop has been idle for `flush_seconds`. A flush swaps the buffer out
atomically and serialises the old one on a worker thread, so the websocket
reader is never blocked on disk IO.

Layout (Hive style, readable by DuckDB / Spark / Polars with no catalog):
    data/raw/trades/symbol=BTCUSDT/date=2026-10-08/hour=14/part-<ts>-0.parquet
Partition columns live in the path only, not inside the file.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from mdp.monitor import metrics
from mdp.schemas import BOOK_TICKER_SCHEMA, TRADE_SCHEMA, Event, Stream, Trade

log = logging.getLogger(__name__)

HOUR_US = 3_600_000_000
PARTITION_FIELDS = pa.schema(
    [pa.field("symbol", pa.string()), pa.field("date", pa.string()), pa.field("hour", pa.int32())]
)


@dataclass
class ColumnBuffer:
    stream: Stream
    cols: dict[str, list] = field(default_factory=dict)
    n: int = 0

    def __post_init__(self) -> None:
        schema = TRADE_SCHEMA if self.stream is Stream.TRADES else BOOK_TICKER_SCHEMA
        self.cols = {name: [] for name in schema.names}

    def append(self, ev: Event) -> None:
        c = self.cols
        if isinstance(ev, Trade):
            c["symbol"].append(ev.symbol)
            c["trade_id"].append(ev.trade_id)
            c["price"].append(ev.price)
            c["qty"].append(ev.qty)
            c["is_buyer_maker"].append(ev.is_buyer_maker)
            c["ts_event"].append(ev.ts_event_us)
            c["ts_recv"].append(ev.ts_recv_us)
        else:
            c["symbol"].append(ev.symbol)
            c["update_id"].append(ev.update_id)
            c["bid_px"].append(ev.bid_px)
            c["bid_qty"].append(ev.bid_qty)
            c["ask_px"].append(ev.ask_px)
            c["ask_qty"].append(ev.ask_qty)
            c["ts_recv"].append(ev.ts_recv_us)
        self.n += 1

    def to_table(self) -> pa.Table:
        schema = TRADE_SCHEMA if self.stream is Stream.TRADES else BOOK_TICKER_SCHEMA
        arrays = []
        for f in schema:
            values = self.cols[f.name]
            if pa.types.is_timestamp(f.type):
                arrays.append(pa.array(values, pa.int64()).cast(f.type))
            else:
                arrays.append(pa.array(values, f.type))
        return pa.Table.from_arrays(arrays, schema=schema)


def add_partition_columns(table: pa.Table, ts_col: str) -> pa.Table:
    """Derive date / hour partition keys from a UTC timestamp column.

    Implementation note: pc.strftime on tz-aware timestamps costs ~15 us/row
    (1.5 s per 100k rows, measured). A buffer spans very few distinct hours,
    so we bucket in integer arithmetic and format each distinct hour once.
    """
    us = table[ts_col].cast(pa.int64())  # microseconds since epoch, UTC
    bucket = pc.divide(us, HOUR_US)  # integer division: hour index since epoch
    uniq = pc.unique(bucket).to_pylist()
    dates = pa.array([datetime.fromtimestamp(b * 3600, tz=UTC).strftime("%Y-%m-%d") for b in uniq])
    hours = pa.array([b % 24 for b in uniq], pa.int32())
    idx = pc.index_in(bucket, value_set=pa.array(uniq, pa.int64()))
    return table.append_column("date", pc.take(dates, idx)).append_column(
        "hour", pc.take(hours, idx)
    )


def write_partitioned(table: pa.Table, base_dir: Path, compression: str = "zstd") -> int:
    """Write a table into the hive layout under base_dir. Returns the number of rows."""
    if table.num_rows == 0:
        return 0
    stamp = time.time_ns()
    ds.write_dataset(
        table,
        base_dir=str(base_dir),
        format="parquet",
        partitioning=ds.partitioning(PARTITION_FIELDS, flavor="hive"),
        basename_template=f"part-{stamp}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
        file_options=ds.ParquetFileFormat().make_write_options(compression=compression),
        max_rows_per_group=64_000,
    )
    return table.num_rows


class ParquetWriter:
    def __init__(
        self,
        raw_dir: Path,
        *,
        flush_rows: int = 20_000,
        flush_seconds: float = 5.0,
        compression: str = "zstd",
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.flush_rows = flush_rows
        self.flush_seconds = flush_seconds
        self.compression = compression
        self.buffers: dict[Stream, ColumnBuffer] = {s: ColumnBuffer(s) for s in Stream}
        self._gauges = {s: metrics.buffer_rows.labels(s.value) for s in Stream}
        self.rows_written = 0
        self.flushes = 0

    @staticmethod
    def _stream_of(ev: Event) -> Stream:
        return Stream.TRADES if isinstance(ev, Trade) else Stream.BOOK_TICKER

    def append(self, ev: Event) -> Stream | None:
        """Buffer an event. Returns the stream that is due for a flush, if any."""
        stream = self._stream_of(ev)
        buf = self.buffers[stream]
        buf.append(ev)
        if buf.n & 1023 == 0:  # label lookup per row is measurable; sample it
            self._gauges[stream].set(buf.n)
        return stream if buf.n >= self.flush_rows else None

    def swap(self, stream: Stream) -> ColumnBuffer:
        buf = self.buffers[stream]
        self.buffers[stream] = ColumnBuffer(stream)
        self._gauges[stream].set(0)
        return buf

    def write_buffer(self, buf: ColumnBuffer) -> int:
        """Serialise one buffer to Parquet (blocking)."""
        if buf.n == 0:
            return 0
        t0 = time.perf_counter()
        table = buf.to_table()
        ts_col = "ts_event" if buf.stream is Stream.TRADES else "ts_recv"
        table = add_partition_columns(table, ts_col)
        n = write_partitioned(table, self.raw_dir / buf.stream.value, self.compression)
        dt = time.perf_counter() - t0
        self.rows_written += n
        self.flushes += 1
        metrics.flushes_total.labels(buf.stream.value).inc()
        metrics.rows_written_total.labels(buf.stream.value).inc(n)
        metrics.flush_seconds.observe(dt)
        log.debug("flushed %d %s rows in %.1f ms", n, buf.stream.value, dt * 1e3)
        return n

    def flush(self, stream: Stream) -> int:
        return self.write_buffer(self.swap(stream))

    def flush_all(self) -> int:
        return sum(self.flush(s) for s in Stream)

    async def run(self, queue: asyncio.Queue[Event]) -> None:
        """Consume events until cancelled; always flushes remaining rows on exit."""
        last_flush = time.monotonic()
        try:
            while True:
                try:
                    ev = await asyncio.wait_for(queue.get(), timeout=self.flush_seconds)
                except TimeoutError:
                    ev = None
                if ev is not None:
                    due = self.append(ev)
                    if due is not None:
                        await asyncio.to_thread(self.write_buffer, self.swap(due))
                        last_flush = time.monotonic()
                if time.monotonic() - last_flush >= self.flush_seconds:
                    bufs = [self.swap(s) for s in Stream if self.buffers[s].n]
                    for buf in bufs:
                        await asyncio.to_thread(self.write_buffer, buf)
                    last_flush = time.monotonic()
        finally:
            # Drain whatever is left in the queue, then flush synchronously.
            while not queue.empty():
                self.append(queue.get_nowait())
            n = self.flush_all()
            log.info("writer stopped; final flush wrote %d rows (total %d)", n, self.rows_written)
