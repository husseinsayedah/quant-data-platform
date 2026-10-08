"""Batch job: trades -> OHLCV bars.

Bars are computed in DuckDB directly over the raw Parquet partitions and
written back as their own partitioned dataset:
    data/bars/interval=1m/symbol=BTCUSDT/date=2026-10-08/part-*.parquet

The job is idempotent per (symbol, interval, date): re-running replaces the
matching partition, so late data or a bug fix is handled by a plain re-run.
Bucket boundaries are computed in integer microseconds, which removes any
dependency on session time zones.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from mdp.schemas import BAR_SCHEMA, Stream
from mdp.storage.catalog import Catalog, _arrow, interval_to_us

log = logging.getLogger(__name__)

BAR_PARTITIONING = ds.partitioning(
    pa.schema([pa.field("symbol", pa.string()), pa.field("date", pa.string())]), flavor="hive"
)
BAR_SCHEMA_WITH_DATE = BAR_SCHEMA.append(pa.field("date", pa.string()))


@dataclass
class BarJobResult:
    symbol: str
    interval: str
    dates: list[str]
    n_trades: int
    n_bars: int
    seconds: float


def bars_sql(scan: str, interval_us: int, date_filter: str) -> str:
    return f"""
        WITH t AS (
            SELECT symbol, trade_id, price, qty, is_buyer_maker, ts_event, date
            FROM {scan}
            WHERE {date_filter}
        )
        SELECT
            symbol,
            to_timestamp(((epoch_us(ts_event) // {interval_us}) * {interval_us}) / 1e6) AS ts_open,
            first(price ORDER BY ts_event, trade_id)  AS open,
            max(price)                                 AS high,
            min(price)                                 AS low,
            last(price ORDER BY ts_event, trade_id)   AS close,
            sum(qty)                                   AS volume,
            sum(CASE WHEN is_buyer_maker THEN 0 ELSE qty END) AS buy_volume,
            sum(price * qty) / sum(qty)                AS vwap,
            count(*)::BIGINT                           AS n_trades,
            CAST(date AS VARCHAR)                      AS date
        FROM t
        GROUP BY symbol, ts_open, date
        ORDER BY ts_open
    """


def compute_bars(
    catalog: Catalog, symbol: str, interval: str, dates: list[str] | None = None
) -> pa.Table:
    """Return bars for the given symbol/dates as an Arrow table (with a `date` column)."""
    interval_us = interval_to_us(interval)
    root = catalog.raw_dir(Stream.TRADES)
    if not catalog._has_files(root, symbol):
        return BAR_SCHEMA_WITH_DATE.empty_table()
    if dates:
        quoted = ", ".join(f"DATE '{d}'" for d in dates)
        date_filter = f"date IN ({quoted})"
    else:
        date_filter = "1=1"
    table = _arrow(
        catalog.con.execute(bars_sql(catalog._scan(root, symbol), interval_us, date_filter))
    )
    return table.select(BAR_SCHEMA_WITH_DATE.names).cast(BAR_SCHEMA_WITH_DATE)


def write_bars(table: pa.Table, bars_dir: Path, interval: str, compression: str = "zstd") -> int:
    if table.num_rows == 0:
        return 0
    ds.write_dataset(
        table,
        base_dir=str(bars_dir / f"interval={interval}"),
        format="parquet",
        partitioning=BAR_PARTITIONING,
        basename_template=f"part-{time.time_ns()}-{{i}}.parquet",
        existing_data_behavior="delete_matching",  # idempotent per (symbol, date)
        file_options=ds.ParquetFileFormat().make_write_options(compression=compression),
    )
    return table.num_rows


def run_bars_job(
    catalog: Catalog,
    symbol: str,
    interval: str = "1m",
    dates: list[str] | None = None,
    compression: str = "zstd",
) -> BarJobResult:
    t0 = time.perf_counter()
    dates = dates or catalog.dates(symbol)
    table = compute_bars(catalog, symbol, interval, dates)
    n_trades = int(sum(table["n_trades"].to_pylist())) if table.num_rows else 0
    n = write_bars(table, catalog.data_dir / "bars", interval, compression)
    dt = time.perf_counter() - t0
    log.info(
        "%s %s: %d trades -> %d bars over %d dates in %.2fs",
        symbol,
        interval,
        n_trades,
        n,
        len(dates),
        dt,
    )
    return BarJobResult(symbol.upper(), interval, dates, n_trades, n, dt)
