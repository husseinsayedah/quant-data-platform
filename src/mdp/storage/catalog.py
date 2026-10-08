"""DuckDB catalog over the Parquet lake.

There is no metadata service: the directory layout *is* the catalog. DuckDB
reads the hive partitions (symbol / date / hour) straight from the paths and
prunes partitions and row groups using the filters we pass. Symbol filters are
also applied in the glob so DuckDB never lists other symbols' files.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from mdp.schemas import BAR_SCHEMA, BOOK_TICKER_SCHEMA, TRADE_SCHEMA, Stream

_INTERVAL_RE = re.compile(r"^(\d+)(s|m|h|d)$")
_UNIT_US = {"s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000, "d": 86_400_000_000}


def interval_to_us(interval: str) -> int:
    m = _INTERVAL_RE.match(interval)
    if not m:
        raise ValueError(f"bad interval {interval!r}; expected e.g. 1s, 1m, 5m, 1h, 1d")
    return int(m.group(1)) * _UNIT_US[m.group(2)]


def _arrow(result) -> pa.Table:
    """DuckDB >= 1.4 names it to_arrow_table(); older versions fetch_arrow_table()."""
    fn = getattr(result, "to_arrow_table", None) or result.fetch_arrow_table
    return fn()


def to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def with_partition_columns(schema: pa.Schema) -> pa.Schema:
    """Schema of a raw stream as DuckDB exposes it (hive columns appended)."""
    out = schema
    for name, typ in (("symbol", pa.string()), ("date", pa.date32()), ("hour", pa.int64())):
        if name not in out.names:
            out = out.append(pa.field(name, typ))
    return out


class Catalog:
    def __init__(self, data_dir: Path | str = "data", threads: int | None = None) -> None:
        self.data_dir = Path(data_dir)
        self.con = duckdb.connect()
        if threads:
            self.con.execute(f"SET threads={int(threads)}")
        try:
            self.con.execute("SET TimeZone='UTC'")
        except duckdb.Error:  # ICU extension missing; the SQL below avoids tz-dependent functions
            pass

    # ---- paths -----------------------------------------------------------
    def raw_dir(self, stream: Stream) -> Path:
        return self.data_dir / "raw" / stream.value

    def bars_dir(self, interval: str) -> Path:
        return self.data_dir / "bars" / f"interval={interval}"

    @staticmethod
    def _glob(root: Path, symbol: str | None) -> str:
        base = root / f"symbol={symbol.upper()}" if symbol else root
        return (base / "**" / "*.parquet").as_posix()

    @staticmethod
    def _has_files(root: Path, symbol: str | None) -> bool:
        base = root / f"symbol={symbol.upper()}" if symbol else root
        return base.is_dir() and any(base.rglob("*.parquet"))

    def symbols(self, stream: Stream = Stream.TRADES) -> list[str]:
        root = self.raw_dir(stream)
        if not root.is_dir():
            return []
        return sorted(
            p.name.split("=", 1)[1] for p in root.iterdir() if p.name.startswith("symbol=")
        )

    def dates(self, symbol: str, stream: Stream = Stream.TRADES) -> list[str]:
        root = self.raw_dir(stream) / f"symbol={symbol.upper()}"
        if not root.is_dir():
            return []
        return sorted(p.name.split("=", 1)[1] for p in root.iterdir() if p.name.startswith("date="))

    # ---- queries ---------------------------------------------------------
    def _scan(self, root: Path, symbol: str | None) -> str:
        # Paths are embedded as SQL literals: escape quotes (Windows user names can contain them).
        path = self._glob(root, symbol).replace("'", "''")
        return f"read_parquet('{path}', hive_partitioning=true)"

    @staticmethod
    def _range(col: str, start: datetime | None, end: datetime | None) -> tuple[str, list]:
        where, params = ["1=1"], []
        if start is not None:
            where.append(f"{col} >= ?")
            params.append(to_utc(start))
        if end is not None:
            where.append(f"{col} < ?")
            params.append(to_utc(end))
        return " AND ".join(where), params

    def trades(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        root = self.raw_dir(Stream.TRADES)
        if not self._has_files(root, symbol):
            return with_partition_columns(TRADE_SCHEMA).empty_table()
        where, params = self._range("ts_event", start, end)
        sql = (
            "SELECT symbol, trade_id, price, qty, is_buyer_maker, ts_event, ts_recv, date, hour "
            f"FROM {self._scan(root, symbol)} WHERE {where} ORDER BY ts_event, trade_id"
        )
        if limit:
            sql += f" LIMIT {int(limit)}"
        return _arrow(self.con.execute(sql, params))

    def book_ticker(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        root = self.raw_dir(Stream.BOOK_TICKER)
        if not self._has_files(root, symbol):
            return with_partition_columns(BOOK_TICKER_SCHEMA).empty_table()
        where, params = self._range("ts_recv", start, end)
        sql = f"SELECT * FROM {self._scan(root, symbol)} WHERE {where} ORDER BY ts_recv, update_id"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return _arrow(self.con.execute(sql, params))

    def bars(
        self,
        symbol: str,
        interval: str = "1m",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> pa.Table:
        interval_to_us(interval)  # validate
        root = self.bars_dir(interval)
        if not self._has_files(root, symbol):
            return BAR_SCHEMA.empty_table()
        where, params = self._range("ts_open", start, end)
        cols = ", ".join(BAR_SCHEMA.names)
        sql = f"SELECT {cols} FROM {self._scan(root, symbol)} WHERE {where} ORDER BY ts_open"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return _arrow(self.con.execute(sql, params)).cast(BAR_SCHEMA)

    def sql(self, query: str, params: list | None = None) -> pa.Table:
        """Escape hatch for research: arbitrary SQL over the lake."""
        return _arrow(self.con.execute(query, params or []))

    def close(self) -> None:
        self.con.close()
