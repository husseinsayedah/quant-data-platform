"""Event types and their Arrow schemas.

Design notes
------------
* Timestamps are stored as UTC microsecond timestamps. Binance publishes
  millisecond event times; receive time is taken locally in nanoseconds and
  truncated to microseconds, which is well below the network jitter floor.
* Prices and quantities are float64. Research consumers want floats; exact
  decimal accounting (int64 ticks) is a roadmap item, see docs/design.md.
* `trade_id` is the exchange-assigned, per-symbol monotonically increasing id.
  It is the basis of gap and duplicate detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")


class Stream(StrEnum):
    TRADES = "trades"
    BOOK_TICKER = "book_ticker"


@dataclass(slots=True, frozen=True)
class Trade:
    symbol: str
    trade_id: int
    price: float
    qty: float
    is_buyer_maker: bool
    ts_event_us: int  # exchange timestamp, microseconds since epoch (UTC)
    ts_recv_us: int  # local receive timestamp, microseconds since epoch (UTC)


@dataclass(slots=True, frozen=True)
class BookTicker:
    symbol: str
    update_id: int
    bid_px: float
    bid_qty: float
    ask_px: float
    ask_qty: float
    ts_recv_us: int


TRADE_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("qty", pa.float64(), nullable=False),
        pa.field("is_buyer_maker", pa.bool_(), nullable=False),
        pa.field("ts_event", TS, nullable=False),
        pa.field("ts_recv", TS, nullable=False),
    ]
)

BOOK_TICKER_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("update_id", pa.int64(), nullable=False),
        pa.field("bid_px", pa.float64(), nullable=False),
        pa.field("bid_qty", pa.float64(), nullable=False),
        pa.field("ask_px", pa.float64(), nullable=False),
        pa.field("ask_qty", pa.float64(), nullable=False),
        pa.field("ts_recv", TS, nullable=False),
    ]
)

BAR_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("ts_open", TS, nullable=False),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("buy_volume", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("n_trades", pa.int64()),
    ]
)

SCHEMAS: dict[Stream, pa.Schema] = {
    Stream.TRADES: TRADE_SCHEMA,
    Stream.BOOK_TICKER: BOOK_TICKER_SCHEMA,
}

Event = Trade | BookTicker
