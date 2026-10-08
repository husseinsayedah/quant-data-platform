"""Binance spot websocket client.

Responsibilities
----------------
* Build the combined-stream URL for N symbols x {trade, bookTicker}.
* Parse raw JSON frames into typed events (orjson, operates on bytes directly).
* Detect trade-id gaps and duplicates per symbol (SequenceTracker).
* Reconnect with capped exponential backoff + jitter, and keep the sequence
  state across reconnects so a gap caused by downtime is recorded, not hidden.

Payload reference (spot):
  trade      {"e":"trade","E":1672515782136,"s":"BNBBTC","t":12345,"p":"0.001",
              "q":"100","T":1672515782136,"m":true,"M":true}
  bookTicker {"u":400900217,"s":"BNBUSDT","b":"25.35","B":"31.21","a":"25.36","A":"40.66"}
Combined streams wrap payloads as {"stream":"<name>","data":{...}}.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Iterable

import orjson
import websockets

from mdp.monitor import metrics
from mdp.schemas import BookTicker, Event, Trade

log = logging.getLogger(__name__)


def now_us() -> int:
    return time.time_ns() // 1_000


def stream_url(
    base: str, symbols: Iterable[str], streams: Iterable[str] = ("trade", "bookTicker")
) -> str:
    names = "/".join(f"{s.lower()}@{st}" for s in symbols for st in streams)
    return f"{base}?streams={names}"


def parse_message(raw: bytes | str, ts_recv_us: int) -> Event | None:
    """Parse one websocket frame. Returns None for acks, pings and unknown payloads."""
    msg = orjson.loads(raw)
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    if not isinstance(data, dict):
        return None
    kind = data.get("e")
    if kind == "trade":
        return Trade(
            symbol=data["s"],
            trade_id=data["t"],
            price=float(data["p"]),
            qty=float(data["q"]),
            is_buyer_maker=bool(data["m"]),
            ts_event_us=data["T"] * 1_000,
            ts_recv_us=ts_recv_us,
        )
    if kind is None and "u" in data and "b" in data and "a" in data:
        return BookTicker(
            symbol=data["s"],
            update_id=data["u"],
            bid_px=float(data["b"]),
            bid_qty=float(data["B"]),
            ask_px=float(data["a"]),
            ask_qty=float(data["A"]),
            ts_recv_us=ts_recv_us,
        )
    return None


class SequenceTracker:
    """Tracks per-symbol trade_id continuity.

    observe() returns 0 when contiguous, n>0 for the number of missing ids
    before this trade, and -1 for a duplicate / out-of-order id (drop it).
    """

    def __init__(self) -> None:
        self.last: dict[str, int] = {}

    def observe(self, symbol: str, trade_id: int) -> int:
        prev = self.last.get(symbol)
        if prev is None or trade_id == prev + 1:
            self.last[symbol] = trade_id
            return 0
        if trade_id <= prev:
            return -1
        self.last[symbol] = trade_id
        return trade_id - prev - 1


class BinanceClient:
    def __init__(
        self,
        url: str,
        queue: asyncio.Queue[Event],
        *,
        tracker: SequenceTracker | None = None,
        reconnect_base: float = 0.5,
        reconnect_max: float = 30.0,
    ) -> None:
        self.url = url
        self.queue = queue
        self.tracker = tracker or SequenceTracker()
        self.reconnect_base = reconnect_base
        self.reconnect_max = reconnect_max
        self.stats = {"messages": 0, "gaps": 0, "duplicates": 0, "reconnects": 0}

    def handle_frame(self, raw: bytes | str, ts_recv_us: int) -> Event | None:
        """Parse + sequence-check one frame."""
        ev = parse_message(raw, ts_recv_us)
        if ev is None:
            metrics.unknown_messages_total.inc()
            return None
        self.stats["messages"] += 1
        if isinstance(ev, Trade):
            metrics.messages_total.labels("trades", ev.symbol).inc()
            gap = self.tracker.observe(ev.symbol, ev.trade_id)
            if gap > 0:
                self.stats["gaps"] += 1
                metrics.gaps_total.labels(ev.symbol).inc(gap)
                log.warning(
                    "gap on %s: %d missing trade ids before %d", ev.symbol, gap, ev.trade_id
                )
            elif gap < 0:
                self.stats["duplicates"] += 1
                metrics.duplicates_total.labels(ev.symbol).inc()
                return None
            metrics.ingest_latency_seconds.labels(ev.symbol).observe(
                max(0.0, (ts_recv_us - ev.ts_event_us) / 1e6)
            )
        else:
            metrics.messages_total.labels("book_ticker", ev.symbol).inc()
        return ev

    async def run(self) -> None:
        """Run until cancelled. Reconnects forever with exponential backoff."""
        attempt = 0
        while True:
            try:
                log.info("connecting %s", self.url)
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20, max_queue=None
                ) as ws:
                    attempt = 0
                    metrics.connected.set(1)
                    log.info("connected")
                    async for raw in ws:
                        ev = self.handle_frame(raw, now_us())
                        if ev is not None:
                            await self.queue.put(ev)
                            metrics.queue_depth.set(self.queue.qsize())
            except asyncio.CancelledError:
                metrics.connected.set(0)
                raise
            except Exception as exc:  # noqa: BLE001 - any transport failure triggers a reconnect
                metrics.connected.set(0)
                metrics.reconnects_total.inc()
                self.stats["reconnects"] += 1
                delay = min(self.reconnect_max, self.reconnect_base * 2**attempt)
                delay *= 0.5 + random.random()  # jitter avoids thundering herd on exchange restarts
                attempt += 1
                log.warning(
                    "disconnected (%s: %s); reconnecting in %.1fs", type(exc).__name__, exc, delay
                )
                await asyncio.sleep(delay)
