"""Offline sources: synthetic market data and replay of captured frames.

Tests and benchmarks run from here, so the project can be exercised end to
end without network access.
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterator
from pathlib import Path

import orjson

from mdp.schemas import Trade


def synthetic_trades(
    symbol: str = "BTCUSDT",
    n: int = 100_000,
    *,
    start_us: int = 1_759_881_600_000_000,  # 2025-10-08 00:00:00 UTC
    rate_per_sec: float = 50.0,
    start_price: float = 60_000.0,
    vol_per_trade: float = 0.0002,
    seed: int = 0,
    first_trade_id: int = 1,
    latency_ms: tuple[float, float] = (20.0, 10.0),
) -> Iterator[Trade]:
    """Geometric random-walk trades with exponential inter-arrival times.

    Timestamps are rounded to milliseconds like the real exchange feed, so
    several trades can share one ts_event; trade_id breaks the tie.
    """
    rng = random.Random(seed)
    price = start_price
    t = start_us
    for i in range(n):
        t += int(rng.expovariate(rate_per_sec) * 1e6)
        price *= math.exp(rng.gauss(0.0, vol_per_trade))
        px = round(price, 2)
        qty = round(rng.lognormvariate(-3.0, 1.0), 5) or 1e-5
        ts_event = (t // 1000) * 1000
        lat = max(1.0, rng.gauss(*latency_ms)) * 1000
        yield Trade(
            symbol=symbol,
            trade_id=first_trade_id + i,
            price=px,
            qty=qty,
            is_buyer_maker=rng.random() < 0.5,
            ts_event_us=ts_event,
            ts_recv_us=ts_event + int(lat),
        )


def trade_to_frame(tr: Trade, stream_name: str | None = None) -> bytes:
    """Encode a Trade as the JSON frame Binance would send (combined-stream form)."""
    payload = {
        "e": "trade",
        "E": tr.ts_event_us // 1000,
        "s": tr.symbol,
        "t": tr.trade_id,
        "p": f"{tr.price:.8f}",
        "q": f"{tr.qty:.8f}",
        "T": tr.ts_event_us // 1000,
        "m": tr.is_buyer_maker,
        "M": True,
    }
    return orjson.dumps({"stream": stream_name or f"{tr.symbol.lower()}@trade", "data": payload})


def synthetic_frames(n: int, symbol: str = "BTCUSDT", seed: int = 0) -> list[bytes]:
    return [trade_to_frame(t) for t in synthetic_trades(symbol, n, seed=seed)]


def iter_jsonl(path: Path) -> Iterator[tuple[bytes, int]]:
    """Yield (raw_frame, ts_recv_us) from a capture file written by `mdp capture`."""
    with open(path, "rb") as fh:
        for line in fh:
            rec = orjson.loads(line)
            yield rec["raw"].encode(), rec["ts_recv_us"]
