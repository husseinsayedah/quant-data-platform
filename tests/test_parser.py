from __future__ import annotations

import asyncio

import orjson

from mdp.ingest.binance import BinanceClient, SequenceTracker, parse_message, stream_url
from mdp.ingest.replay import synthetic_frames, synthetic_trades, trade_to_frame
from mdp.schemas import BookTicker, Trade

TRADE_FRAME = (
    b'{"stream":"btcusdt@trade","data":{"e":"trade","E":1672515782136,"s":"BTCUSDT",'
    b'"t":12345,"p":"16500.10000000","q":"0.00200000","T":1672515782130,"m":true,"M":true}}'
)
BOOK_FRAME_WRAPPED = (
    b'{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT",'
    b'"b":"16500.00","B":"3.1","a":"16500.10","A":"0.5"}}'
)
BOOK_FRAME_RAW = b'{"u":400900217,"s":"BTCUSDT","b":"16500.00","B":"3.1","a":"16500.10","A":"0.5"}'
ACK_FRAME = b'{"result":null,"id":1}'


def test_stream_url():
    url = stream_url("wss://x/stream", ["BTCUSDT", "ethusdt"])
    assert url == (
        "wss://x/stream?streams=btcusdt@trade/btcusdt@bookTicker/ethusdt@trade/ethusdt@bookTicker"
    )


def test_parse_trade():
    ev = parse_message(TRADE_FRAME, ts_recv_us=1672515782150_000)
    assert isinstance(ev, Trade)
    assert ev.symbol == "BTCUSDT"
    assert ev.trade_id == 12345
    assert ev.price == 16500.1
    assert ev.qty == 0.002
    assert ev.is_buyer_maker is True
    assert ev.ts_event_us == 1672515782130_000  # trade time T, not event time E
    assert ev.ts_recv_us == 1672515782150_000


def test_parse_book_ticker_wrapped_and_raw():
    for frame in (BOOK_FRAME_WRAPPED, BOOK_FRAME_RAW):
        ev = parse_message(frame, ts_recv_us=1)
        assert isinstance(ev, BookTicker)
        assert ev.update_id == 400900217
        assert (ev.bid_px, ev.bid_qty, ev.ask_px, ev.ask_qty) == (16500.0, 3.1, 16500.1, 0.5)


def test_parse_ignores_acks_and_garbage():
    assert parse_message(ACK_FRAME, 0) is None
    assert parse_message(b"[1,2,3]", 0) is None
    assert parse_message(b'{"data":{"e":"aggTrade"}}', 0) is None


def test_frame_roundtrip_matches_synthetic_trade():
    tr = next(synthetic_trades("ETHUSDT", 1, seed=3))
    ev = parse_message(trade_to_frame(tr), tr.ts_recv_us)
    assert ev == tr


def test_sequence_tracker():
    s = SequenceTracker()
    assert s.observe("A", 10) == 0
    assert s.observe("A", 11) == 0
    assert s.observe("A", 15) == 3  # 12, 13, 14 missing
    assert s.observe("A", 15) == -1  # duplicate
    assert s.observe("A", 13) == -1  # out of order
    assert s.observe("B", 1) == 0  # independent per symbol
    assert s.last == {"A": 15, "B": 1}


def test_client_handle_frame_drops_duplicates_and_counts_gaps():
    client = BinanceClient("wss://unused", asyncio.Queue())
    frames = synthetic_frames(10, "BTCUSDT")
    out = [client.handle_frame(f, 0) for f in frames]
    assert all(isinstance(e, Trade) for e in out)
    # replay frame 5 (duplicate) and skip to a later id (gap)
    assert client.handle_frame(frames[5], 0) is None
    tr = next(synthetic_trades("BTCUSDT", 1, first_trade_id=20))
    assert isinstance(client.handle_frame(trade_to_frame(tr), 0), Trade)
    assert client.stats["duplicates"] == 1
    assert client.stats["gaps"] == 1
    assert client.handle_frame(ACK_FRAME, 0) is None


def test_parse_handles_str_frames_like_websockets_delivers():
    ev = parse_message(TRADE_FRAME.decode(), 0)
    assert isinstance(ev, Trade)
    assert orjson.loads(TRADE_FRAME)["data"]["t"] == ev.trade_id
