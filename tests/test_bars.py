from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from mdp.batch.bars import compute_bars, run_bars_job
from mdp.schemas import Trade
from mdp.storage.catalog import Catalog


def pandas_bars(trades: list[Trade], freq: str) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "ts": pd.to_datetime([t.ts_event_us for t in trades], unit="us", utc=True),
            "trade_id": [t.trade_id for t in trades],
            "price": [t.price for t in trades],
            "qty": [t.qty for t in trades],
            "buy": [0.0 if t.is_buyer_maker else t.qty for t in trades],
        }
    ).sort_values(["ts", "trade_id"])
    df["notional"] = df.price * df.qty
    g = df.groupby(df.ts.dt.floor(freq))
    out = pd.DataFrame(
        {
            "open": g.price.first(),
            "high": g.price.max(),
            "low": g.price.min(),
            "close": g.price.last(),
            "volume": g.qty.sum(),
            "buy_volume": g.buy.sum(),
            "vwap": g.notional.sum() / g.qty.sum(),
            "n_trades": g.size(),
        }
    )
    return out


@pytest.mark.parametrize("interval,freq", [("1m", "1min"), ("5m", "5min"), ("10s", "10s")])
def test_bars_match_pandas_reference(
    catalog: Catalog, trades: list[Trade], interval: str, freq: str
):
    got = compute_bars(catalog, "BTCUSDT", interval).to_pandas().set_index("ts_open")
    ref = pandas_bars(trades, freq)
    assert len(got) == len(ref)
    assert list(got.index) == list(ref.index)
    for col in ("open", "high", "low", "close", "volume", "buy_volume", "vwap"):
        pd.testing.assert_series_equal(
            got[col].astype(float), ref[col].astype(float), check_names=False, rtol=1e-9
        )
    assert got.n_trades.tolist() == ref.n_trades.tolist()
    assert got.n_trades.sum() == len(trades)


def test_bars_job_is_idempotent(catalog: Catalog):
    r1 = run_bars_job(catalog, "BTCUSDT", "1m")
    r2 = run_bars_job(catalog, "BTCUSDT", "1m")
    assert r1.n_bars == r2.n_bars > 0
    part = catalog.data_dir / "bars" / "interval=1m" / "symbol=BTCUSDT" / "date=2025-10-08"
    assert len(list(part.glob("*.parquet"))) == 1, "re-run must replace, not append"
    bars = catalog.bars("BTCUSDT", "1m")
    assert bars.num_rows == r1.n_bars
    assert bars.column("ts_open").to_pylist() == sorted(bars.column("ts_open").to_pylist())


def test_bars_for_unknown_symbol_is_empty(catalog: Catalog):
    assert compute_bars(catalog, "XXX", "1m").num_rows == 0
    assert run_bars_job(catalog, "XXX", "1m").n_bars == 0


def test_bars_rejects_bad_interval(catalog: Catalog):
    with pytest.raises(ValueError):
        compute_bars(catalog, "BTCUSDT", "1 minute")


def test_bars_date_filter(catalog: Catalog, tmp_path: Path):
    assert compute_bars(catalog, "BTCUSDT", "1m", dates=["1999-01-01"]).num_rows == 0
    assert compute_bars(catalog, "BTCUSDT", "1m", dates=["2025-10-08"]).num_rows > 0
