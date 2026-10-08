"""Example research consumer: a moving-average crossover on 1-minute bars.

This is not a trading strategy pitch. It exists to show the platform from the
researcher's seat: pull bars through the client (or straight from the lake),
get a DataFrame, compute signals, evaluate.

    python research/backtest.py --symbol BTCUSDT --fast 10 --slow 60
    python research/backtest.py --api http://localhost:8000
"""

from __future__ import annotations

import argparse
import math

import numpy as np
import pandas as pd

from mdp.storage.catalog import Catalog, interval_to_us


def load_bars(args) -> pd.DataFrame:
    if args.api:
        from mdp.api.client import MDPClient

        return MDPClient(args.api).bars(args.symbol, args.interval)
    cat = Catalog(args.data_dir)
    df = cat.bars(args.symbol, args.interval).to_pandas()
    return df.set_index("ts_open")


def evaluate(bars: pd.DataFrame, fast: int, slow: int, cost_bps: float, interval: str) -> dict:
    px = bars["close"]
    sig = np.sign(px.rolling(fast).mean() - px.rolling(slow).mean()).shift(1).fillna(0)
    ret = px.pct_change().fillna(0)
    turnover = sig.diff().abs().fillna(0)
    strat = sig * ret - turnover * cost_bps / 1e4
    bars_per_year = 365 * 24 * 3600 * 1_000_000 / interval_to_us(interval)
    equity = (1 + strat).cumprod()
    dd = equity / equity.cummax() - 1
    vol = strat.std()
    return {
        "bars": len(bars),
        "period": f"{bars.index[0]} -> {bars.index[-1]}",
        "total_return_pct": (equity.iloc[-1] - 1) * 100,
        "buy_and_hold_pct": (px.iloc[-1] / px.iloc[0] - 1) * 100,
        "annualised_sharpe": (strat.mean() / vol * math.sqrt(bars_per_year))
        if vol > 0
        else float("nan"),
        "max_drawdown_pct": dd.min() * 100,
        "n_position_changes": int((turnover > 0).sum()),
        "time_in_market_pct": (sig != 0).mean() * 100,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--interval", default="1m")
    ap.add_argument("--fast", type=int, default=10)
    ap.add_argument("--slow", type=int, default=60)
    ap.add_argument(
        "--cost-bps", type=float, default=2.0, help="round-trip cost per position change"
    )
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--api", default=None, help="use the HTTP API instead of reading the lake")
    args = ap.parse_args()

    bars = load_bars(args)
    if bars.empty:
        raise SystemExit("no bars; run `mdp synth` (or `mdp ingest`) then `mdp bars` first")
    res = evaluate(bars, args.fast, args.slow, args.cost_bps, args.interval)
    width = max(len(k) for k in res)
    print(f"SMA({args.fast}/{args.slow}) crossover on {args.symbol} {args.interval}")
    for k, v in res.items():
        print(f"  {k:<{width}}  {v:,.2f}" if isinstance(v, float) else f"  {k:<{width}}  {v}")


if __name__ == "__main__":
    main()
