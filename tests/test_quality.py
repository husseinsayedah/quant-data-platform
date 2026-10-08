from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from mdp.batch.quality import load_latest, run_quality, write_report
from mdp.ingest.replay import synthetic_trades
from mdp.schemas import Trade
from mdp.storage.catalog import Catalog
from tests.conftest import write_trades


def test_clean_data_is_ok(catalog: Catalog):
    rep = run_quality(catalog, "BTCUSDT")
    assert rep.ok
    assert rep.n_trades == 20_000
    assert rep.n_gaps == rep.missing_ids == rep.n_duplicates == rep.n_non_monotonic == 0
    assert rep.n_price_outliers == 0
    assert 10 < rep.latency_p50_ms < 30  # synthetic latency ~ N(20, 10) ms
    assert rep.latency_p99_ms > rep.latency_p50_ms
    assert rep.max_silence_s is not None and rep.max_silence_s < 2


def test_detects_gap_duplicate_outlier_and_non_monotonic(tmp_path: Path):
    trades: list[Trade] = list(synthetic_trades("ETHUSDT", 2_000, seed=7))
    # gap: drop ids 101..105 (5 missing ids, one discontinuity)
    trades = [t for t in trades if not 101 <= t.trade_id <= 105]
    # duplicate: trade 50 appears twice
    trades.insert(49, trades[49])
    # non-monotonic: trade 300 timestamped 10s before its predecessor
    i = next(k for k, t in enumerate(trades) if t.trade_id == 300)
    trades[i] = replace(trades[i], ts_event_us=trades[i - 1].ts_event_us - 10_000_000)
    # price outlier: a permanent +5% level shift from trade 700 onwards -> exactly one jump
    trades = [
        replace(t, price=round(t.price * 1.05, 2)) if t.trade_id >= 700 else t for t in trades
    ]
    write_trades(tmp_path, trades, flush_rows=400)

    rep = run_quality(Catalog(tmp_path), "ETHUSDT", max_jump_pct=2.0)
    assert not rep.ok
    assert rep.n_trades == 2_000 - 5 + 1
    assert rep.n_gaps == 1 and rep.missing_ids == 5
    assert rep.n_duplicates == 1
    assert rep.n_non_monotonic == 1
    assert rep.n_price_outliers == 1
    assert rep.max_silence_s is not None


def test_report_written_and_loaded(catalog: Catalog, tmp_path: Path):
    rep = run_quality(catalog, "BTCUSDT")
    path = write_report(rep, tmp_path / "quality")
    assert path.exists()
    latest = load_latest(tmp_path / "quality", "btcusdt")
    assert latest == json.loads(rep.to_json())
    assert latest["ok"] is True


def test_missing_symbol_report(catalog: Catalog):
    rep = run_quality(catalog, "NOPE")
    assert rep.n_trades == 0 and rep.ok


def test_negative_latency_is_flagged_as_clock_skew(tmp_path: Path):
    # recv timestamps 700 ms *before* exchange timestamps: the local clock is behind.
    trades = [
        replace(t, ts_recv_us=t.ts_event_us - 700_000)
        for t in synthetic_trades("BTCUSDT", 500, seed=2)
    ]
    write_trades(tmp_path, trades)
    rep = run_quality(Catalog(tmp_path), "BTCUSDT")
    assert rep.ok  # skew is a warning, not a data loss
    assert rep.latency_p50_ms is not None and rep.latency_p50_ms < 0
    assert any("clock" in w for w in rep.warnings)
    assert json.loads(rep.to_json())["warnings"] == rep.warnings
