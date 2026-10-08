from __future__ import annotations

from pathlib import Path

import pytest

from mdp.ingest.replay import synthetic_trades
from mdp.ingest.writer import ParquetWriter
from mdp.schemas import Trade
from mdp.storage.catalog import Catalog

N_TRADES = 20_000


def write_trades(data_dir: Path, trades: list[Trade], flush_rows: int = 5_000) -> ParquetWriter:
    w = ParquetWriter(data_dir / "raw", flush_rows=flush_rows)
    for tr in trades:
        due = w.append(tr)
        if due:
            w.flush(due)
    w.flush_all()
    return w


@pytest.fixture(scope="session")
def trades() -> list[Trade]:
    return list(synthetic_trades("BTCUSDT", N_TRADES, rate_per_sec=50, seed=1))


@pytest.fixture
def lake(tmp_path: Path, trades: list[Trade]) -> Path:
    """A data dir holding N_TRADES synthetic BTCUSDT trades across several files."""
    write_trades(tmp_path, trades)
    return tmp_path


@pytest.fixture
def catalog(lake: Path) -> Catalog:
    c = Catalog(lake)
    yield c
    c.close()
