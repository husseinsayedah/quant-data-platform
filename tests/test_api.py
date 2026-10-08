from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from mdp.api.app import ARROW_MEDIA, create_app
from mdp.batch.bars import run_bars_job
from mdp.batch.quality import run_quality, write_report
from mdp.storage.catalog import Catalog


@pytest.fixture
def client(lake: Path):
    cat = Catalog(lake)
    run_bars_job(cat, "BTCUSDT", "1m")
    cat.close()
    with TestClient(create_app(lake)) as c:
        yield c


def test_health_and_symbols(client: TestClient):
    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/symbols").json() == {
        "symbols": [{"symbol": "BTCUSDT", "dates": ["2025-10-08"]}]
    }


def test_bars_json_and_arrow_agree(client: TestClient):
    js = client.get("/bars", params={"symbol": "BTCUSDT", "interval": "1m"}).json()
    assert js["count"] > 0
    assert js["rows"][0]["ts_open"].startswith("2025-10-08T00:00:00")
    r = client.get("/bars", params={"symbol": "BTCUSDT", "interval": "1m", "format": "arrow"})
    assert r.headers["content-type"].startswith(ARROW_MEDIA)
    table = pa.ipc.open_stream(r.content).read_all()
    assert table.num_rows == js["count"]
    assert table["close"].to_pylist() == [row["close"] for row in js["rows"]]


def test_arrow_is_smaller_than_json_at_scale(client: TestClient):
    p = {"symbol": "BTCUSDT", "limit": 5_000}
    js = client.get("/trades", params=p).content
    ar = client.get("/trades", params={**p, "format": "arrow"}).content
    assert pa.ipc.open_stream(ar).read_all().num_rows == 5_000
    assert len(ar) < len(js) / 3


def test_trades_limit_and_time_filter(client: TestClient):
    js = client.get("/trades", params={"symbol": "BTCUSDT", "limit": 5}).json()
    assert js["count"] == 5 and js["rows"][0]["trade_id"] == 1
    js = client.get(
        "/trades",
        params={
            "symbol": "BTCUSDT",
            "start": "2025-10-08T00:01:00Z",
            "end": "2025-10-08T00:01:10Z",
        },
    ).json()
    assert 0 < js["count"] < 5_000
    assert all("2025-10-08T00:01:0" in r["ts_event"] for r in js["rows"])


def test_bad_interval_is_400(client: TestClient):
    assert client.get("/bars", params={"symbol": "BTCUSDT", "interval": "nope"}).status_code == 400


def test_quality_endpoint(client: TestClient, lake: Path):
    assert client.get("/quality/latest", params={"symbol": "BTCUSDT"}).status_code == 404
    write_report(run_quality(Catalog(lake), "BTCUSDT"), lake / "quality")
    rep = client.get("/quality/latest", params={"symbol": "BTCUSDT"}).json()
    assert rep["ok"] is True and rep["n_trades"] == 20_000


def test_metrics_exposed(client: TestClient):
    body = client.get("/metrics").text
    assert "mdp_api_requests_total" in body
