"""Research-facing HTTP API.

Two response formats on every data endpoint:
  * format=json   convenient for curl and browsers
  * format=arrow  Arrow IPC stream; the Python client turns it into a DataFrame
                  with no parsing and far less bandwidth than JSON.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa
from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from mdp import __version__
from mdp.batch.quality import load_latest
from mdp.config import settings
from mdp.monitor import metrics
from mdp.storage.catalog import Catalog, interval_to_us

ARROW_MEDIA = "application/vnd.apache.arrow.stream"


def table_to_arrow_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return sink.getvalue().to_pybytes()


def table_to_json(table: pa.Table) -> dict:
    cols: dict[str, list] = {}
    for name in table.column_names:
        col = table[name]
        if pa.types.is_timestamp(col.type):
            cols[name] = [v.isoformat() if v is not None else None for v in col.to_pylist()]
        elif pa.types.is_date(col.type):
            cols[name] = [str(v) for v in col.to_pylist()]
        else:
            cols[name] = col.to_pylist()
    rows = [dict(zip(cols, vals, strict=True)) for vals in zip(*cols.values(), strict=True)]
    return {"count": len(rows), "rows": rows}


def table_response(table: pa.Table, fmt: str):
    if fmt == "arrow":
        return Response(content=table_to_arrow_bytes(table), media_type=ARROW_MEDIA)
    return table_to_json(table)


def create_app(data_dir: Path | str | None = None) -> FastAPI:
    data_dir = Path(data_dir or settings.data_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.catalog = Catalog(data_dir)
        yield
        app.state.catalog.close()

    app = FastAPI(title="mdp - market data platform", version=__version__, lifespan=lifespan)

    @app.middleware("http")
    async def observe(request: Request, call_next: Callable):
        t0 = time.perf_counter()
        response = await call_next(request)
        endpoint = request.url.path
        metrics.api_requests_total.labels(endpoint, str(response.status_code)).inc()
        metrics.api_latency_seconds.labels(endpoint).observe(time.perf_counter() - t0)
        return response

    def catalog() -> Catalog:
        return app.state.catalog

    @app.get("/health")
    def health():
        return {"status": "ok", "version": __version__, "data_dir": str(data_dir)}

    @app.get("/symbols")
    def symbols():
        c = catalog()
        return {"symbols": [{"symbol": s, "dates": c.dates(s)} for s in c.symbols()]}

    @app.get("/trades")
    def trades(
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = Query(10_000, le=1_000_000),
        format: Literal["json", "arrow"] = "json",
    ):
        return table_response(catalog().trades(symbol, start, end, limit), format)

    @app.get("/book_ticker")
    def book_ticker(
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = Query(10_000, le=1_000_000),
        format: Literal["json", "arrow"] = "json",
    ):
        return table_response(catalog().book_ticker(symbol, start, end, limit), format)

    @app.get("/bars")
    def bars(
        symbol: str,
        interval: str = "1m",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = Query(None, le=1_000_000),
        format: Literal["json", "arrow"] = "json",
    ):
        try:
            interval_to_us(interval)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return table_response(catalog().bars(symbol, interval, start, end, limit), format)

    @app.get("/quality/latest")
    def quality_latest(symbol: str):
        rep = load_latest(data_dir / "quality", symbol)
        if rep is None:
            raise HTTPException(404, f"no quality report for {symbol}")
        return rep

    @app.get("/metrics")
    def prom_metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
