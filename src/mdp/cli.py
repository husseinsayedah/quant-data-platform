"""Operational CLI: `mdp --help`."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path

import typer

from mdp.config import settings

app = typer.Typer(
    no_args_is_help=True,
    help="Market data platform: ingest, build bars, check quality, serve.",
)
log = logging.getLogger("mdp")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def _symbols(arg: str | None) -> list[str]:
    raw = arg or ",".join(settings.symbols)
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


@app.command()
def ingest(
    symbols: str = typer.Option(None, help="Comma separated, e.g. btcusdt,ethusdt"),
    duration: float = typer.Option(0.0, help="Seconds to run; 0 = until Ctrl-C"),
    data_dir: Path = typer.Option(settings.data_dir),
    metrics_port: int = typer.Option(settings.metrics_port, help="0 disables the metrics server"),
    verbose: bool = typer.Option(False, "-v"),
) -> None:
    """Stream live trades + top-of-book from Binance into partitioned Parquet."""
    from mdp.ingest.binance import BinanceClient, stream_url
    from mdp.ingest.writer import ParquetWriter
    from mdp.monitor.metrics import start_metrics_server

    _setup_logging(verbose)
    syms = _symbols(symbols)
    if metrics_port:
        start_metrics_server(metrics_port)

    async def main() -> None:
        queue: asyncio.Queue = asyncio.Queue(maxsize=200_000)
        client = BinanceClient(
            stream_url(settings.binance_ws_url, syms),
            queue,
            reconnect_base=settings.reconnect_base_seconds,
            reconnect_max=settings.reconnect_max_seconds,
        )
        writer = ParquetWriter(
            Path(data_dir) / "raw",
            flush_rows=settings.flush_rows,
            flush_seconds=settings.flush_seconds,
            compression=settings.parquet_compression,
        )
        reader_task = asyncio.create_task(client.run())
        writer_task = asyncio.create_task(writer.run(queue))
        try:
            if duration > 0:
                await asyncio.sleep(duration)
            else:
                await asyncio.gather(reader_task, writer_task)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            reader_task.cancel()
            await asyncio.gather(reader_task, return_exceptions=True)
            writer_task.cancel()
            await asyncio.gather(writer_task, return_exceptions=True)
            log.info("client stats: %s; rows written: %d", client.stats, writer.rows_written)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


@app.command()
def synth(
    symbol: str = "BTCUSDT",
    n: int = typer.Option(1_000_000, help="Number of trades"),
    rate: float = typer.Option(50.0, help="Trades per second"),
    seed: int = 0,
    data_dir: Path = typer.Option(settings.data_dir),
    verbose: bool = typer.Option(False, "-v"),
) -> None:
    """Generate a synthetic trade history (offline demo / benchmarks)."""
    from mdp.ingest.replay import synthetic_trades
    from mdp.ingest.writer import ParquetWriter

    _setup_logging(verbose)
    w = ParquetWriter(Path(data_dir) / "raw", flush_rows=200_000)
    for tr in synthetic_trades(symbol.upper(), n, rate_per_sec=rate, seed=seed):
        due = w.append(tr)
        if due:
            w.flush(due)
    w.flush_all()
    typer.echo(f"wrote {w.rows_written} synthetic trades for {symbol.upper()} to {data_dir}")


@app.command()
def bars(
    symbols: str = typer.Option(None, help="Comma separated; default: every symbol in the lake"),
    interval: str = typer.Option("1m"),
    dates: str = typer.Option(None, help="Comma separated YYYY-MM-DD; default: all"),
    data_dir: Path = typer.Option(settings.data_dir),
    verbose: bool = typer.Option(False, "-v"),
) -> None:
    """Build OHLCV bars from raw trades (idempotent per symbol/date)."""
    from mdp.batch.bars import run_bars_job
    from mdp.storage.catalog import Catalog

    _setup_logging(verbose)
    cat = Catalog(data_dir)
    syms = _symbols(symbols) if symbols else cat.symbols()
    for s in syms:
        res = run_bars_job(
            cat, s, interval, dates.split(",") if dates else None, settings.parquet_compression
        )
        typer.echo(
            f"{res.symbol} {res.interval}: {res.n_trades} trades -> {res.n_bars} bars "
            f"in {res.seconds:.2f}s"
        )


@app.command()
def quality(
    symbols: str = typer.Option(None),
    start: datetime = typer.Option(None),
    end: datetime = typer.Option(None),
    data_dir: Path = typer.Option(settings.data_dir),
    fail_on_issues: bool = typer.Option(False, help="Exit 1 if gaps/duplicates/non-monotonic"),
) -> None:
    """Run data quality checks and write JSON reports."""
    from mdp.batch.quality import run_quality, write_report
    from mdp.storage.catalog import Catalog

    _setup_logging(False)
    cat = Catalog(data_dir)
    syms = _symbols(symbols) if symbols else cat.symbols()
    bad = False
    for s in syms:
        rep = run_quality(cat, s, start, end, max_jump_pct=settings.max_price_jump_pct)
        write_report(rep, Path(data_dir) / "quality")
        typer.echo(rep.to_json())
        bad |= not rep.ok
    if fail_on_issues and bad:
        raise typer.Exit(1)


@app.command()
def serve(
    host: str = typer.Option(settings.api_host),
    port: int = typer.Option(settings.api_port),
    data_dir: Path = typer.Option(settings.data_dir),
    reload: bool = False,
) -> None:
    """Serve the research API (FastAPI / uvicorn)."""
    import os

    import uvicorn

    os.environ["MDP_DATA_DIR"] = str(data_dir)
    uvicorn.run("mdp.api.app:app", host=host, port=port, reload=reload)


@app.command()
def capture(
    symbols: str = typer.Option(None),
    seconds: float = 10.0,
    out: Path = Path("tests/fixtures/capture.jsonl"),
) -> None:
    """Record raw websocket frames to JSONL (fixtures and replay benchmarks)."""
    import json
    import time

    import websockets

    from mdp.ingest.binance import stream_url

    async def main() -> None:
        out.parent.mkdir(parents=True, exist_ok=True)
        n = 0
        deadline = time.monotonic() + seconds
        url = stream_url(settings.binance_ws_url, _symbols(symbols))
        async with websockets.connect(url) as ws, open(out, "w") as fh:
            while time.monotonic() < deadline:
                timeout = max(0.1, deadline - time.monotonic())
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                raw_s = raw if isinstance(raw, str) else raw.decode()
                fh.write(json.dumps({"ts_recv_us": time.time_ns() // 1000, "raw": raw_s}) + "\n")
                n += 1
        typer.echo(f"captured {n} frames to {out}")

    asyncio.run(main())


if __name__ == "__main__":
    app()
