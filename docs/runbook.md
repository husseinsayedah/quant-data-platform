# Runbook

How to operate `mdp`, and what to do when a dashboard panel turns red.

## Start / stop

```bash
docker compose up -d --build          # ingest + batch + api + prometheus + grafana
docker compose logs -f ingest         # follow the feed
docker compose down                   # stop; the `lake` volume keeps the data
```

Without Docker:

```bash
mdp ingest --symbols btcusdt,ethusdt  # Ctrl-C flushes and exits cleanly
mdp bars --interval 1m                # rebuild bars for every symbol/date
mdp quality                           # write data/quality/<SYMBOL>/latest.json
mdp serve                             # http://localhost:8000/docs
```

Ports: API 8000, ingest metrics 9100, Prometheus 9090, Grafana 3000 (admin / admin).

## Alerts and what they mean

| Signal | Likely cause | Action |
|---|---|---|
| `mdp_ws_connected == 0` for > 1 min | Exchange outage, network, or the stream URL changed | Check `docker compose logs ingest`; backoff is capped at 30 s so it will reconnect by itself. Expect a gap in the quality report afterwards |
| `increase(mdp_trade_gaps_total[30m]) > 0` | Messages lost during a reconnect, or the exchange skipped ids | Confirm with `mdp quality`; if it is an isolated gap after a reconnect, note it. Backfill from the REST `aggTrades` endpoint is a roadmap item |
| `mdp_trade_duplicates_total` rising | Exchange replayed messages after a reconnect | Expected in small numbers right after a reconnect. Nothing to do: they are dropped |
| `mdp_queue_depth` growing steadily | Disk slower than the feed, or the writer thread is stuck | Check `mdp_writer_flush_seconds` p99; if flushes are slow, look at disk IO. The queue is bounded at 200k events; past that the reader back-pressures and gaps will appear |
| Latency p50 negative | Host clock behind the exchange clock | Fix NTP on the host. Data is fine; only latency figures are wrong. The quality report says this in `warnings` |
| Latency p99 > 1 s | Network path to the exchange, or the host is CPU-starved | Compare with p50: if both moved, it is network; if only p99, it is local scheduling |
| `quality.ok == false` | Gaps, duplicates or non-monotonic timestamps in the stored data | Read the report, correlate with reconnect times in the logs |
| `max_silence_s` large on a liquid symbol | Feed stalled without disconnecting | Check the exchange status page; consider a watchdog that reconnects when no message arrives for N seconds |

## Recovery procedures

**Rebuild all bars from raw**
```bash
mdp bars --interval 1m
mdp bars --interval 5m
```
Jobs are idempotent: each `(symbol, date)` partition is replaced.

**A raw Parquet file is corrupt**
Delete the file (one hour of one symbol) and rerun `mdp bars` for that date. The gap will show in the quality report, which is the correct outcome: the lake must never pretend to have data it lost.

**Disk is full**
Stop ingest, move older `date=` directories to cold storage, restart. Nothing in the lake depends on anything outside the directory tree.

**Change the symbol list**
Edit `MDP_SYMBOLS` in `.env` (or the compose command) and restart `ingest`. New symbols appear in `/symbols` after the first flush.

## Checking health by hand

```bash
curl localhost:8000/health
curl "localhost:8000/symbols"
curl "localhost:8000/quality/latest?symbol=BTCUSDT"
curl "localhost:8000/bars?symbol=BTCUSDT&interval=1m&limit=5"
curl -s localhost:9100/metrics | grep -E "mdp_ws_connected|mdp_trade_gaps_total"
```
