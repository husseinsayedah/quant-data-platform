"""Prometheus metrics shared by the ingestion pipeline and the API."""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger(__name__)

messages_total = Counter("mdp_messages_total", "Parsed websocket messages", ["stream", "symbol"])
unknown_messages_total = Counter(
    "mdp_unknown_messages_total", "Messages that did not parse to a known event"
)
gaps_total = Counter("mdp_trade_gaps_total", "Missing trade ids detected", ["symbol"])
duplicates_total = Counter(
    "mdp_trade_duplicates_total", "Duplicate / out-of-order trade ids dropped", ["symbol"]
)
reconnects_total = Counter("mdp_ws_reconnects_total", "Websocket reconnect attempts")
connected = Gauge("mdp_ws_connected", "1 when the websocket is connected")
queue_depth = Gauge("mdp_queue_depth", "Events waiting between reader and writer")
buffer_rows = Gauge("mdp_writer_buffer_rows", "Rows buffered per stream", ["stream"])
flushes_total = Counter("mdp_writer_flushes_total", "Parquet flushes", ["stream"])
rows_written_total = Counter("mdp_rows_written_total", "Rows written to Parquet", ["stream"])
flush_seconds = Histogram(
    "mdp_writer_flush_seconds",
    "Time to serialise and write one Parquet flush",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
ingest_latency_seconds = Histogram(
    "mdp_ingest_latency_seconds",
    "Exchange event time -> local receive time",
    ["symbol"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
api_requests_total = Counter("mdp_api_requests_total", "API requests", ["endpoint", "status"])
api_latency_seconds = Histogram("mdp_api_latency_seconds", "API request latency", ["endpoint"])


def start_metrics_server(port: int) -> None:
    start_http_server(port)
    log.info("metrics exposed on :%d/metrics", port)
