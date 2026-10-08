"""Data quality checks over the raw trades.

Checks (per symbol, over a date range):
  * gaps            trade_id discontinuities and the total number of missing ids
  * duplicates      repeated trade ids (should be 0; the writer drops them online)
  * non_monotonic   exchange timestamps going backwards in trade_id order
  * price_outliers  single-trade returns above a threshold (bad ticks / feed issues)
  * latency         exchange->local latency percentiles (ms)
  * max_silence_s   longest interval with no trades (feed stalls / downtime)
Reports are written as JSON next to the data so monitoring can scrape them.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from mdp.schemas import Stream
from mdp.storage.catalog import Catalog, to_utc

log = logging.getLogger(__name__)


@dataclass
class QualityReport:
    symbol: str
    start: str | None
    end: str | None
    n_trades: int
    first_ts: str | None
    last_ts: str | None
    n_gaps: int
    missing_ids: int
    n_duplicates: int
    n_non_monotonic: int
    n_price_outliers: int
    latency_p50_ms: float | None
    latency_p99_ms: float | None
    max_silence_s: float | None
    generated_at: str

    @property
    def ok(self) -> bool:
        return self.n_gaps == 0 and self.n_duplicates == 0 and self.n_non_monotonic == 0

    @property
    def warnings(self) -> list[str]:
        """Soft signals that do not fail the check but deserve a look."""
        out = []
        if self.latency_p50_ms is not None and self.latency_p50_ms < 0:
            out.append(
                f"median latency is negative ({self.latency_p50_ms} ms): local clock is behind "
                "the exchange clock; latency figures are unreliable until NTP is fixed"
            )
        if self.n_price_outliers:
            out.append(f"{self.n_price_outliers} single-trade price jumps above threshold")
        if self.max_silence_s is not None and self.max_silence_s > 60:
            out.append(
                f"feed silent for {self.max_silence_s:.0f}s at some point (stall or downtime?)"
            )
        return out

    def to_json(self) -> str:
        d = asdict(self)
        d["ok"] = self.ok
        d["warnings"] = self.warnings
        return json.dumps(d, indent=2)


def quality_sql(scan: str, where: str, max_jump_pct: float) -> str:
    return f"""
        WITH t AS (
            SELECT trade_id, price, ts_event, ts_recv,
                   lag(trade_id) OVER w AS prev_id,
                   lag(price)    OVER w AS prev_px,
                   lag(ts_event) OVER w AS prev_ts
            FROM {scan}
            WHERE {where}
            WINDOW w AS (ORDER BY trade_id, ts_event)
        )
        SELECT
            count(*)                                                        AS n_trades,
            min(ts_event)                                                   AS first_ts,
            max(ts_event)                                                   AS last_ts,
            count(*) FILTER (WHERE trade_id - prev_id > 1)                  AS n_gaps,
            coalesce(sum(CASE WHEN trade_id - prev_id > 1
                              THEN trade_id - prev_id - 1 ELSE 0 END), 0)   AS missing_ids,
            count(*) FILTER (WHERE trade_id = prev_id)                      AS n_duplicates,
            count(*) FILTER (WHERE ts_event < prev_ts)                      AS n_non_monotonic,
            count(*) FILTER (WHERE abs(price / prev_px - 1) * 100 > {max_jump_pct})
                                                                            AS n_price_outliers,
            quantile_cont((epoch_us(ts_recv) - epoch_us(ts_event)) / 1000.0, 0.5)
                                                                            AS latency_p50_ms,
            quantile_cont((epoch_us(ts_recv) - epoch_us(ts_event)) / 1000.0, 0.99)
                                                                            AS latency_p99_ms,
            max(epoch_us(ts_event) - epoch_us(prev_ts)) / 1e6               AS max_silence_s
        FROM t
    """


def run_quality(
    catalog: Catalog,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    max_jump_pct: float = 2.0,
) -> QualityReport:
    root = catalog.raw_dir(Stream.TRADES)
    now = datetime.now(tz=UTC).isoformat()
    if not catalog._has_files(root, symbol):
        return QualityReport(
            symbol.upper(), None, None, 0, None, None, 0, 0, 0, 0, 0, None, None, None, now
        )
    where, params = catalog._range("ts_event", start, end)
    row = catalog.con.execute(
        quality_sql(catalog._scan(root, symbol), where, max_jump_pct), params
    ).fetchone()
    (n, first_ts, last_ts, n_gaps, missing, dups, nonmono, outliers, p50, p99, silence) = row
    return QualityReport(
        symbol=symbol.upper(),
        start=to_utc(start).isoformat() if start else None,
        end=to_utc(end).isoformat() if end else None,
        n_trades=int(n),
        first_ts=first_ts.isoformat() if first_ts else None,
        last_ts=last_ts.isoformat() if last_ts else None,
        n_gaps=int(n_gaps),
        missing_ids=int(missing),
        n_duplicates=int(dups),
        n_non_monotonic=int(nonmono),
        n_price_outliers=int(outliers),
        latency_p50_ms=round(float(p50), 3) if p50 is not None else None,
        latency_p99_ms=round(float(p99), 3) if p99 is not None else None,
        max_silence_s=round(float(silence), 3) if silence is not None else None,
        generated_at=now,
    )


def write_report(report: QualityReport, quality_dir: Path) -> Path:
    out_dir = quality_dir / report.symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = report.generated_at.replace(":", "").replace("+00:00", "Z")
    path = out_dir / f"{stamp}.json"
    path.write_text(report.to_json())
    (out_dir / "latest.json").write_text(report.to_json())
    return path


def load_latest(quality_dir: Path, symbol: str) -> dict | None:
    p = quality_dir / symbol.upper() / "latest.json"
    return json.loads(p.read_text()) if p.exists() else None
