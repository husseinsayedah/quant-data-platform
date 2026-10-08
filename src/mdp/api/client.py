"""Thin Python client: the thing a researcher imports.

from mdp.api.client import MDPClient
bars = MDPClient("http://localhost:8000").bars("BTCUSDT", "1m", start, end)  # DataFrame
"""

from __future__ import annotations

from datetime import datetime

import httpx
import pandas as pd
import pyarrow as pa


class MDPClient:
    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout)

    @staticmethod
    def _params(**kw) -> dict:
        out = {}
        for k, v in kw.items():
            if v is None:
                continue
            out[k] = v.isoformat() if isinstance(v, datetime) else v
        return out

    def _arrow(self, path: str, **params) -> pa.Table:
        r = self._http.get(path, params=self._params(format="arrow", **params))
        r.raise_for_status()
        return pa.ipc.open_stream(r.content).read_all()

    def symbols(self) -> list[dict]:
        r = self._http.get("/symbols")
        r.raise_for_status()
        return r.json()["symbols"]

    def trades(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100_000,
    ) -> pd.DataFrame:
        return self._arrow("/trades", symbol=symbol, start=start, end=end, limit=limit).to_pandas()

    def bars(
        self,
        symbol: str,
        interval: str = "1m",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        df = self._arrow(
            "/bars", symbol=symbol, interval=interval, start=start, end=end
        ).to_pandas()
        return df.set_index("ts_open") if "ts_open" in df else df

    def quality(self, symbol: str) -> dict:
        r = self._http.get("/quality/latest", params={"symbol": symbol})
        r.raise_for_status()
        return r.json()

    def close(self) -> None:
        self._http.close()
