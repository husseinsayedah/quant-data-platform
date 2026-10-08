"""Runtime configuration. Every value can be overridden with an MDP_* env var."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MDP_", env_file=".env", extra="ignore")

    data_dir: Path = Path("data")
    binance_ws_url: str = "wss://stream.binance.com:9443/stream"
    symbols: list[str] = ["btcusdt", "ethusdt"]

    # Writer: flush when either threshold is reached.
    flush_rows: int = 20_000
    flush_seconds: float = 5.0
    parquet_compression: str = "zstd"

    # Reconnect policy for the websocket client.
    reconnect_base_seconds: float = 0.5
    reconnect_max_seconds: float = 30.0

    # Quality thresholds.
    max_price_jump_pct: float = 2.0  # single-trade move considered an outlier

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    metrics_port: int = 9100

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def bars_dir(self) -> Path:
        return self.data_dir / "bars"

    @property
    def quality_dir(self) -> Path:
        return self.data_dir / "quality"


settings = Settings()
