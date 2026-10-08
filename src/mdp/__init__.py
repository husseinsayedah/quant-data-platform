"""mdp: a market data platform for quantitative research.

Pipeline: exchange websocket -> parsed events -> buffered Parquet writer ->
DuckDB catalog -> batch bars & quality checks -> HTTP/Arrow API -> research code.
"""

__version__ = "0.1.0"
