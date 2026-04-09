"""Read project settings from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineSettings:
    """Settings for the pipeline."""

    coingecko_api_url: str
    coingecko_api_key: str | None
    coingecko_api_key_header: str
    vs_currency: str
    market_page_size: int
    market_pages: int
    price_change_windows: str
    request_timeout: int
    max_retries: int
    backoff_factor: float
    initial_snapshot_date: str
    dlt_pipeline_name: str
    dlt_dataset_name: str
    duckdb_path: Path
    log_level: str
    project_root: Path

    @classmethod
    def from_env(cls) -> "PipelineSettings":
        """Build settings from env vars."""
        project_root = Path(__file__).resolve().parents[1]
        page_size = int(os.getenv("MARKET_PAGE_SIZE", "100"))
        page_count = int(os.getenv("MARKET_PAGES", "2"))

        if page_size <= 0 or page_size > 250:
            raise ValueError("MARKET_PAGE_SIZE must be between 1 and 250.")
        if page_count <= 0:
            raise ValueError("MARKET_PAGES must be greater than 0.")

        duckdb_path = cls._resolve_path(
            project_root,
            os.getenv("DUCKDB_PATH", "data/warehouse/crypto_elt.duckdb"),
        )
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)

        api_key = os.getenv("COINGECKO_API_KEY", "").strip() or None
        api_url = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3").rstrip("/")
        api_key_header = os.getenv("COINGECKO_API_KEY_HEADER", "x-cg-demo-api-key").strip()
        vs_currency = os.getenv("VS_CURRENCY", "usd").strip().lower()

        return cls(
            coingecko_api_url=api_url,
            coingecko_api_key=api_key,
            coingecko_api_key_header=api_key_header,
            vs_currency=vs_currency,
            market_page_size=page_size,
            market_pages=page_count,
            price_change_windows=os.getenv("PRICE_CHANGE_WINDOWS", "24h,7d,30d").strip(),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
            max_retries=int(os.getenv("MAX_RETRIES", "5")),
            backoff_factor=float(os.getenv("BACKOFF_FACTOR", "1.0")),
            initial_snapshot_date=os.getenv("INITIAL_SNAPSHOT_DATE", "2026-01-01T00:00:00Z"),
            dlt_pipeline_name=os.getenv("DLT_PIPELINE_NAME", "crypto_batch_elt"),
            dlt_dataset_name=os.getenv("DLT_DATASET_NAME", "crypto_raw"),
            duckdb_path=duckdb_path,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            project_root=project_root,
        )

    @staticmethod
    def _resolve_path(project_root: Path, raw_path: str) -> Path:
        """Return an absolute path."""
        path = Path(raw_path)
        return path if path.is_absolute() else (project_root / path).resolve()
