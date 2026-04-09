"""Small CoinGecko helper used by the pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import time
from typing import Any, Iterator

from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.settings import PipelineSettings


logger = logging.getLogger(__name__)


class CoinGeckoClient:
    """Client for the CoinGecko endpoints used in this project."""

    def __init__(self, settings: PipelineSettings) -> None:
        self.settings = settings
        self.session = self._build_session()

    def _build_session(self) -> Session:
        """Create one requests session with retry support."""
        session = Session()

        retry_config = Retry(
            total=self.settings.max_retries,
            connect=self.settings.max_retries,
            read=self.settings.max_retries,
            backoff_factor=self.settings.backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry_config)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "crypto-batch-elt-pipeline",
            }
        )

        if self.settings.coingecko_api_key:
            session.headers[self.settings.coingecko_api_key_header] = self.settings.coingecko_api_key

        return session

    def _build_url(self, endpoint: str) -> str:
        """Build the full API URL for one endpoint."""
        clean_endpoint = endpoint.lstrip("/")
        return f"{self.settings.coingecko_api_url}/{clean_endpoint}"

    def _request(self, endpoint: str, *, params: dict[str, Any] | None = None) -> Response:
        """Send one GET request to CoinGecko."""
        url = self._build_url(endpoint)
        response = self.session.get(url, params=params, timeout=self.settings.request_timeout)

        if response.status_code == 429:
            wait_seconds = int(response.headers.get("Retry-After", "0"))

            if wait_seconds > 60:
                raise RuntimeError(
                    "CoinGecko API rate limit exceeded. Add COINGECKO_API_KEY or retry later."
                )

            if wait_seconds > 0:
                logger.warning(
                    "CoinGecko rate limit hit. Sleeping for %s seconds before retrying.",
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                response = self.session.get(url, params=params, timeout=self.settings.request_timeout)

        response.raise_for_status()
        return response

    def iter_coin_markets(self, *, snapshot_at: str, snapshot_date: str) -> Iterator[dict[str, Any]]:
        """Yield market rows page by page."""
        for page_number in range(1, self.settings.market_pages + 1):
            params = {
                "vs_currency": self.settings.vs_currency,
                "order": "market_cap_desc",
                "per_page": self.settings.market_page_size,
                "page": page_number,
                "sparkline": "false",
                "price_change_percentage": self.settings.price_change_windows,
            }
            response = self._request("/coins/markets", params=params)
            rows = response.json()

            if not isinstance(rows, list):
                raise TypeError(f"Expected list payload from /coins/markets, received {type(rows)!r}.")

            if not rows:
                return

            for asset in rows:
                yield self._shape_market_snapshot(
                    asset,
                    snapshot_at=snapshot_at,
                    snapshot_date=snapshot_date,
                )

    def get_global_metrics(self, *, snapshot_at: str, snapshot_date: str) -> dict[str, Any]:
        """Return the global market row for the current run."""
        payload = self._request("/global").json()

        if not isinstance(payload, dict) or "data" not in payload:
            raise TypeError("Expected payload shaped like {'data': {...}} from /global.")

        return self._shape_global_snapshot(
            payload["data"],
            snapshot_at=snapshot_at,
            snapshot_date=snapshot_date,
        )

    def _shape_market_snapshot(
        self,
        asset: dict[str, Any],
        *,
        snapshot_at: str,
        snapshot_date: str,
    ) -> dict[str, Any]:
        """Turn one asset from the API into one row."""
        asset_id = asset["id"]

        row: dict[str, Any] = {}
        row["market_snapshot_key"] = f"{snapshot_date}:{asset_id}"
        row["asset_id"] = asset_id
        row["symbol"] = asset.get("symbol")
        row["name"] = asset.get("name")
        row["image"] = asset.get("image")
        row["current_price"] = self._as_float(asset.get("current_price"))
        row["market_cap"] = self._as_float(asset.get("market_cap"))
        row["market_cap_rank"] = self._as_int(asset.get("market_cap_rank"))
        row["fully_diluted_valuation"] = self._as_float(asset.get("fully_diluted_valuation"))
        row["total_volume"] = self._as_float(asset.get("total_volume"))
        row["high_24h"] = self._as_float(asset.get("high_24h"))
        row["low_24h"] = self._as_float(asset.get("low_24h"))
        row["price_change_24h"] = self._as_float(asset.get("price_change_24h"))
        row["price_change_percentage_24h"] = self._as_float(asset.get("price_change_percentage_24h"))
        row["market_cap_change_24h"] = self._as_float(asset.get("market_cap_change_24h"))
        row["market_cap_change_percentage_24h"] = self._as_float(
            asset.get("market_cap_change_percentage_24h")
        )
        row["circulating_supply"] = self._as_float(asset.get("circulating_supply"))
        row["total_supply"] = self._as_float(asset.get("total_supply"))
        row["max_supply"] = self._as_float(asset.get("max_supply"))
        row["ath"] = self._as_float(asset.get("ath"))
        row["ath_change_percentage"] = self._as_float(asset.get("ath_change_percentage"))
        row["ath_date"] = asset.get("ath_date")
        row["atl"] = self._as_float(asset.get("atl"))
        row["atl_change_percentage"] = self._as_float(asset.get("atl_change_percentage"))
        row["atl_date"] = asset.get("atl_date")
        row["last_updated"] = asset.get("last_updated")
        row["price_change_percentage_24h_in_currency"] = self._as_float(
            asset.get("price_change_percentage_24h_in_currency")
        )
        row["price_change_percentage_7d_in_currency"] = self._as_float(
            asset.get("price_change_percentage_7d_in_currency")
        )
        row["price_change_percentage_30d_in_currency"] = self._as_float(
            asset.get("price_change_percentage_30d_in_currency")
        )
        row["snapshot_at"] = snapshot_at
        row["snapshot_date"] = snapshot_date
        row["vs_currency"] = self.settings.vs_currency

        return row

    def _shape_global_snapshot(
        self,
        payload: dict[str, Any],
        *,
        snapshot_at: str,
        snapshot_date: str,
    ) -> dict[str, Any]:
        """Turn the global API response into one row."""
        total_market_cap = payload.get("total_market_cap") or {}
        total_volume = payload.get("total_volume") or {}
        market_cap_percentage = payload.get("market_cap_percentage") or {}

        row: dict[str, Any] = {}
        row["global_snapshot_key"] = snapshot_date
        row["active_cryptocurrencies"] = self._as_int(payload.get("active_cryptocurrencies"))
        row["upcoming_icos"] = self._as_int(payload.get("upcoming_icos"))
        row["ongoing_icos"] = self._as_int(payload.get("ongoing_icos"))
        row["ended_icos"] = self._as_int(payload.get("ended_icos"))
        row["markets"] = self._as_int(payload.get("markets"))
        row["total_market_cap_usd"] = self._as_float(total_market_cap.get(self.settings.vs_currency))
        row["total_volume_usd"] = self._as_float(total_volume.get(self.settings.vs_currency))
        row["btc_market_cap_percentage"] = self._as_float(market_cap_percentage.get("btc"))
        row["eth_market_cap_percentage"] = self._as_float(market_cap_percentage.get("eth"))
        row["market_cap_change_percentage_24h_usd"] = self._as_float(
            payload.get("market_cap_change_percentage_24h_usd")
        )
        row["total_market_cap"] = total_market_cap
        row["total_volume"] = total_volume
        row["market_cap_percentage"] = market_cap_percentage
        row["snapshot_at"] = snapshot_at
        row["snapshot_date"] = snapshot_date
        row["vs_currency"] = self.settings.vs_currency

        return row

    @staticmethod
    def current_snapshot_markers() -> tuple[str, str]:
        """Return the timestamp values used for one pipeline run."""
        now = datetime.now(timezone.utc).replace(microsecond=0)
        snapshot_at = now.isoformat().replace("+00:00", "Z")
        snapshot_date = now.strftime("%Y-%m-%dT00:00:00Z")
        return snapshot_at, snapshot_date

    @staticmethod
    def _as_float(value: Any) -> float | None:
        """Convert a value to float when possible."""
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _as_int(value: Any) -> int | None:
        """Convert a value to int when possible."""
        if value is None:
            return None
        return int(value)
