"""Run the dlt load for this project."""

from __future__ import annotations

import logging

import dlt
from dotenv import load_dotenv

from pipelines.crypto_client import CoinGeckoClient
from pipelines.settings import PipelineSettings


logger = logging.getLogger(__name__)


@dlt.source(name="crypto_source")
def crypto_source(settings: PipelineSettings):
    """Build the dlt source used by the pipeline."""
    client = CoinGeckoClient(settings)
    snapshot_at, snapshot_date = client.current_snapshot_markers()
    initial_snapshot_date = settings.initial_snapshot_date

    @dlt.resource(
        name="coin_markets",
        table_name="raw_coin_markets",
        primary_key="market_snapshot_key",
        write_disposition="merge",
    )
    def coin_markets(
        _snapshot_cursor=dlt.sources.incremental(
            "snapshot_date",
            initial_value=initial_snapshot_date,
        )
    ):
        rows = client.iter_coin_markets(snapshot_at=snapshot_at, snapshot_date=snapshot_date)
        yield from rows

    @dlt.resource(
        name="global_metrics",
        table_name="raw_global_metrics",
        primary_key="global_snapshot_key",
        write_disposition="merge",
    )
    def global_metrics(
        _snapshot_cursor=dlt.sources.incremental(
            "snapshot_date",
            initial_value=initial_snapshot_date,
        )
    ):
        row = client.get_global_metrics(snapshot_at=snapshot_at, snapshot_date=snapshot_date)
        yield row

    return coin_markets, global_metrics


def configure_logging(log_level: str) -> None:
    """Set up logging for the pipeline run."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def run() -> None:
    """Run the pipeline end to end."""
    load_dotenv()
    settings = PipelineSettings.from_env()
    configure_logging(settings.log_level)

    logger.info("Running dlt pipeline '%s'.", settings.dlt_pipeline_name)

    destination = dlt.destinations.duckdb(str(settings.duckdb_path))
    pipeline = dlt.pipeline(
        pipeline_name=settings.dlt_pipeline_name,
        destination=destination,
        dataset_name=settings.dlt_dataset_name,
    )

    source = crypto_source(settings)
    result = pipeline.run(source)

    logger.info("Pipeline finished.")
    logger.info("%s", result)


if __name__ == "__main__":
    run()
