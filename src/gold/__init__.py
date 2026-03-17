"""
Gold layer: runs all reports against the silver data.
"""
import logging
from pathlib import Path

import duckdb

from config import GOLD_PATH, SILVER_PATH
from src.gold import vin_last_state, top_fastest
from src.anomaly_detection import detect

logger = logging.getLogger(__name__)


def run(silver_path: Path = SILVER_PATH, gold_path: Path = GOLD_PATH) -> None:
    logger.info("Starting gold layer")

    vin_last_state.run(silver_path, gold_path)
    top_fastest.run(silver_path, gold_path)
    _run_anomaly_detection(silver_path, gold_path)

    logger.info("Gold layer done")


def _run_anomaly_detection(silver_path: Path, gold_path: Path) -> None:
    conn = duckdb.connect()

    # Velocity threshold is data-driven: p95 * 2, so it adapts to the dataset.
    velocity_threshold = conn.execute(f"""
        SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY velocity) * 2
        FROM read_parquet('{silver_path}/**/*.parquet')
    """).fetchone()[0]

    logger.info("Velocity anomaly threshold (p95 × 2): %.1f", velocity_threshold)

    # Each rule returns rows with (vin, manufacturer, model, year, anomaly_type, detail).
    rules = {
        "velocity_outlier": f"""
            SELECT
                vin,
                ANY_VALUE(manufacturer)  AS manufacturer,
                ANY_VALUE(model)         AS model,
                ANY_VALUE(year)          AS year,
                'velocity_outlier'       AS anomaly_type,
                'max velocity ' || CAST(MAX(velocity) AS VARCHAR)
                    || ' exceeds p95×2 threshold of '
                    || CAST(ROUND({velocity_threshold}, 1) AS VARCHAR) AS detail
            FROM read_parquet('{silver_path}/**/*.parquet')
            GROUP BY vin
            HAVING MAX(velocity) > {velocity_threshold}
        """,
        "implausible_year": f"""
            SELECT
                vin,
                ANY_VALUE(manufacturer)  AS manufacturer,
                ANY_VALUE(model)         AS model,
                ANY_VALUE(year)          AS year,
                'implausible_year'       AS anomaly_type,
                'year ' || CAST(ANY_VALUE(year) AS VARCHAR)
                    || ' predates public internet (1991)' AS detail
            FROM read_parquet('{silver_path}/**/*.parquet')
            GROUP BY vin
            HAVING ANY_VALUE(year) < 1991
        """,
    }

    detect(out_path=gold_path / "anomaly_detection", rules=rules)
