"""
Gold report: vin_last_state

One row per vehicle containing:
  - vin                      — vehicle id
  - last_reported_timestamp  — most recent timestamp seen in the data
  - front_left_door_state    — last known non-null value
  - wipers_state             — last known non-null value
"""
import logging
from pathlib import Path

import duckdb

from src.validate import run_checks

logger = logging.getLogger(__name__)

QUALITY_CHECKS = {
    "no duplicate vins": (
        "SELECT COUNT(*) FROM ("
        "  SELECT vin FROM read_csv_auto('{path}/*.csv') GROUP BY vin HAVING COUNT(*) > 1"
        ")"
    ),
    "no null last_reported_timestamp": (
        "SELECT COUNT(*) FROM read_csv_auto('{path}/*.csv') WHERE last_reported_timestamp IS NULL"
    ),
}


def run(silver_path: Path, gold_path: Path) -> None:
    out_path = gold_path / "vin_last_state"
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "vin_last_state.csv"

    logger.info("Building vin_last_state → %s", out_path)

    duckdb.execute(f"""
        COPY (
            SELECT DISTINCT
                vin,
                MAX(timestamp) OVER (PARTITION BY vin)  AS last_reported_timestamp,

                LAST_VALUE(front_left_door_state IGNORE NULLS) OVER (
                    PARTITION BY vin
                    ORDER BY timestamp
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS front_left_door_state,

                LAST_VALUE(wipers_state IGNORE NULLS) OVER (
                    PARTITION BY vin
                    ORDER BY timestamp
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS wipers_state

            FROM read_parquet('{silver_path}/**/*.parquet')
        ) TO '{out_file}' (FORMAT CSV, HEADER TRUE)
    """)

    run_checks(out_path, QUALITY_CHECKS)
