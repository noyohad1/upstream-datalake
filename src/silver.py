"""
Silver layer: clean and standardize the bronze data.

Transformations:
  1. Strip leading/trailing whitespace from manufacturer.
  2. Filter out rows where vin is null or empty.
  3. Standardize gearPosition to integer:
       REVERSE → -1, NEUTRAL → 0, numeric strings → integer, anything else → NULL
"""
import logging
from pathlib import Path
from typing import Optional

import duckdb

from config import BRONZE_PATH, SILVER_PATH
from src.validate import run_checks

logger = logging.getLogger(__name__)

# Checks run against the silver output after writing.
# Each query returns the count of violating rows — 0 means pass.
QUALITY_CHECKS = {
    "no null or empty vins": (
        "SELECT COUNT(*) FROM '{path}/**/*.parquet' WHERE vin IS NULL OR TRIM(vin) = ''"
    ),
    "no trailing spaces in manufacturer": (
        "SELECT COUNT(*) FROM '{path}/**/*.parquet' WHERE manufacturer != TRIM(manufacturer)"
    ),
    "gear_position is integer": (
        "SELECT COUNT(*) FROM '{path}/**/*.parquet' "
        "WHERE gear_position IS NOT NULL AND TRY_CAST(gear_position AS INTEGER) IS NULL"
    ),
}


def _source_glob(bronze_path: Path, date: Optional[str], hour: Optional[int]) -> str:
    """Build a partition-pruned glob path based on the provided filters."""
    if date and hour is not None:
        return str(bronze_path / f"date={date}" / f"hour={hour}" / "*.parquet")
    if date:
        return str(bronze_path / f"date={date}" / "**" / "*.parquet")
    return str(bronze_path / "**" / "*.parquet")


def run(
    bronze_path: Path = BRONZE_PATH,
    silver_path: Path = SILVER_PATH,
    date: Optional[str] = None,
    hour: Optional[int] = None,
) -> None:
    """
    Transform Bronze → Silver.

    Args:
        date: Restrict to a single date partition (e.g. "2026-03-16").
              If omitted, all dates are processed.
        hour: Restrict to a specific hour (0–23). Only used when date is also set.
    """
    logger.info("Starting silver transformation")

    conn = duckdb.connect()

    silver_path.mkdir(parents=True, exist_ok=True)
    src = _source_glob(bronze_path, date, hour)
    logger.info("Reading from: %s", src)

    # Map string gear names to integers: REVERSE → -1, NEUTRAL → 0.
    # LOWER + TRIM applied first to catch case/whitespace variants.
    conn.execute(f"""
        COPY (
            SELECT
                vin,
                TRIM(manufacturer)          AS manufacturer,
                year,
                model,
                latitude,
                longitude,
                timestamp,
                velocity,
                frontLeftDoorState          AS front_left_door_state,
                wipersState                 AS wipers_state,
                CASE LOWER(TRIM(gearPosition))
                    WHEN 'reverse' THEN -1
                    WHEN 'neutral' THEN  0
                    ELSE TRY_CAST(gearPosition AS INTEGER)
                END                         AS gear_position,
                driverSeatbeltState         AS driver_seatbelt_state,
                date,
                hour
            FROM '{src}'
            WHERE vin IS NOT NULL
              AND TRIM(vin) != ''
        ) TO '{silver_path}' (
            FORMAT PARQUET,
            PARTITION_BY (date, hour),
            OVERWRITE_OR_IGNORE TRUE
        )
    """)

    logger.info("Silver written to %s — running quality checks", silver_path)
    run_checks(silver_path, QUALITY_CHECKS)
    logger.info("Silver done")
