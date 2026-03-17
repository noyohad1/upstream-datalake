"""
Gold report: top_fastest

Top 10 fastest vehicles per hour, ranked by highest reported velocity.
Columns: vin, date_hour, top_velocity
"""
import logging
from pathlib import Path
from typing import Optional

import duckdb

from src.validate import run_checks

logger = logging.getLogger(__name__)

QUALITY_CHECKS = {
    "at most 10 rows per date_hour": (
        "SELECT COUNT(*) FROM ("
        "  SELECT date_hour, COUNT(*) AS cnt"
        "  FROM read_csv_auto('{path}/*.csv')"
        "  GROUP BY date_hour HAVING cnt > 10"
        ")"
    ),
    "no null top_velocity": (
        "SELECT COUNT(*) FROM read_csv_auto('{path}/*.csv') WHERE top_velocity IS NULL"
    ),
}


def run(silver_path: Path, gold_path: Path, date: Optional[str] = None) -> None:
    """
    Args:
        date: Restrict to a single date partition (e.g. "2026-03-16").
              If omitted, all dates are processed.
    """
    out_path = gold_path / "top_fastest"
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "top_fastest.csv"

    src = (
        str(silver_path / f"date={date}" / "**" / "*.parquet")
        if date
        else str(silver_path / "**" / "*.parquet")
    )
    logger.info("Building top_fastest → %s (source: %s)", out_path, src)

    duckdb.execute(f"""
        COPY (
            WITH ranked AS (
                SELECT
                    vin,
                    date || 'T' || LPAD(CAST(hour AS VARCHAR), 2, '0') || ':00' AS date_hour,
                    MAX(velocity) AS top_velocity,
                    ROW_NUMBER() OVER (
                        PARTITION BY date, hour
                        ORDER BY MAX(velocity) DESC
                    ) AS rank
                FROM read_parquet('{src}')
                GROUP BY vin, date, hour
            )
            SELECT vin, date_hour, top_velocity
            FROM ranked
            WHERE rank <= 10
            ORDER BY date_hour, top_velocity DESC
        ) TO '{out_file}' (FORMAT CSV, HEADER TRUE)
    """)

    run_checks(out_path, QUALITY_CHECKS)
