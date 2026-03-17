"""
SQL injection detection on the Bronze dataset.

Optimization strategy — two-stage filtering:
  Stage 1: cheap CONTAINS checks for known suspicious characters (;, --, etc.)
           — no regex engine involved, runs at native string-scan speed.
  Stage 2: full regex match, executed ONLY on rows that survived Stage 1.

On clean data (the realistic majority at scale) almost no rows reach Stage 2,
making the overall scan significantly cheaper than applying regex unconditionally.
Both stages run inside DuckDB, which processes columns in vectorized C++
rather than row-by-row in Python.
"""
import logging
from pathlib import Path
from typing import List

import duckdb

from config import BRONZE_PATH, REPORTS_PATH

logger = logging.getLogger(__name__)

# Characters that are cheap to scan for and are present in virtually every
# SQL injection payload. Used as the Stage-1 pre-filter.
_SUSPICIOUS_CHARS = ["'", ";", "--", "/*", "("]


def sql_injection_report(
    columns: List[str],
    patterns: List[str],
    bronze_path: Path = BRONZE_PATH,
    out_path: Path = REPORTS_PATH / "sql_injection",
) -> int:
    """
    Scan Bronze parquet files for SQL injection patterns and write a CSV report.

    Args:
        columns:     Column names to inspect.
        patterns:    SQL injection regex patterns. Multiple patterns are merged
                     into a single regex (one pass per value, not N passes).
        bronze_path: Root of the Bronze parquet dataset.
        out_path:    Directory where the CSV report is written.

    Returns:
        Total number of violating rows found.
    """
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "sql_injection_report.csv"

    conn = duckdb.connect()

    # Merge all patterns into one so each value is scanned exactly once.
    # Escape single quotes for embedding in a SQL string literal.
    combined_pattern = "|".join(f"(?:{p})" for p in patterns).replace("'", "''")

    per_column_queries = []
    for col in columns:
        # Stage 1: at least one suspicious character must be present.
        # Single quotes are escaped as '' for SQL string literals.
        pre_filter = " OR ".join(
            f"CONTAINS(CAST({col} AS VARCHAR), '{c.replace(chr(39), chr(39)*2)}')"
            for c in _SUSPICIOUS_CHARS
        )
        # Stage 2: full regex, only evaluated when Stage 1 passes.
        per_column_queries.append(f"""
            SELECT *, '{col}' AS violating_column
            FROM read_parquet('{bronze_path}/**/*.parquet')
            WHERE ({pre_filter})
              AND regexp_matches(CAST({col} AS VARCHAR), '{combined_pattern}')
        """)

    full_query = "\nUNION ALL\n".join(per_column_queries)

    conn.execute(f"""
        COPY (
            {full_query}
            ORDER BY date, hour, violating_column
        ) TO '{out_file}' (FORMAT CSV, HEADER TRUE)
    """)

    count = conn.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{out_file}')"
    ).fetchone()[0]

    logger.info("SQL injection scan complete: %d violation(s) → %s", count, out_file)
    return count
