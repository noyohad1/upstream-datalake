"""
SQL injection detection on the Bronze dataset.

Usage
-----
    from src.tools.sql_injection_detection import sql_injection_report

    sql_injection_report(
        columns=["vin", "manufacturer", "model"],
        patterns=[r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|SELECT)\b)"],
    )

How it works — two-stage filtering
------------------------------------
Regex matching is expensive at scale: evaluating a complex pattern against
millions of string values is CPU-intensive, especially when the vast majority
of values are clean.

This implementation avoids running the regex unconditionally by splitting the
scan into two stages:

  Stage 1 — cheap pre-filter
    Check each value for known suspicious characters (', ;, --, /*, ()
    using simple string CONTAINS — no regex engine, runs at native scan speed.
    On a clean dataset almost every row is rejected here.

  Stage 2 — full regex, only on suspects
    Only rows that passed Stage 1 are evaluated against the full pattern.
    DuckDB short-circuits the AND: if Stage 1 fails, Stage 2 never executes.

Additional optimisations applied:
  - All caller-supplied patterns are merged into a single regex with | so each
    value is scanned exactly once regardless of how many patterns are passed in.
  - Both stages run inside DuckDB, which processes columns in vectorized C++
    rather than row-by-row in Python.
  - The merged pattern is built once before the loop, not inside it.
"""
import logging
from pathlib import Path
from typing import List, Optional

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
    date: Optional[str] = None,
) -> int:
    """
    Scan Bronze parquet files for SQL injection patterns and write a CSV report.

    Args:
        columns:     Column names to inspect.
        patterns:    SQL injection regex patterns. Multiple patterns are merged
                     into a single regex (one pass per value, not N passes).
        bronze_path: Root of the Bronze parquet dataset.
        out_path:    Directory where the CSV report is written.
        date:        Restrict scan to a single date partition (e.g. "2026-03-16").
                     If omitted, all dates are scanned.

    Returns:
        Total number of violating rows found.
    """
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "sql_injection_report.csv"

    src = (
        str(bronze_path / f"date={date}" / "**" / "*.parquet")
        if date
        else str(bronze_path / "**" / "*.parquet")
    )

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
            FROM read_parquet('{src}')
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
