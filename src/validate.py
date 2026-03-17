"""
Central data quality validation.

Each layer defines its own checks (a dict of name → SQL query).
Queries must return a single integer — the number of violating rows.
A count of 0 means the check passed.
"""
import logging
from pathlib import Path
from typing import Dict

import duckdb

logger = logging.getLogger(__name__)


def run_checks(path: Path, checks: Dict[str, str]) -> None:
    """
    Run all checks against the parquet files at `path`.
    Raises ValueError if any check fails.
    """
    conn = duckdb.connect()
    failed = []

    for name, query in checks.items():
        count = conn.execute(query.format(path=path)).fetchone()[0]
        if count == 0:
            logger.info("  ✓ PASS  —  %s", name)
        else:
            logger.error("  ✗ FAIL  —  %s (%d violating rows)", name, count)
            failed.append(name)

    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")
