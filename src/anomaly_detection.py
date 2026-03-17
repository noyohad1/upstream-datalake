"""
Central anomaly detection.

Each caller defines its own anomaly rules as a dict of:
    name → SQL query that returns rows with columns (vin, anomaly_type, detail, ...)

Results from all rules are unioned and written to a single CSV report.
"""
import logging
from pathlib import Path
from typing import Dict

import duckdb

logger = logging.getLogger(__name__)


def detect(out_path: Path, rules: Dict[str, str]) -> int:
    """
    Run all anomaly rules and write violating rows to `out_path/anomalies.csv`.

    Each rule query must return at least (vin, anomaly_type, detail).
    Returns the total number of anomalies found.
    """
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "anomalies.csv"

    conn = duckdb.connect()
    branches = "\nUNION ALL\n".join(
        f"-- {name}\n({query})" for name, query in rules.items()
    )

    conn.execute(f"""
        COPY (
            SELECT * FROM (
                {branches}
            )
            ORDER BY anomaly_type, vin
        ) TO '{out_file}' (FORMAT CSV, HEADER TRUE)
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{out_file}')").fetchone()[0]
    logger.info("Anomaly detection: %d row(s) flagged → %s", count, out_file)
    return count
