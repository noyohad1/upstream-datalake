"""
Bronze layer: fetch raw vehicle messages from the API and write them as
Parquet files partitioned by date and hour.
"""
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from config import API_URL, BATCH_SIZE, BRONZE_PATH, TOTAL_MESSAGES

logger = logging.getLogger(__name__)


def fetch_messages(total: int = TOTAL_MESSAGES, batch_size: int = BATCH_SIZE) -> pd.DataFrame:
    """Fetch `total` vehicle messages from the API in batches."""
    batches = []
    fetched = 0

    while fetched < total:
        amount = min(batch_size, total - fetched)
        response = requests.get(API_URL, params={"amount": amount}, timeout=30)
        response.raise_for_status()
        batches.append(pd.DataFrame(response.json()))
        fetched += amount
        logger.info("Fetched %d / %d messages", fetched, total)

    return pd.concat(batches, ignore_index=True)


def add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Derive `date` and `hour` partition columns from the Unix-ms timestamp."""
    ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.copy()
    df["date"] = ts.dt.strftime("%Y-%m-%d")
    df["hour"] = ts.dt.hour
    return df


def write_bronze(df: pd.DataFrame, path: Path = BRONZE_PATH) -> None:
    """Write the DataFrame as Parquet partitioned by date and hour."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(path),
        partition_cols=["date", "hour"],
        existing_data_behavior="overwrite_or_ignore",
    )
    logger.info("Bronze written to %s", path)


def run(path: Path = BRONZE_PATH) -> None:
    logger.info("Starting bronze ingestion (%d messages)", TOTAL_MESSAGES)
    df = fetch_messages()
    df = add_partition_columns(df)
    write_bronze(df, path)
    logger.info(
        "Done — %d rows across %d partition(s)",
        len(df),
        df.groupby(["date", "hour"]).ngroups,
    )
