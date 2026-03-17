# Upstream DataLake

A local three-layer DataLake built on vehicle telemetry messages fetched from a local API.

Data is fetched from the API into **pandas** DataFrames (via a straightforward HTTP ingestion step), while transformations and queries are executed in **DuckDB** — an in-process SQL engine optimized for analytical workloads that can efficiently query Parquet data with minimal memory overhead. This setup keeps ingestion simple and enables fast, expressive transformations.

## Layers

| Layer | What it does |
|-------|-------------|
| **Bronze** | Fetches 10K raw messages from the API, writes them as Parquet partitioned by date and hour |
| **Silver** | Cleans the data: strips manufacturer whitespace, drops null VINs, maps gear positions to integers |
| **Gold** | Produces CSV reports built on silver data. Each report lives in its own subdirectory under `datalake/gold/` |

## Running

Run the full pipeline (Bronze → Silver → Gold):
```bash
python main.py
```

Process only a specific date — reads just that day's partitions, skips everything else:
```bash
python main.py --date 2026-03-16

# Narrow it further to a single hour
python main.py --date 2026-03-16 --hour 17

# Skip layers you don't need to re-run
python main.py --skip-bronze --date 2026-03-16
```

Output lands in `datalake/` (gitignored). Reports are CSV files, one subdirectory per report under `datalake/gold/`.

## Tools

Reusable utilities in `src/tools/`, independent of any specific layer.

| Tool | What it does |
|------|-------------|
| `anomaly_detection` | Runs caller-defined SQL rules and flags violating rows to a CSV |
| `sql_injection_detection` | Scans columns for injection patterns using a two-stage filter (cheap string check → full regex) |

## Setup

```bash
# Start the API server
docker load -i upstream_interview.tar
docker run -p 9900:9900 upstream-interview
# Server is ready when you see: "Server Started"

# Install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

