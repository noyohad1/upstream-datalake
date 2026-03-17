# Upstream DataLake

A local three-layer DataLake built on vehicle telemetry messages fetched from a local API.

## Layers

| Layer | What it does |
|-------|-------------|
| **Bronze** | Fetches 10K raw messages from the API, writes them as Parquet partitioned by date and hour |
| **Silver** | Cleans the data: strips manufacturer whitespace, drops null VINs, maps gear positions to integers |
| **Gold** | Produces CSV reports built on silver data. Each report lives in its own subdirectory under `datalake/gold/` |

## Running

Each layer can be run independently from the project root:

```bash
# Bronze — fetch from API and write raw parquet
python -c "from src.bronze import run; run()"

# Silver — clean and standardize
python -c "from src.silver import run; run()"

# Gold — generate reports
python -c "from src.gold import run; run()"
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

