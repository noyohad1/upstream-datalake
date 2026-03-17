from pathlib import Path

_ROOT = Path(__file__).parent

API_URL = "http://localhost:9900/upstream/vehicle_messages"
TOTAL_MESSAGES = 10_000
BATCH_SIZE = 1_000

BRONZE_PATH  = _ROOT / "datalake" / "bronze"
SILVER_PATH  = _ROOT / "datalake" / "silver"
GOLD_PATH    = _ROOT / "datalake" / "gold"
REPORTS_PATH = _ROOT / "reports"
