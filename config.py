from pathlib import Path

API_URL = "http://localhost:9900/upstream/vehicle_messages"
TOTAL_MESSAGES = 10_000
BATCH_SIZE = 1_000

BRONZE_PATH = Path("datalake/bronze")
SILVER_PATH = Path("datalake/silver")
