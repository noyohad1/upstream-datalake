"""
Pipeline entry point.

Runs the full Bronze → Silver → Gold pipeline in sequence.

Usage
-----
Run the full pipeline:
    python main.py

Run for a specific date only (partition-pruned — faster, processes only that day):
    python main.py --date 2026-03-16

Run for a specific date and hour:
    python main.py --date 2026-03-16 --hour 17

Skip layers you don't need:
    python main.py --skip-bronze
    python main.py --skip-bronze --skip-silver
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upstream DataLake pipeline")
    parser.add_argument("--date", help="Process only this date partition (YYYY-MM-DD)")
    parser.add_argument("--hour", type=int, help="Process only this hour (0-23); requires --date")
    parser.add_argument("--skip-bronze", action="store_true", help="Skip the bronze ingestion step")
    parser.add_argument("--skip-silver", action="store_true", help="Skip the silver transformation step")
    parser.add_argument("--skip-gold",   action="store_true", help="Skip the gold report generation step")
    args = parser.parse_args()

    if args.hour is not None and not args.date:
        parser.error("--hour requires --date")

    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

    if not args.skip_bronze:
        logger.info("=== BRONZE ===")
        from src.bronze import run as bronze_run
        bronze_run()

    if not args.skip_silver:
        logger.info("=== SILVER ===")
        from src.silver import run as silver_run
        silver_run(date=args.date, hour=args.hour)

    if not args.skip_gold:
        logger.info("=== GOLD ===")
        from src.gold import run as gold_run
        gold_run(date=args.date)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
