"""USDA NASS Data Connector - fetches agricultural statistics.

Data sources:
- QuickStats API: https://quickstats.nass.usda.gov/api

Datasets:
- Crops (production, yields, acreage)
- Livestock (counts, production)
- Economics (farm income, land values)
- Environmental (irrigation, fertilizer use)
"""

import argparse
import os

os.environ["RUN_ID"] = os.getenv("RUN_ID", "local-run")

from subsets_utils import validate_environment
from ingest import quickstats as ingest_quickstats


def main():
    parser = argparse.ArgumentParser(description="USDA NASS Data Connector")
    parser.add_argument(
        "--ingest-only", action="store_true", help="Only fetch data from API"
    )
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Only transform existing raw data",
    )
    args = parser.parse_args()

    validate_environment(["NASS_API_KEY"])

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_quickstats.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("Transforms not yet implemented")


if __name__ == "__main__":
    main()
