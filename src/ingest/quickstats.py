"""Ingest data from USDA NASS QuickStats API.

API Docs: https://quickstats.nass.usda.gov/api
Requires API key (NASS_API_KEY environment variable).
Max 50,000 records per request - must filter by commodity/year.
"""

import os
import time
from urllib.parse import quote
from subsets_utils import get, save_raw_json, load_state, save_state

API_KEY = os.environ["NASS_API_KEY"]
BASE_URL = "https://quickstats.nass.usda.gov/api"

# Focus on key commodities with national-level production data
# Each query is scoped to stay under 50k limit
DATASETS = {
    "corn_production": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Corn Production",
        "desc": "Corn production by state",
    },
    "soybeans_production": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Soybeans Production",
        "desc": "Soybean production by state",
    },
    "wheat_production": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Wheat Production",
        "desc": "Wheat production by state",
    },
    "cattle_inventory": {
        "params": {
            "commodity_desc": "CATTLE",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
        },
        "name": "Cattle Inventory",
        "desc": "Cattle inventory by state",
    },
    "hogs_inventory": {
        "params": {
            "commodity_desc": "HOGS",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
        },
        "name": "Hogs Inventory",
        "desc": "Hog inventory by state",
    },
    "milk_production": {
        "params": {
            "commodity_desc": "MILK",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Milk Production",
        "desc": "Milk production by state",
    },
    "cotton_production": {
        "params": {
            "commodity_desc": "COTTON",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Cotton Production",
        "desc": "Cotton production by state",
    },
    "rice_production": {
        "params": {
            "commodity_desc": "RICE",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Rice Production",
        "desc": "Rice production by state",
    },
}

# Fetch all years at once for national/state data (should be under 50k)
YEAR_RANGES = [
    (1990, 2024),
]


def fetch_data(params: dict, year_start: int, year_end: int) -> list:
    """Fetch data for a year range with given parameters."""
    query_params = {
        "key": API_KEY,
        "format": "JSON",
        "year__GE": str(year_start),
        "year__LE": str(year_end),
        **params,
    }

    # Properly URL-encode parameters
    query_string = "&".join(f"{k}={quote(str(v))}" for k, v in query_params.items())
    url = f"{BASE_URL}/api_GET/?{query_string}"

    response = get(url, timeout=300)
    result = response.json()

    if "data" not in result:
        return []

    return result["data"]


def run():
    """Fetch all NASS QuickStats datasets."""
    print("Fetching USDA NASS QuickStats data...")

    state = load_state("nass_quickstats")
    completed = set(state.get("completed", []))

    # Create list of all dataset+year_range combinations
    all_jobs = []
    for dataset_key, dataset_info in DATASETS.items():
        for year_start, year_end in YEAR_RANGES:
            job_key = f"{dataset_key}_{year_start}_{year_end}"
            if job_key not in completed:
                all_jobs.append((dataset_key, dataset_info, year_start, year_end, job_key))

    if not all_jobs:
        print("All datasets up to date")
        return

    print(f"  Jobs to fetch: {len(all_jobs)}")

    for i, (dataset_key, dataset_info, year_start, year_end, job_key) in enumerate(all_jobs, 1):
        print(f"\n[{i}/{len(all_jobs)}] Fetching {dataset_info['name']} ({year_start}-{year_end})...")

        data = fetch_data(dataset_info["params"], year_start, year_end)

        if data:
            save_raw_json(
                {
                    "key": dataset_key,
                    "name": dataset_info["name"],
                    "description": dataset_info["desc"],
                    "year_start": year_start,
                    "year_end": year_end,
                    "data": data,
                },
                f"nass_{dataset_key}_{year_start}_{year_end}",
                compress=True,
            )
            print(f"    Saved {len(data):,} records")
        else:
            print(f"    No data available")

        completed.add(job_key)
        save_state("nass_quickstats", {"completed": list(completed)})
        time.sleep(1)  # Rate limiting

    print(f"\nIngested {len(all_jobs)} dataset chunks")
