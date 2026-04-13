"""USDA NASS QuickStats - download agricultural statistics.

Source: https://quickstats.nass.usda.gov/api
Requires NASS__get_api_key() environment variable.
"""
import os
import time
from urllib.parse import quote
from subsets_utils import get, save_raw_json, load_state, save_state

BASE_URL = "https://quickstats.nass.usda.gov/api"


def _get_api_key():
    return os.environ["NASS__get_api_key()"]

# Comprehensive agricultural datasets
DATASETS = {
    # === CORN ===
    "corn_production": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Corn Production",
        "desc": "Corn production by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    "corn_yield": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Corn Yield",
        "desc": "Corn yield by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    "corn_area_harvested": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Corn Area Harvested",
        "desc": "Corn area harvested by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    # === SOYBEANS ===
    "soybeans_production": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Soybeans Production",
        "desc": "Soybean production by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    "soybeans_yield": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Soybeans Yield",
        "desc": "Soybean yield by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    "soybeans_area_harvested": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Soybeans Area Harvested",
        "desc": "Soybean area harvested by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    # === WHEAT ===
    "wheat_production": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Wheat Production",
        "desc": "Wheat production by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    "wheat_yield": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Wheat Yield",
        "desc": "Wheat yield by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    "wheat_area_harvested": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Wheat Area Harvested",
        "desc": "Wheat area harvested by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    # === RICE ===
    "rice_production": {
        "params": {
            "commodity_desc": "RICE",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Rice Production",
        "desc": "Rice production by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    "rice_yield": {
        "params": {
            "commodity_desc": "RICE",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Rice Yield",
        "desc": "Rice yield by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    # === LIVESTOCK ===
    "cattle_inventory": {
        "params": {
            "commodity_desc": "CATTLE",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "source_desc": "SURVEY",
            "domain_desc": "TOTAL",
            "class_desc": "INCL CALVES",
        },
        "name": "Cattle Inventory",
        "desc": "Cattle inventory by state (all cattle including calves)",
        "year_ranges": [(1950, 2025)],
    },
    "hogs_inventory": {
        "params": {
            "commodity_desc": "HOGS",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "source_desc": "SURVEY",
            "domain_desc": "TOTAL",
            "class_desc": "ALL CLASSES",
        },
        "name": "Hogs Inventory",
        "desc": "Hog inventory by state (all classes)",
        "year_ranges": [(1950, 2025)],
    },
    "chickens_inventory": {
        "params": {
            "commodity_desc": "CHICKENS",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Chickens Inventory",
        "desc": "Chickens inventory by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
    # === DAIRY ===
    "milk_production": {
        "params": {
            "commodity_desc": "MILK",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
            "class_desc": "ALL CLASSES",
            "freq_desc": "ANNUAL",
        },
        "name": "Milk Production",
        "desc": "Milk production by state (annual, all classes)",
        "year_ranges": [(1950, 2025)],
    },
    "eggs_production": {
        "params": {
            "commodity_desc": "EGGS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Eggs Production",
        "desc": "Eggs production by state (totals)",
        "year_ranges": [(1950, 2025)],
    },
}


def fetch_data(params: dict, year_start: int, year_end: int) -> list:
    """Fetch data for a year range with given parameters."""
    query_params = {
        "key": _get_api_key(),
        "format": "JSON",
        "year__GE": str(year_start),
        "year__LE": str(year_end),
        **params,
    }

    query_string = "&".join(f"{k}={quote(str(v))}" for k, v in query_params.items())
    url = f"{BASE_URL}/api_GET/?{query_string}"

    response = get(url, timeout=300)
    result = response.json()

    if "error" in result:
        raise RuntimeError(f"NASS API error: {result['error']}")

    if "data" not in result:
        return []

    return result["data"]


def run():
    """Fetch all NASS QuickStats datasets."""
    print("Fetching USDA NASS QuickStats data...")

    state = load_state("nass_quickstats")
    completed = set(state.get("completed", []))

    all_jobs = []
    for dataset_key, dataset_info in DATASETS.items():
        for year_start, year_end in dataset_info["year_ranges"]:
            job_key = f"{dataset_key}_{year_start}_{year_end}"
            if job_key not in completed:
                all_jobs.append((dataset_key, dataset_info, year_start, year_end, job_key))

    if not all_jobs:
        print("  All datasets up to date")
        return

    print(f"  Jobs to fetch: {len(all_jobs)}")

    for i, (dataset_key, dataset_info, year_start, year_end, job_key) in enumerate(all_jobs, 1):
        print(f"  [{i}/{len(all_jobs)}] Fetching {dataset_info['name']} ({year_start}-{year_end})...")

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
        time.sleep(1)

    print(f"  Ingested {len(all_jobs)} dataset chunks")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
