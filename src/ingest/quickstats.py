"""Ingest data from USDA NASS QuickStats API.

API Docs: https://quickstats.nass.usda.gov/api
Requires API key (NASS_API_KEY environment variable).
Max 50,000 records per request - must filter by year or other params.
"""

import os
import time
from subsets_utils import get, save_raw_json, load_state, save_state

API_KEY = os.environ["NASS_API_KEY"]
BASE_URL = "https://quickstats.nass.usda.gov/api"

# Key commodity groups to fetch with their parameters
# We fetch by sector and source to stay under 50k limit
DATASETS = {
    "crops_survey": {
        "params": {"sector_desc": "CROPS", "source_desc": "SURVEY"},
        "name": "Crops Survey Data",
        "desc": "Crop production, yields, planted/harvested acres from surveys",
    },
    "crops_census": {
        "params": {"sector_desc": "CROPS", "source_desc": "CENSUS"},
        "name": "Crops Census Data",
        "desc": "Crop data from Census of Agriculture",
    },
    "animals_survey": {
        "params": {"sector_desc": "ANIMALS & PRODUCTS", "source_desc": "SURVEY"},
        "name": "Animals & Products Survey Data",
        "desc": "Livestock counts, milk production, eggs from surveys",
    },
    "animals_census": {
        "params": {"sector_desc": "ANIMALS & PRODUCTS", "source_desc": "CENSUS"},
        "name": "Animals & Products Census Data",
        "desc": "Livestock data from Census of Agriculture",
    },
    "economics": {
        "params": {"sector_desc": "ECONOMICS"},
        "name": "Agricultural Economics",
        "desc": "Farm income, expenses, land values",
    },
    "environmental": {
        "params": {"sector_desc": "ENVIRONMENTAL"},
        "name": "Environmental Data",
        "desc": "Fertilizer use, irrigation, conservation practices",
    },
}

# Years to fetch - split into chunks to stay under 50k limit
YEAR_RANGES = [
    (2020, 2024),
    (2015, 2019),
    (2010, 2014),
    (2005, 2009),
    (2000, 2004),
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

    query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
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
