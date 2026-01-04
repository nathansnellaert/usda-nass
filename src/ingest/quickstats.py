"""Ingest data from USDA NASS QuickStats API.

API Docs: https://quickstats.nass.usda.gov/api
Requires API key (NASS_API_KEY environment variable).
Max 50,000 records per request - must filter by commodity/year.

Coverage:
- Major grains: Corn, Soybeans, Wheat, Rice, Barley, Oats, Sorghum
- Other field crops: Cotton, Hay, Peanuts, Sunflower, Sugar Beets, Tobacco, Potatoes
- Livestock: Cattle, Hogs, Chickens, Sheep, Turkeys
- Dairy: Milk, Eggs
- Statistics: Production, Yield, Area Harvested, Inventory (where applicable)
- Time range: 1950-present (split into chunks to stay under 50k limit)
"""

import os
import time
from urllib.parse import quote
from subsets_utils import get, save_raw_json, load_state, save_state

API_KEY = os.environ["NASS_API_KEY"]
BASE_URL = "https://quickstats.nass.usda.gov/api"

# Comprehensive agricultural datasets
# Each query is scoped to stay under 50k limit by filtering on commodity/statistic/years
# Year ranges split into chunks for large datasets

# Major grains - production, yield, area harvested
DATASETS = {
    # === CORN ===
    "corn_production": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Corn Production",
        "desc": "Corn production by state",
        "year_ranges": [(1950, 1979), (1980, 2005), (2006, 2025)],
    },
    "corn_yield": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Corn Yield",
        "desc": "Corn yield by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    "corn_area_harvested": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
        },
        "name": "Corn Area Harvested",
        "desc": "Corn area harvested by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    # === SOYBEANS ===
    "soybeans_production": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Soybeans Production",
        "desc": "Soybean production by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    "soybeans_yield": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Soybeans Yield",
        "desc": "Soybean yield by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    "soybeans_area_harvested": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
        },
        "name": "Soybeans Area Harvested",
        "desc": "Soybean area harvested by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    # === WHEAT ===
    "wheat_production": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Wheat Production",
        "desc": "Wheat production by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    "wheat_yield": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Wheat Yield",
        "desc": "Wheat yield by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    "wheat_area_harvested": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
        },
        "name": "Wheat Area Harvested",
        "desc": "Wheat area harvested by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    # === RICE ===
    "rice_production": {
        "params": {
            "commodity_desc": "RICE",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Rice Production",
        "desc": "Rice production by state",
        "year_ranges": [(1950, 2025)],
    },
    "rice_yield": {
        "params": {
            "commodity_desc": "RICE",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Rice Yield",
        "desc": "Rice yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === BARLEY ===
    "barley_production": {
        "params": {
            "commodity_desc": "BARLEY",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Barley Production",
        "desc": "Barley production by state",
        "year_ranges": [(1950, 2025)],
    },
    "barley_yield": {
        "params": {
            "commodity_desc": "BARLEY",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Barley Yield",
        "desc": "Barley yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === OATS ===
    "oats_production": {
        "params": {
            "commodity_desc": "OATS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Oats Production",
        "desc": "Oats production by state",
        "year_ranges": [(1950, 2025)],
    },
    "oats_yield": {
        "params": {
            "commodity_desc": "OATS",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Oats Yield",
        "desc": "Oats yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === SORGHUM ===
    "sorghum_production": {
        "params": {
            "commodity_desc": "SORGHUM",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Sorghum Production",
        "desc": "Sorghum production by state",
        "year_ranges": [(1950, 2025)],
    },
    "sorghum_yield": {
        "params": {
            "commodity_desc": "SORGHUM",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Sorghum Yield",
        "desc": "Sorghum yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === COTTON ===
    "cotton_production": {
        "params": {
            "commodity_desc": "COTTON",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Cotton Production",
        "desc": "Cotton production by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    "cotton_yield": {
        "params": {
            "commodity_desc": "COTTON",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Cotton Yield",
        "desc": "Cotton yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === HAY ===
    "hay_production": {
        "params": {
            "commodity_desc": "HAY",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Hay Production",
        "desc": "Hay production by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    "hay_yield": {
        "params": {
            "commodity_desc": "HAY",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Hay Yield",
        "desc": "Hay yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === PEANUTS ===
    "peanuts_production": {
        "params": {
            "commodity_desc": "PEANUTS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Peanuts Production",
        "desc": "Peanuts production by state",
        "year_ranges": [(1950, 2025)],
    },
    "peanuts_yield": {
        "params": {
            "commodity_desc": "PEANUTS",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Peanuts Yield",
        "desc": "Peanuts yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === SUNFLOWER ===
    "sunflower_production": {
        "params": {
            "commodity_desc": "SUNFLOWER",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Sunflower Production",
        "desc": "Sunflower production by state",
        "year_ranges": [(1970, 2025)],
    },
    # === SUGAR BEETS ===
    "sugarbeets_production": {
        "params": {
            "commodity_desc": "SUGARBEETS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Sugar Beets Production",
        "desc": "Sugar beets production by state",
        "year_ranges": [(1950, 2025)],
    },
    "sugarbeets_yield": {
        "params": {
            "commodity_desc": "SUGARBEETS",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Sugar Beets Yield",
        "desc": "Sugar beets yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === TOBACCO ===
    "tobacco_production": {
        "params": {
            "commodity_desc": "TOBACCO",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Tobacco Production",
        "desc": "Tobacco production by state",
        "year_ranges": [(1950, 2025)],
    },
    # === POTATOES ===
    "potatoes_production": {
        "params": {
            "commodity_desc": "POTATOES",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Potatoes Production",
        "desc": "Potatoes production by state",
        "year_ranges": [(1950, 2025)],
    },
    "potatoes_yield": {
        "params": {
            "commodity_desc": "POTATOES",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
        },
        "name": "Potatoes Yield",
        "desc": "Potatoes yield by state",
        "year_ranges": [(1950, 2025)],
    },
    # === LIVESTOCK - CATTLE ===
    "cattle_inventory": {
        "params": {
            "commodity_desc": "CATTLE",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "source_desc": "SURVEY",
            "domain_desc": "TOTAL",
        },
        "name": "Cattle Inventory",
        "desc": "Cattle inventory by state (survey totals)",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    # === LIVESTOCK - HOGS ===
    "hogs_inventory": {
        "params": {
            "commodity_desc": "HOGS",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "source_desc": "SURVEY",
            "domain_desc": "TOTAL",
        },
        "name": "Hogs Inventory",
        "desc": "Hog inventory by state (survey totals)",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    # === LIVESTOCK - CHICKENS ===
    "chickens_inventory": {
        "params": {
            "commodity_desc": "CHICKENS",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Chickens Inventory",
        "desc": "Chickens inventory by state",
        "year_ranges": [(1950, 2025)],
    },
    # === LIVESTOCK - SHEEP ===
    "sheep_inventory": {
        "params": {
            "commodity_desc": "SHEEP",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Sheep Inventory",
        "desc": "Sheep inventory by state",
        "year_ranges": [(1950, 2025)],
    },
    # === LIVESTOCK - TURKEYS ===
    "turkeys_inventory": {
        "params": {
            "commodity_desc": "TURKEYS",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "name": "Turkeys Inventory",
        "desc": "Turkeys inventory by state",
        "year_ranges": [(1950, 2025)],
    },
    # === DAIRY - MILK ===
    "milk_production": {
        "params": {
            "commodity_desc": "MILK",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Milk Production",
        "desc": "Milk production by state",
        "year_ranges": [(1950, 1989), (1990, 2025)],
    },
    # === EGGS ===
    "eggs_production": {
        "params": {
            "commodity_desc": "EGGS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
        },
        "name": "Eggs Production",
        "desc": "Eggs production by state",
        "year_ranges": [(1950, 2025)],
    },
}


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

    # Create list of all dataset+year_range combinations
    all_jobs = []
    for dataset_key, dataset_info in DATASETS.items():
        for year_start, year_end in dataset_info["year_ranges"]:
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
