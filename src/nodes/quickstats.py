"""USDA NASS QuickStats - US agricultural statistics by state.

Source: https://quickstats.nass.usda.gov/api
Covers major crops (corn, soybeans, wheat, rice), livestock (cattle, hogs,
chickens), and dairy/eggs at the US state level, 1950-2025.
"""
import os
import time

import pyarrow as pa
from urllib.parse import quote
from subsets_utils import (
    get, save_raw_json, load_raw_json, load_state, save_state,
    merge, publish, data_hash,
)

BASE_URL = "https://quickstats.nass.usda.gov/api"
LICENSE = "Public domain (US Government work)"

COLUMN_DESCRIPTIONS = {
    "year": "Survey year",
    "state": "US state name",
    "state_code": "Two-letter state abbreviation",
    "value": "Reported numeric value (null if withheld or suppressed by USDA)",
    "unit": "Unit of measurement",
    "reference_period": "Time period of observation",
}

DATASETS = {
    "corn_production": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Corn Production by State",
        "description": "Annual corn production by US state from USDA National Agricultural Statistics Service surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "corn_yield": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Corn Yield by State",
        "description": "Annual corn yield per acre by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "corn_area_harvested": {
        "params": {
            "commodity_desc": "CORN",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Corn Area Harvested by State",
        "description": "Annual corn area harvested by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "soybeans_production": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Soybean Production by State",
        "description": "Annual soybean production by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "soybeans_yield": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Soybean Yield by State",
        "description": "Annual soybean yield per acre by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "soybeans_area_harvested": {
        "params": {
            "commodity_desc": "SOYBEANS",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Soybean Area Harvested by State",
        "description": "Annual soybean area harvested by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "wheat_production": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Wheat Production by State",
        "description": "Annual wheat production by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "wheat_yield": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Wheat Yield by State",
        "description": "Annual wheat yield per acre by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "wheat_area_harvested": {
        "params": {
            "commodity_desc": "WHEAT",
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Wheat Area Harvested by State",
        "description": "Annual wheat area harvested by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "rice_production": {
        "params": {
            "commodity_desc": "RICE",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Rice Production by State",
        "description": "Annual rice production by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "rice_yield": {
        "params": {
            "commodity_desc": "RICE",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Rice Yield by State",
        "description": "Annual rice yield per acre by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "cattle_inventory": {
        "params": {
            "commodity_desc": "CATTLE",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "source_desc": "SURVEY",
            "domain_desc": "TOTAL",
            "class_desc": "INCL CALVES",
        },
        "title": "USDA NASS: Cattle Inventory by State",
        "description": "Annual cattle inventory (including calves) by US state from USDA NASS surveys.",
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
        "title": "USDA NASS: Hog Inventory by State",
        "description": "Annual hog inventory by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "chickens_inventory": {
        "params": {
            "commodity_desc": "CHICKENS",
            "statisticcat_desc": "INVENTORY",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Chicken Inventory by State",
        "description": "Annual chicken inventory by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "milk_production": {
        "params": {
            "commodity_desc": "MILK",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
            "class_desc": "ALL CLASSES",
            "freq_desc": "ANNUAL",
        },
        "title": "USDA NASS: Milk Production by State",
        "description": "Annual milk production by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
    "eggs_production": {
        "params": {
            "commodity_desc": "EGGS",
            "statisticcat_desc": "PRODUCTION",
            "agg_level_desc": "STATE",
            "domain_desc": "TOTAL",
        },
        "title": "USDA NASS: Egg Production by State",
        "description": "Annual egg production by US state from USDA NASS surveys.",
        "year_ranges": [(1950, 2025)],
    },
}


def _api_key():
    return os.environ["NASS_API_KEY"]


def _fetch(params, year_start, year_end):
    query_params = {
        "key": _api_key(),
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
    return result.get("data", [])


def _raw_id(key, ys, ye):
    return f"nass_{key}_{ys}_{ye}"


def _parse_value(raw):
    if not raw or not isinstance(raw, str):
        return None
    v = raw.strip()
    if not v or v.startswith("("):
        return None
    try:
        return float(v.replace(",", ""))
    except ValueError:
        return None


def download():
    print("Fetching USDA NASS QuickStats data...")
    state = load_state("nass_quickstats")
    completed = set(state.get("completed", []))

    jobs = []
    for key, info in DATASETS.items():
        for ys, ye in info["year_ranges"]:
            job_key = f"{key}_{ys}_{ye}"
            if job_key not in completed:
                jobs.append((key, info, ys, ye, job_key))

    if not jobs:
        print("  All datasets up to date")
        return

    print(f"  {len(jobs)} datasets to fetch")
    for i, (key, info, ys, ye, job_key) in enumerate(jobs, 1):
        print(f"  [{i}/{len(jobs)}] {info['title']} ({ys}-{ye})...")
        data = _fetch(info["params"], ys, ye)
        save_raw_json(data, _raw_id(key, ys, ye), compress=True)
        print(f"    {len(data):,} records")

        completed.add(job_key)
        save_state("nass_quickstats", {"completed": list(completed)})
        time.sleep(1)


def transform():
    print("Transforming USDA NASS datasets...")

    for key, info in DATASETS.items():
        dataset_id = f"nass_{key}"

        records = []
        for ys, ye in info["year_ranges"]:
            raw = load_raw_json(_raw_id(key, ys, ye))
            if isinstance(raw, list):
                records.extend(raw)
            elif isinstance(raw, dict) and "data" in raw:
                records.extend(raw["data"])

        if not records:
            print(f"  Skipping {dataset_id} - no data")
            continue

        rows = []
        for r in records:
            rows.append({
                "year": int(r["year"]),
                "state": r["state_name"],
                "state_code": r["state_alpha"],
                "value": _parse_value(r.get("Value", "")),
                "unit": r["unit_desc"],
                "reference_period": r["reference_period_desc"],
            })

        table = pa.Table.from_pylist(rows)

        h = data_hash(table)
        prev = load_state(dataset_id)
        if prev.get("hash") == h:
            print(f"  Skipping {dataset_id} - unchanged")
            continue

        merge(table, dataset_id, key=["year", "state", "reference_period"])
        publish(dataset_id, {
            "id": dataset_id,
            "title": info["title"],
            "description": info["description"],
            "license": LICENSE,
            "column_descriptions": COLUMN_DESCRIPTIONS,
        })
        save_state(dataset_id, {"hash": h})
        print(f"  Published {dataset_id}: {len(table):,} rows")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
