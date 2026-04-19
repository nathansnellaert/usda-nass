# USDA NASS Data Connector

Fetches agricultural statistics from USDA National Agricultural Statistics Service QuickStats API.

## Source

- **API**: https://quickstats.nass.usda.gov/api
- **Auth**: API key (set `NASS_API_KEY` environment variable)
- **Limit**: 50,000 records per request

## Coverage

All datasets are state-level, annual, 1950-2025.

| Category | Datasets |
|----------|----------|
| Corn | production, yield, area harvested |
| Soybeans | production, yield, area harvested |
| Wheat | production, yield, area harvested |
| Rice | production, yield |
| Livestock | cattle inventory, hog inventory, chicken inventory |
| Dairy/Eggs | milk production, egg production |

**Scope decision**: Focused on the most widely-used USDA NASS indicators at the state level. County-level data, census data, and less common commodities (cotton, sugar, tobacco, vegetables) are excluded to keep the connector focused. Can be expanded by adding entries to the DATASETS dict.

## Output

Each dataset produces a Delta table with columns: `year`, `state`, `state_code`, `value`, `unit`, `reference_period`.

Values suppressed by USDA (marked as "(D)", "(Z)", etc.) are stored as null.
