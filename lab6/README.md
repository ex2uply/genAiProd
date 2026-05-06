# Lab 6 — Schema Evolution Pipeline (Pandas)

AI-powered solution for Lab 6, demonstrating handling schema changes over time as new columns are added.

## Schema Evolution Example

```
Day 1: product_id, name, category, price
Day 2: + brand (new column)
Day 3: + discount (another new column)
```

## Architecture

```
Day1.csv ──┐
Day2.csv ──┼──> Merge Schema ──> Clean (Fill Nulls) ──> Save
Day3.csv ──┘         ↑
               AI-Configurable:
               - default_brand
               - default_discount
```

## Project Structure

```
lab6/
├── README.md
├── requirements.txt
├── data/
│   ├── products_day1.csv   # Base schema
│   ├── products_day2.csv   # + brand column
│   └── products_day3.csv   # + discount column
├── genai/                  # Self-contained Ollama module
│   ├── __init__.py
│   ├── ollama_client.py
│   └── json_extract.py
└── tests/
    └── test_pipeline.py
```

## Setup

```bash
cd lab6
pip install -r requirements.txt
```

## Run

### Standard Mode
```bash
python pipeline.py
```

### AI-Enabled Mode
```bash
python pipeline.py --ai --intent "Use 'N/A' for missing brands and 0.5 for discounts"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="Set default brand to Generic"
python pipeline.py
```

## Outputs

- `output/final_products.csv` — Merged products with all columns and filled nulls

## How Schema Evolution Works

1. **Load All Snapshots**: Read all daily CSV files
2. **Union Columns**: Merge all DataFrames, adding missing columns as NA
3. **Clean**: Fill nulls with defaults (brand → "Unknown", discount → 0)
4. **Save**: Write unified output

## AI Integration

The `--ai` flag enables AI-driven default values:
- **default_brand**: String for missing brand values
- **default_discount**: Number for missing discount values

## Tests

```bash
python -m pytest tests -v
```

**Test Coverage:**
- Daily snapshot file existence
- Schema union (all columns present)
- Null filling with defaults
- Full pipeline execution
- AI parameter application

## Key Features

- ✅ Schema evolution handling (adding columns over time)
- ✅ AI-configurable default values
- ✅ Self-contained genai module
- ✅ Graceful fallbacks when AI unavailable
