# Lab 3 — AI-Powered Retail Workflow Pipeline

This is the AI-powered solution for Lab 3, demonstrating a retail ETL pipeline with optional Ollama integration for dynamic cleaning parameters.

## Architecture

```
Raw Orders CSV → Clean → Transform → Revenue + Daily Sales Reports
                     ↑
              AI-Configurable:
              - quantity fill value
              - price fill value
              - filter non-positive prices
```

## Setup

```bash
pip install -r requirements.txt
```

## Run

### Standard Mode
```bash
cd pipeline
python main.py
```

### AI-Enabled Mode (with Ollama)
```bash
cd pipeline
python main.py --ai --intent "Default fills stay 1 and 0; do not drop zero-price rows"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="Keep zero-price rows for reporting"
python main.py
```

## Outputs

- `output/revenue.csv` — Total revenue by category
- `output/daily_sales.csv` — Daily order counts

## Tests

```bash
cd pipeline
python -m pytest tests -v
```

## AI Integration Details

The `--ai` flag enables runtime JSON parameter generation:
- **quantity_missing_fill**: Number to fill null quantities (default: 1)
- **price_missing_fill**: Number to fill null prices (default: 0)
- **filter_non_positive_price**: Boolean to filter rows with price <= 0

### Environment Variables

- `OLLAMA_HOST`: Ollama base URL (default: `http://127.0.0.1:11434`)
- `OLLAMA_MODEL`: Model to use (default: `llama3`)
- `USE_OLLAMA_PIPELINE`: Enable AI mode without `--ai` flag
- `PIPELINE_AI_INTENT`: Default intent if `--intent` not provided

## Project Structure

```
pipeline/
├── main.py                 # Entrypoint with AI integration
├── genai/
│   ├── __init__.py
│   ├── ollama_client.py    # HTTP client for Ollama
│   └── json_extract.py     # JSON extraction utilities
└── tests/
    └── test_pipeline.py    # pytest tests
```

## Data Flow

1. **Load**: Read orders from `data/orders.csv`
2. **Clean**: Remove duplicates, fill nulls, optionally filter non-positive prices
3. **Transform**: Calculate total revenue per category and daily order counts
4. **Save**: Write results to `output/` folder

## Key Features

- ✅ Self-contained genai module (no external dependencies)
- ✅ Safe AI usage (validated JSON outputs only)
- ✅ Configurable cleaning strategies via natural language
- ✅ Graceful fallbacks when AI unavailable
