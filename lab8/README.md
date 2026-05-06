# Lab 8 — Auto DAG Pipeline with Prefect (Logistics)

AI-powered solution for Lab 8, demonstrating workflow orchestration with Prefect including retries, caching, and pure function design.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Prefect Flow: logistics-pipeline               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│   │   ingest     │───▶│    clean     │───▶│  transform   │      │
│   │   (retries)  │    │   (retries)  │    │  (retries)   │      │
│   └──────────────┘    └──────────────┘    └──────────────┘      │
│         │                     │                    │             │
│    [cache 1hr]          [retries=2]          [retries=1]         │
│                                                                  │
│                                          │                       │
│                                          ▼                       │
│                                    ┌──────────────┐             │
│                                    │  save_data   │             │
│                                    └──────────────┘             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
lab8/
├── README.md
├── requirements.txt
├── data/
│   └── shipments.csv          # Logistics data
├── genai/                     # Self-contained Ollama module
│   ├── __init__.py
│   ├── ollama_client.py
│   └── json_extract.py
└── tests/
    └── test_pipeline.py       # Tests for pure functions
```

## Setup

```bash
cd lab8
pip install -r requirements.txt
```

## Run

### Standard Mode
```bash
python pipeline.py
```

Prefect may start a temporary local API server on first run—wait until tasks complete.

### AI-Enabled Mode
```bash
python pipeline.py --ai --intent "Use mean for null delivery times, 5 retries for ingest"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="High reliability: 5 retries for all tasks"
python pipeline.py
```

## Outputs

- `output/avg_delivery_by_destination.csv` — Mean delivery time grouped by destination

## Prefect Features Demonstrated

1. **Task Retries**: Automatic retry on failure with exponential backoff
   - `ingest`: 3 retries (network operations)
   - `clean`: 2 retries
   - `transform`: 1 retry

2. **Task Caching**: Results cached for 1 hour to avoid reprocessing
   - `ingest` uses `task_input_hash` for caching

3. **Pure Functions**: Core logic is pure Pandas (testable without Prefect)
   - `clean_shipments()`
   - `avg_delivery_by_destination()`

4. **Flow Composition**: Clean DAG structure with dependencies

## AI Integration

The `--ai` flag enables AI-driven DAG configuration:
- **null_fill**: How to handle null delivery times (0, mean, median)
- **ingest_retries**: Number of retries for data ingestion
- **clean_retries**: Number of retries for data cleaning

## Tests

```bash
python -m pytest tests -v
```

**Test Coverage:**
- Data file existence
- Pure clean function
- Pure transform function
- Full flow integration
- AI parameter application

**Note:** These tests validate pure functions only—no Prefect runtime required. Fast execution.

## Key Features

- ✅ Prefect workflow orchestration with retries
- ✅ Task caching for performance
- ✅ Pure functions for testability
- ✅ AI-configurable retry and null-fill logic
- ✅ Self-contained genai module
- ✅ Graceful fallbacks when AI unavailable

## Prefect UI

To view runs in Prefect UI:

```bash
prefect server start
```

Then access `http://localhost:4200` to see flow runs and task states.
