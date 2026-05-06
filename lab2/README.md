# Lab 2 — AI-Powered Medallion Healthcare Pipeline (PySpark)

This is the AI-powered solution for Lab 2, demonstrating a Bronze → Silver → Gold medallion architecture with optional Ollama integration for dynamic watermark configuration.

## Architecture

```
Raw CSV → Bronze (Parquet) → Silver (Cleaned) → Gold (Aggregates)
              ↑                    ↑                  ↑
         Incremental          Deduplicated      Billing by
         Filter               Partitioned       Diagnosis
                                by date
```

## Setup

```bash
pip install -r requirements.txt
```

**Prerequisites:** Java JDK 11 or 17 (required for PySpark locally)

## Run

### Full Pipeline
```bash
cd pipeline
python main.py
```

### Individual Stages
```bash
cd pipeline
python main.py --stage bronze
python main.py --stage silver
python main.py --stage gold
```

### AI-Enabled Mode (with Ollama)
```bash
cd pipeline
python main.py --ai --intent "Keep incremental visits strictly after 2024-01-01"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="Set watermark to 2024-01-15"
python main.py
```

## Prefect Orchestration

```bash
cd pipeline
python dags/prefect_flow.py
```

## Outputs

- `output/bronze/`: Raw ingested data (Parquet)
- `output/silver/`: Cleaned data, partitioned by `visit_date`
- `output/gold/`: Aggregated billing by diagnosis

## Tests

```bash
cd pipeline
python -m pytest tests -v
```

Run integration tests (requires pipeline execution first):
```bash
cd pipeline
export RUN_LAB2_SPARK_INTEGRATION=1
python -m pytest tests -v
```

## AI Integration Details

The `--ai` flag enables runtime JSON watermark generation:
- **Safe**: Only validated `last_run_date` parameter is used
- **Configurable**: Override via `--intent` or `PIPELINE_AI_INTENT` env var
- **Fallbacks**: Uses config default if Ollama unavailable

### Environment Variables

- `OLLAMA_HOST`: Ollama base URL (default: `http://127.0.0.1:11434`)
- `OLLAMA_MODEL`: Model to use (default: `llama3`)
- `USE_OLLAMA_PIPELINE`: Enable AI mode without CLI flag
- `PIPELINE_AI_INTENT`: Default intent if `--intent` not provided

## Project Structure

```
pipeline/
├── main.py                 # Entrypoint with AI integration
├── config/
│   └── settings.yaml       # Pipeline configuration
├── bronze/
│   └── ingest.py           # CSV → Parquet with incremental filter
├── silver/
│   └── clean.py            # Clean, dedupe, partition
├── gold/
│   └── aggregate.py        # Business aggregates
├── dags/
│   └── prefect_flow.py     # Prefect orchestration
├── genai/
│   ├── ollama_client.py    # HTTP client for Ollama
│   └── json_extract.py     # JSON extraction utilities
└── tests/
    ├── test_pipeline.py    # Config tests
    └── test_integration_optional.py  # Spark integration tests
```
