# Case Study — AI-Powered Smart Agriculture Pipeline

End-to-end PySpark medallion flow with watermarking, schema evolution, broadcast enrichment, Prefect orchestration, alert logic, and **AI-driven alert thresholds**.

## Features

- **Bronze Layer**: Watermark-based CDC with `unionByName` schema evolution
- **Silver Layer**: Data cleaning, `mergeSchema`, partitioning by `field_id`
- **Gold Layer**: Broadcast join with fields dimension, AI-driven alerts
- **Alert Logic**: Temperature > threshold = "High Temp", Moisture < threshold = "Low Moisture"
- **AI Integration**: Runtime alert threshold configuration via Ollama

## Dataset

- `sensor_day1.csv` — sensor readings (sensor_id, field_id, temperature, moisture, timestamp)
- `sensor_day2.csv` — adds **humidity** column (schema evolution exercise)
- `fields.csv` — farm metadata (field_id, farm_name, region)

## Setup

```bash
cd case-study
pip install -r requirements.txt
```

## Run Pipeline

### Full Pipeline
```bash
cd pipeline
python main.py
```

### Run Specific Stages
```bash
cd pipeline
python main.py --stage bronze --stage silver
```

### With AI-Driven Alert Thresholds
```bash
# Via command line
cd pipeline
python main.py --ai "stricter monitoring with lower temperature threshold"

# Or via environment variable
export USE_OLLAMA_PIPELINE=true
export PIPELINE_AI_INTENT="more sensitive to temperature changes"
cd pipeline
python main.py
```

## Prefect Orchestration

```bash
cd pipeline
python prefect_flow.py
```

Note: Prefect may start a temporary local API server on first run.

## Outputs

Artifacts land under `output/`:

- `output/bronze/` — Incremental slice landed as parquet (unionByName schema evolution)
- `output/silver/` — Cleaned data partitioned by `field_id`
- `output/gold/alerts/` — Alerts with status (High Temp / Low Moisture / Normal)
- `output/gold/field_summary/` — Aggregated metrics by field
- `output/watermark.txt` — Last processed timestamp for CDC

## AI Integration

### Alert Threshold Configuration

The `--ai` flag or `PIPELINE_AI_INTENT` environment variable enables AI-driven alert thresholds:

```bash
export USE_OLLAMA_PIPELINE=true
export OLLAMA_HOST=http://localhost:11434
export OLLAMA_MODEL=llama3.2:latest
export PIPELINE_AI_INTENT="monitor for heat stress in crops"

cd pipeline
python main.py
```

**Default thresholds** (when AI unavailable):
- High Temp: 35°C
- Low Moisture: 40%

### Gen AI Module

Located in `genai/`:

- `ollama_client.py` — HTTP client for Ollama, `ask_alert_config()` function
- `json_extract.py` — Robust JSON extraction from LLM outputs

## Tests

```bash
python -m pytest tests -v
```

**Test Coverage**:

- `test_alert_rules.py` — Alert logic validation with custom thresholds
- `test_pipeline.py` — Config loading, data files, watermark functions, AI spec

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Bronze (Raw)                                           │
│  ├── unionByName with allowMissingColumns              │
│  └── Watermark-based CDC                               │
├─────────────────────────────────────────────────────────┤
│  Silver (Cleaned)                                       │
│  ├── mergeSchema for schema evolution                  │
│  ├── fillna defaults                                   │
│  └── partitionBy field_id                              │
├─────────────────────────────────────────────────────────┤
│  Gold (Business)                                        │
│  ├── broadcast join fields dimension                   │
│  ├── AI-driven alert thresholds                          │
│  └── Aggregation: avg + alert counts                    │
└─────────────────────────────────────────────────────────┘
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `JAVA_HOME` / Spark errors | Install JDK 11/17 and add to PATH |
| `java -version` fails | Verify Java installation |
| Ollama connection error | Ensure `ollama serve` is running |
| Prefect API timeout | Wait for first run to complete |

## Requirements

- Python 3.10+
- PySpark >= 3.4.0
- prefect >= 2.14.0
- pytest >= 7.0.0
- PyYAML >= 6.0
- pandas >= 2.0.0
- Java JDK 11 or 17
- Ollama (optional, for AI features)

---

**Created for:** Week 2, Day 3 — AI-Powered Data Pipelines
