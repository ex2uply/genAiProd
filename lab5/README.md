# Lab 5 — Telecom CDC Pipeline with Watermark (Pandas)

AI-powered solution for Lab 5, demonstrating Change Data Capture (CDC) with incremental loading using watermarks.

## Architecture

```
Watermark File → Load New Records → Transform → Save Output → Update Watermark
                      ↑
               AI-Configurable:
               - Lookback days
               - Processing strategy
```

## Project Structure

```
lab5/
├── README.md
├── requirements.txt
├── watermark.txt           # Tracks last processed date
├── data/
│   ├── cdr_day1.csv       # Day 1 CDR data
│   └── cdr_day2.csv       # Day 2 CDR data
├── genai/                 # Self-contained Ollama module
│   ├── __init__.py
│   ├── ollama_client.py
│   └── json_extract.py
└── tests/
    └── test_pipeline.py
```

## Setup

```bash
cd lab5
pip install -r requirements.txt
```

## Run

### Standard Mode (Incremental)
```bash
python pipeline.py
```

### AI-Enabled Mode (with Ollama)
```bash
# AI-controlled watermark configuration
python pipeline.py --ai --intent "Process with 2-day lookback for late-arriving data"

# Full reload mode
python pipeline.py --ai --intent "Full reload all historical data"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="Reload last 7 days"
python pipeline.py
```

## Outputs

- `output/caller_summary.csv` — Total duration and call count per caller
- `watermark.txt` — Updated with latest processed date

## How CDC Works

1. **Read Watermark**: Load the last processed date from `watermark.txt`
2. **Load Incremental Data**: Read all CSV files but filter `call_date > watermark`
3. **Transform**: Aggregate duration and count per caller
4. **Save Output**: Write results to `output/caller_summary.csv`
5. **Update Watermark**: Write the latest `call_date` to `watermark.txt`

## AI Integration

The `--ai` flag enables AI-driven CDC configuration:
- **watermark_date**: Suggested new watermark after processing
- **processing_strategy**: `"incremental"` or `"full_reload"`
- **lookback_days**: Number of days to look back (for late-arriving data)

## Tests

```bash
python -m pytest tests -v
```

**Test Coverage:**
- Watermark read/write operations
- Data file existence
- Watermark filtering logic
- Data transformation
- Full pipeline execution
- AI parameter application

## Key Features

- ✅ Incremental loading with watermark pattern
- ✅ AI-driven CDC configuration (lookback, full reload)
- ✅ Self-contained genai module
- ✅ Graceful fallbacks when AI unavailable
