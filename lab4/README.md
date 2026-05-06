# Lab 4 — AI-Powered Banking Pipeline (Modular Scaffold)

This is the AI-powered solution for Lab 4, demonstrating a modular `src/` package structure with optional Ollama integration for dynamic fraud detection configuration.

## Architecture

```
Raw Transactions CSV → Load → Clean → Fraud Detection → Aggregate → Save
                           ↑              ↑
                    AI-Configurable  AI-Configurable:
                    - missing_fill     - fraud_threshold
```

## Project Structure

```
banking_pipeline/
├── src/
│   ├── __init__.py
│   ├── main.py              # Entrypoint with AI integration
│   ├── load.py              # Data loading
│   ├── transform.py         # Cleaning and aggregation
│   ├── fraud.py             # Fraud detection
│   ├── save.py              # Data persistence
│   └── genai/               # Self-contained Ollama module
│       ├── __init__.py
│       ├── ollama_client.py
│       └── json_extract.py
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py     # pytest tests
└── output/                  # Generated on run
    ├── clean.csv
    └── agg.csv
```

## Setup

```bash
pip install -r requirements.txt
```

## Run

### Standard Mode
```bash
cd banking_pipeline/src
python main.py
```

### AI-Enabled Mode (with Ollama)
```bash
cd banking_pipeline/src
python main.py --ai --intent "Lower fraud threshold to 500 for stricter detection"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="Set fraud threshold to 600"
python main.py
```

## Outputs

- `banking_pipeline/output/clean.csv` — Cleaned transactions with `is_fraud` flag
- `banking_pipeline/output/agg.csv` — Total amounts per account

## Tests

```bash
cd banking_pipeline
python -m pytest tests -v
```

## AI Integration Details

The `--ai` flag enables runtime JSON parameter generation:
- **fraud_threshold**: Number above which transactions are flagged as fraud (default: 800)
- **missing_fill**: Number to fill missing amounts (default: 0)

### Environment Variables

- `OLLAMA_HOST`: Ollama base URL (default: `http://127.0.0.1:11434`)
- `OLLAMA_MODEL`: Model to use (default: `llama3`)
- `USE_OLLAMA_PIPELINE`: Enable AI mode without `--ai` flag
- `PIPELINE_AI_INTENT`: Default intent if `--intent` not provided

## Key Features

- ✅ Modular `src/` package structure
- ✅ Self-contained genai module (no external dependencies)
- ✅ Safe AI usage (validated JSON outputs only)
- ✅ Configurable fraud threshold via natural language
- ✅ Graceful fallbacks when AI unavailable
