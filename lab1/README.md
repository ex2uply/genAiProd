# Lab 1 — AI-Powered Healthcare ETL (Solution)

This is the AI-powered solution for Lab 1, demonstrating a modular healthcare ETL pipeline with optional Ollama integration for dynamic cleaning parameters.

## Setup

```bash
pip install -r requirements.txt
```

## Run

### Standard Mode (without AI)
```bash
python pipeline.py
```

### AI-Enabled Mode (with Ollama)
```bash
# Requires Ollama running locally
python pipeline.py --ai --intent "Use median for missing age; billing missing must be 0"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="Prefer median for missing age"
python pipeline.py
```

## Outputs

- `output/billing.csv`: Total billing grouped by diagnosis
- `output/daily.csv`: Daily patient counts

## Tests

```bash
python -m pytest tests -v
```

## AI Integration Details

The `--ai` flag enables runtime JSON spec generation from Ollama:
- **Safe**: Only validated JSON parameters are used; no code execution
- **Configurable**: Override via `--intent` or `PIPELINE_AI_INTENT` env var
- **Defaults**: Falls back to sensible defaults if Ollama is unavailable

### Environment Variables

- `OLLAMA_HOST`: Ollama base URL (default: `http://127.0.0.1:11434`)
- `OLLAMA_MODEL`: Model to use (default: `llama3`)
- `USE_OLLAMA_PIPELINE`: Enable AI mode without CLI flag
- `PIPELINE_AI_INTENT`: Default intent if `--intent` not provided
