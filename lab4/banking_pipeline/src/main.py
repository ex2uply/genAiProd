"""Banking ETL entrypoint with optional AI integration."""
import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_PATH = LAB_ROOT / "data" / "transactions.csv"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def load_data(file_path: Path) -> pd.DataFrame:
    """Load transaction data from CSV."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        LOG.error("Error loading file: %s", e)
        raise


def clean_data(df: pd.DataFrame, missing_fill: float = 0) -> pd.DataFrame:
    """Remove duplicates and handle missing amounts."""
    df = df.drop_duplicates().copy()
    df["amount"] = df["amount"].fillna(missing_fill)
    return df


def detect_fraud(df: pd.DataFrame, threshold: float = 800) -> pd.DataFrame:
    """Flag high-value transactions."""
    out = df.copy()
    out["is_fraud"] = out["amount"] > threshold
    return out


def aggregate_by_account(df: pd.DataFrame) -> pd.DataFrame:
    """Total transaction amount per account."""
    return df.groupby("account_id")["amount"].sum().reset_index()


def save_data(df: pd.DataFrame, path: Path) -> None:
    """Save DataFrame to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def dataframe_summary(df: pd.DataFrame, max_rows_sample: int = 3) -> str:
    """Generate a summary of the DataFrame for AI context."""
    lines = [
        f"rows={len(df)} cols={len(df.columns)}",
        "columns: " + ", ".join(df.columns.astype(str).tolist()),
        "dtypes:\n" + df.dtypes.astype(str).to_string(),
        "null_counts:\n" + df.isnull().sum().astype(str).to_string(),
        "head:\n" + df.head(max_rows_sample).to_string(index=False),
    ]
    return "\n".join(lines)


def banking_fraud_spec(
    summary: str,
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """Get fraud detection parameters from AI or use defaults.
    
    Args:
        summary: DataFrame summary string
        user_intent: Natural language intent for fraud detection
        model: Ollama model to use
        
    Returns:
        Dict with fraud_threshold and missing_fill
    """
    defaults = {"fraud_threshold": 800.0, "missing_fill": 0.0}
    intent = user_intent or os.environ.get(
        "PIPELINE_AI_INTENT",
        "Banking fraud detection: flag high-value transactions as fraud.",
    )
    
    try:
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        system_msg = (
            "You are a careful data pipeline planner. Reply with ONE JSON object only. "
            "No markdown fences, no commentary outside JSON."
        )
        user_msg = f"""Lab: banking transactions CSV.

Dataset summary:
{summary}

User intent:
{intent}

Return JSON with exactly these keys:
- fraud_threshold: number (typically 500-1000, default 800)
- missing_fill: number (use 0 if unsure)

Suggested defaults: {defaults}
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)

        threshold = spec.get("fraud_threshold", defaults["fraud_threshold"])
        fill = spec.get("missing_fill", defaults["missing_fill"])

        try:
            threshold_f = float(threshold)
        except (TypeError, ValueError):
            threshold_f = float(defaults["fraud_threshold"])
        try:
            fill_f = float(fill)
        except (TypeError, ValueError):
            fill_f = float(defaults["missing_fill"])

        return {"fraud_threshold": threshold_f, "missing_fill": fill_f}
    except Exception:
        return defaults


def run_pipeline(*, use_ai: bool = False, ai_intent: str | None = None) -> None:
    """Run the banking pipeline.
    
    Args:
        use_ai: Whether to use AI for parameter configuration
        ai_intent: Natural language intent for AI
    """
    LOG.info("Pipeline started")
    
    # Default parameters
    fraud_threshold = 800.0
    missing_fill = 0.0
    
    df = load_data(DATA_PATH)
    
    # Get AI parameters if enabled
    if use_ai:
        summary = dataframe_summary(df)
        spec = banking_fraud_spec(summary, ai_intent)
        LOG.info("AI-enabled spec: %s", spec)
        fraud_threshold = spec["fraud_threshold"]
        missing_fill = spec["missing_fill"]
    
    df = clean_data(df, missing_fill=missing_fill)
    df = detect_fraud(df, threshold=fraud_threshold)
    agg = aggregate_by_account(df)
    
    save_data(df, OUTPUT_DIR / "clean.csv")
    save_data(agg, OUTPUT_DIR / "agg.csv")
    
    LOG.info("Pipeline finished")
    LOG.info("Fraud threshold used: %s", fraud_threshold)
    LOG.info("Missing amount fill: %s", missing_fill)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Banking ETL (optional --ai uses Ollama JSON spec).")
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for fraud parameters (JSON-only).")
    parser.add_argument("--intent", default=None, help="Natural-language intent forwarded to the model.")
    args = parser.parse_args()
    
    use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
    run_pipeline(use_ai=use_ai, ai_intent=args.intent)
