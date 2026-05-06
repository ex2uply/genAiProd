"""Retail ETL: optional Ollama-driven cleaning parameters (JSON-only)."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "data" / "orders.csv"
OUT_DIR = ROOT / "output"


def load_data(file_path: Path) -> pd.DataFrame:
    """Load order data from CSV."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        LOG.error("Failed to load orders: %s", e)
        raise


def clean_data(df: pd.DataFrame, ai_spec: dict | None = None) -> pd.DataFrame:
    """Remove duplicates and fill missing quantity/price."""
    df = df.drop_duplicates().copy()
    if ai_spec is None:
        df["quantity"] = df["quantity"].fillna(1)
        df["price"] = df["price"].fillna(0)
        return df.loc[df["price"] > 0].copy()

    q = float(ai_spec.get("quantity_missing_fill", 1))
    p = float(ai_spec.get("price_missing_fill", 0))
    filt = bool(ai_spec.get("filter_non_positive_price", True))
    df["quantity"] = df["quantity"].fillna(q)
    df["price"] = df["price"].fillna(p)
    if filt:
        return df.loc[df["price"] > 0].copy()
    return df


def transform_data(df: pd.DataFrame):
    """Revenue per category and daily sales counts."""
    df = df.copy()
    df["total"] = df["price"] * df["quantity"]
    revenue = df.groupby("category")["total"].sum().reset_index()
    daily_sales = df.groupby("order_date")["order_id"].count().reset_index()
    daily_sales = daily_sales.rename(columns={"order_id": "order_count"})
    return revenue, daily_sales


def save_data(df: pd.DataFrame, path: Path) -> None:
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


def retail_cleaning_spec(
    summary: str,
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """Get cleaning parameters from AI or use defaults.
    
    Args:
        summary: DataFrame summary string
        user_intent: Natural language intent for cleaning
        model: Ollama model to use
        
    Returns:
        Dict with quantity_missing_fill, price_missing_fill, filter_non_positive_price
    """
    defaults = {"quantity_missing_fill": 1.0, "price_missing_fill": 0.0, "filter_non_positive_price": True}
    intent = user_intent or os.environ.get(
        "PIPELINE_AI_INTENT",
        "Retail revenue by category and daily order counts.",
    )
    
    try:
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        system_msg = (
            "You are a careful data pipeline planner. Reply with ONE JSON object only. "
            "No markdown fences, no commentary outside JSON."
        )
        user_msg = f"""Lab: retail orders CSV.

Dataset summary:
{summary}

User intent:
{intent}

Return JSON with exactly these keys:
- quantity_missing_fill: number (typically 1)
- price_missing_fill: number (typically 0)
- filter_non_positive_price: boolean

Suggested defaults: {defaults}
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)

        q = spec.get("quantity_missing_fill", defaults["quantity_missing_fill"])
        p = spec.get("price_missing_fill", defaults["price_missing_fill"])
        f = spec.get("filter_non_positive_price", defaults["filter_non_positive_price"])

        try:
            q_f = float(q)
        except (TypeError, ValueError):
            q_f = float(defaults["quantity_missing_fill"])
        try:
            p_f = float(p)
        except (TypeError, ValueError):
            p_f = float(defaults["price_missing_fill"])
        if not isinstance(f, bool):
            f = bool(f)

        return {"quantity_missing_fill": q_f, "price_missing_fill": p_f, "filter_non_positive_price": f}
    except Exception:
        return defaults


def run_pipeline(*, use_ai: bool = False, ai_intent: str | None = None) -> None:
    LOG.info("Pipeline started")
    df = load_data(DATA_PATH)
    spec = None
    if use_ai:
        summary = dataframe_summary(df)
        spec = retail_cleaning_spec(summary, ai_intent)
        LOG.info("AI-enabled spec: %s", spec)
    df = clean_data(df, ai_spec=spec)
    revenue, daily = transform_data(df)
    save_data(revenue, OUT_DIR / "revenue.csv")
    save_data(daily, OUT_DIR / "daily_sales.csv")
    LOG.info("Pipeline finished")


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail ETL (optional --ai uses Ollama JSON spec).")
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for cleaning parameters (JSON-only).")
    parser.add_argument("--intent", default=None, help="Natural-language intent forwarded to the model.")
    args = parser.parse_args()
    use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
    run_pipeline(use_ai=use_ai, ai_intent=args.intent)
