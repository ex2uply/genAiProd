"""
Lab 6 — Schema Evolution Pipeline (Pandas)

This pipeline demonstrates handling schema changes over time:
- Day 1: product_id, name, category, price
- Day 2: adds 'brand' column
- Day 3: adds 'discount' column

The pipeline merges all schemas and fills missing values.
"""
import argparse
import logging
import os
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parent
DATA_DIR = LAB_ROOT / "data"
OUTPUT_DIR = LAB_ROOT / "output"


def _env_truthy(name: str) -> bool:
    """Check if environment variable is truthy."""
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def load(path: Path) -> pd.DataFrame:
    """Load CSV file."""
    return pd.read_csv(path)


def merge_schema(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge multiple DataFrames with different schemas.
    Union all columns and fill missing with NA.
    
    Args:
        dfs: List of DataFrames with potentially different columns
        
    Returns:
        Merged DataFrame with all columns from all inputs
    """
    # Get union of all columns
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    
    # Align each DataFrame to have all columns
    aligned = []
    for df in dfs:
        for col in all_columns:
            if col not in df.columns:
                df = df.copy()
                df[col] = pd.NA
        aligned.append(df)
    
    return pd.concat(aligned, ignore_index=True)


def clean(df: pd.DataFrame, default_brand: str = "Unknown", default_discount: float = 0.0) -> pd.DataFrame:
    """
    Clean merged data by filling nulls with defaults.
    
    Args:
        df: Merged DataFrame with potential nulls
        default_brand: Value to fill missing brands
        default_discount: Value to fill missing discounts
        
    Returns:
        Cleaned DataFrame
    """
    df = df.copy()
    
    if "brand" in df.columns:
        df["brand"] = df["brand"].fillna(default_brand)
    
    if "discount" in df.columns:
        df["discount"] = df["discount"].fillna(default_discount)
    
    return df


def run_pipeline(
    default_brand: str = "Unknown",
    default_discount: float = 0.0,
) -> None:
    """
    Run the schema evolution pipeline.
    
    Args:
        default_brand: Default value for missing brand
        default_discount: Default value for missing discount
    """
    LOG.info("Schema Evolution Pipeline started")
    
    # Load all daily snapshots
    files = sorted(DATA_DIR.glob("products_day*.csv"))
    LOG.info("Found %d daily snapshots", len(files))
    
    dfs = [load(f) for f in files]
    
    # Show schema evolution
    for i, df in enumerate(dfs, 1):
        LOG.info("Day %d columns: %s", i, list(df.columns))
    
    # Merge schemas
    merged = merge_schema(dfs)
    LOG.info("Merged schema with columns: %s", list(merged.columns))
    
    # Clean
    cleaned = clean(merged, default_brand, default_discount)
    LOG.info("Cleaned data: %d rows", len(cleaned))
    
    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "final_products.csv"
    cleaned.to_csv(output_path, index=False)
    LOG.info("Saved to %s", output_path)
    
    LOG.info("Pipeline finished")


def dataframe_summary(dfs: list[pd.DataFrame]) -> str:
    """Generate summary of all DataFrames for AI context."""
    lines = []
    for i, df in enumerate(dfs, 1):
        lines.append(f"Day {i}: rows={len(df)}, columns={list(df.columns)}")
    return "\n".join(lines)


def schema_evolution_spec(
    summary: str,
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """
    Get schema evolution defaults from AI or use defaults.
    
    Args:
        summary: DataFrames summary string
        user_intent: Natural language intent
        model: Ollama model to use
        
    Returns:
        Dict with default_brand and default_discount
    """
    defaults = {
        "default_brand": "Unknown",
        "default_discount": 0.0,
    }
    
    intent = user_intent or os.environ.get(
        "PIPELINE_AI_INTENT",
        "Fill missing schema values appropriately",
    )
    
    try:
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        system_msg = (
            "You are a careful data pipeline planner. Reply with ONE JSON object only. "
            "No markdown fences, no commentary outside JSON."
        )
        user_msg = f"""Lab: Schema evolution with daily product snapshots.

Schema evolution:
{summary}

User intent:
{intent}

Return JSON with exactly these keys:
- default_brand: string for missing brands (default: "Unknown")
- default_discount: number for missing discounts (default: 0)

Suggested defaults: {defaults}
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)

        return {
            "default_brand": spec.get("default_brand", defaults["default_brand"]),
            "default_discount": float(spec.get("default_discount", defaults["default_discount"])),
        }
    except Exception as e:
        LOG.warning("AI spec failed, using defaults: %s", e)
        return defaults


def run_pipeline_ai(*, use_ai: bool = False, ai_intent: str | None = None) -> None:
    """
    Run pipeline with optional AI configuration.
    
    Args:
        use_ai: Whether to use AI for defaults
        ai_intent: Natural language intent for AI
    """
    default_brand = "Unknown"
    default_discount = 0.0
    
    if use_ai:
        files = sorted(DATA_DIR.glob("products_day*.csv"))
        dfs = [load(f) for f in files]
        summary = dataframe_summary(dfs)
        spec = schema_evolution_spec(summary, ai_intent)
        LOG.info("AI-enabled spec: %s", spec)
        default_brand = spec["default_brand"]
        default_discount = spec["default_discount"]
    
    run_pipeline(default_brand, default_discount)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Schema Evolution Pipeline (optional --ai uses Ollama for defaults)."
    )
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for default values.")
    parser.add_argument("--intent", default=None, help="Natural-language intent for AI.")
    args = parser.parse_args()
    
    use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
    run_pipeline_ai(use_ai=use_ai, ai_intent=args.intent)
