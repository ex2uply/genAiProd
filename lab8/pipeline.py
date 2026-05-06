"""
Lab 8 — Auto DAG Pipeline with Prefect (Logistics)

This pipeline demonstrates workflow orchestration with Prefect:
- Task retries with exponential backoff
- Pure functions for testability
- Flow composition
- AI-driven retry and timeout configuration
"""
import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parent
DATA_PATH = LAB_ROOT / "data" / "shipments.csv"
OUTPUT_PATH = LAB_ROOT / "output" / "avg_delivery_by_destination.csv"


def _env_truthy(name: str) -> bool:
    """Check if environment variable is truthy."""
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


# Pure functions (testable without Prefect)
def clean_shipments(df: pd.DataFrame, null_fill: float = 0.0) -> pd.DataFrame:
    """
    Clean shipments data by filling null delivery times.
    
    Args:
        df: Input DataFrame
        null_fill: Value to fill null delivery times
        
    Returns:
        Cleaned DataFrame
    """
    df = df.copy()
    df["delivery_time"] = df["delivery_time"].fillna(null_fill)
    return df


def avg_delivery_by_destination(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate average delivery time by destination.
    
    Args:
        df: Input DataFrame with destination and delivery_time columns
        
    Returns:
        Aggregated DataFrame with mean delivery time per destination
    """
    return df.groupby("destination")["delivery_time"].mean().reset_index()


# Prefect tasks
@task(
    retries=3,
    retry_delay_seconds=5,
    cache_key_fn=task_input_hash,
    cache_expiration_seconds=3600,
)
def ingest_data(data_path: Path) -> pd.DataFrame:
    """
    Ingest shipments data from CSV.
    
    Args:
        data_path: Path to shipments CSV
        
    Returns:
        DataFrame with shipments data
    """
    LOG.info("Ingesting data from %s", data_path)
    return pd.read_csv(data_path)


@task(retries=2, retry_delay_seconds=3)
def clean_data(df: pd.DataFrame, null_fill: float = 0.0) -> pd.DataFrame:
    """
    Task wrapper for clean_shipments.
    
    Args:
        df: Input DataFrame
        null_fill: Value to fill nulls
        
    Returns:
        Cleaned DataFrame
    """
    LOG.info("Cleaning data: filling nulls with %s", null_fill)
    return clean_shipments(df, null_fill)


@task(retries=1, retry_delay_seconds=2)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task wrapper for avg_delivery_by_destination.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Aggregated DataFrame
    """
    LOG.info("Transforming data: aggregating by destination")
    return avg_delivery_by_destination(df)


@task
def save_data(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save DataFrame to CSV.
    
    Args:
        df: DataFrame to save
        output_path: Output file path
    """
    LOG.info("Saving data to %s", output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    LOG.info("Saved %d rows", len(df))


# Prefect flow
@flow(name="logistics-pipeline", log_prints=True)
def logistics_pipeline(null_fill: float = 0.0) -> None:
    """
    Logistics pipeline flow: ingest → clean → transform → save.
    
    Args:
        null_fill: Value to fill null delivery times
    """
    LOG.info("Flow started")
    
    # Ingest
    data = ingest_data(DATA_PATH)
    
    # Clean
    cleaned = clean_data(data, null_fill)
    
    # Transform
    result = transform_data(cleaned)
    
    # Save
    save_data(result, OUTPUT_PATH)
    
    LOG.info("Flow finished")


def dag_config_spec(
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """
    Get DAG configuration from AI.
    
    Args:
        user_intent: Natural language intent
        model: Ollama model
        
    Returns:
        Dict with null_fill and retry config
    """
    defaults = {
        "null_fill": 0.0,
        "ingest_retries": 3,
        "clean_retries": 2,
    }
    
    intent = user_intent or os.environ.get(
        "PIPELINE_AI_INTENT",
        "Standard logistics pipeline with retries",
    )
    
    try:
        import sys
        sys.path.insert(0, str(LAB_ROOT))
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        system_msg = (
            "You are a workflow orchestration expert. Reply with ONE JSON object only. "
            "No markdown fences, no commentary."
        )
        user_msg = f"""Lab: Logistics pipeline with Prefect orchestration.

Data: shipments.csv with delivery_time column (may have nulls)

User intent:
{intent}

Return JSON with exactly these keys:
- null_fill: number (default 0, or use mean/median for null delivery times)
- ingest_retries: number (3 for network ops)
- clean_retries: number (2 for data cleaning)

Suggested defaults: {defaults}
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)

        return {
            "null_fill": float(spec.get("null_fill", defaults["null_fill"])),
            "ingest_retries": int(spec.get("ingest_retries", defaults["ingest_retries"])),
            "clean_retries": int(spec.get("clean_retries", defaults["clean_retries"])),
        }
    except Exception as e:
        LOG.warning("AI spec failed, using defaults: %s", e)
        return defaults


def run_pipeline(*, use_ai: bool = False, ai_intent: str | None = None) -> None:
    """
    Run pipeline with optional AI configuration.
    
    Args:
        use_ai: Whether to use AI for config
        ai_intent: Natural language intent
    """
    if use_ai:
        spec = dag_config_spec(ai_intent)
        LOG.info("AI-enabled spec: %s", spec)
        logistics_pipeline(null_fill=spec["null_fill"])
    else:
        logistics_pipeline()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Logistics Auto DAG with Prefect (optional --ai uses Ollama)."
    )
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for DAG config.")
    parser.add_argument("--intent", default=None, help="Natural-language intent for AI.")
    args = parser.parse_args()
    
    use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
    run_pipeline(use_ai=use_ai, ai_intent=args.intent)
