"""
Lab 5 — Telecom CDC Pipeline with Watermark (Pandas)

This pipeline demonstrates Change Data Capture (CDC) with incremental loading:
- Reads a watermark file to track last processed date
- Loads only new records since the watermark
- Updates the watermark after successful processing
- Supports AI-driven watermark configuration
"""
import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parent
DATA_DIR = LAB_ROOT / "data"
OUTPUT_DIR = LAB_ROOT / "output"
WATERMARK_FILE = LAB_ROOT / "watermark.txt"


def _env_truthy(name: str) -> bool:
    """Check if environment variable is truthy."""
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def read_watermark() -> str:
    """Read the current watermark date from file."""
    if not WATERMARK_FILE.exists():
        return "1970-01-01"
    return WATERMARK_FILE.read_text().strip()


def write_watermark(date_str: str) -> None:
    """Write new watermark date to file."""
    WATERMARK_FILE.write_text(date_str)


def load_data(watermark: str) -> pd.DataFrame:
    """
    Load all CSV files and filter records newer than watermark.
    
    Args:
        watermark: ISO date string (YYYY-MM-DD)
        
    Returns:
        DataFrame with records where call_date > watermark
    """
    files = sorted(DATA_DIR.glob("cdr_day*.csv"))
    if not files:
        LOG.error("No data files found in %s", DATA_DIR)
        raise FileNotFoundError(f"No cdr_day*.csv files in {DATA_DIR}")
    
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
            LOG.info("Loaded %s: %d rows", f.name, len(df))
        except Exception as e:
            LOG.error("Error loading %s: %s", f.name, e)
    
    combined = pd.concat(dfs, ignore_index=True)
    
    # Filter records newer than watermark
    filtered = combined[combined["call_date"] > watermark]
    LOG.info("Filtered %d records newer than %s", len(filtered), watermark)
    
    return filtered


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform CDR data: calculate total duration per caller.
    
    Args:
        df: Input DataFrame with CDR records
        
    Returns:
        Aggregated DataFrame with total duration per caller
    """
    if df.empty:
        return pd.DataFrame(columns=["caller", "total_duration", "call_count"])
    
    result = df.groupby("caller").agg(
        total_duration=("duration", "sum"),
        call_count=("call_id", "count")
    ).reset_index()
    
    return result


def save_data(df: pd.DataFrame, path: Path) -> None:
    """Save DataFrame to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    LOG.info("Saved output to %s", path)


def dataframe_summary(df: pd.DataFrame) -> str:
    """Generate DataFrame summary for AI context."""
    lines = [
        f"rows={len(df)} cols={len(df.columns)}",
        "columns: " + ", ".join(df.columns.astype(str).tolist()),
        "date range: " + (f"{df['call_date'].min()} to {df['call_date'].max()}" if not df.empty and 'call_date' in df.columns else "N/A"),
    ]
    return "\n".join(lines)


def cdc_watermark_spec(
    summary: str,
    current_watermark: str,
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """
    Get CDC watermark configuration from AI or use defaults.
    
    Args:
        summary: DataFrame summary string
        current_watermark: Current watermark date
        user_intent: Natural language intent for CDC
        model: Ollama model to use
        
    Returns:
        Dict with watermark_date and processing_strategy
    """
    defaults = {
        "watermark_date": current_watermark,
        "processing_strategy": "incremental",
        "lookback_days": 0,
    }
    
    intent = user_intent or os.environ.get(
        "PIPELINE_AI_INTENT",
        "Process only new records since last watermark",
    )
    
    try:
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        system_msg = (
            "You are a careful data pipeline planner. Reply with ONE JSON object only. "
            "No markdown fences, no commentary outside JSON."
        )
        user_msg = f"""Lab: Telecom CDC pipeline with watermark.

Dataset summary:
{summary}

Current watermark: {current_watermark}

User intent:
{intent}

Return JSON with exactly these keys:
- watermark_date: "YYYY-MM-DD" format (suggested new watermark after processing)
- processing_strategy: "incremental" or "full_reload"
- lookback_days: number (days to look back, 0 for strict incremental)

Suggested defaults: {defaults}
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)

        return {
            "watermark_date": spec.get("watermark_date", defaults["watermark_date"]),
            "processing_strategy": spec.get("processing_strategy", defaults["processing_strategy"]),
            "lookback_days": int(spec.get("lookback_days", defaults["lookback_days"])),
        }
    except Exception as e:
        LOG.warning("AI spec failed, using defaults: %s", e)
        return defaults


def run_pipeline(*, use_ai: bool = False, ai_intent: str | None = None) -> None:
    """
    Run the telecom CDC pipeline.
    
    Args:
        use_ai: Whether to use AI for watermark configuration
        ai_intent: Natural language intent for AI
    """
    LOG.info("CDC Pipeline started")
    
    # Read current watermark
    current_watermark = read_watermark()
    LOG.info("Current watermark: %s", current_watermark)
    
    # Get AI configuration if enabled
    if use_ai:
        # Load sample data for AI context
        sample_df = load_data(current_watermark)
        summary = dataframe_summary(sample_df)
        spec = cdc_watermark_spec(summary, current_watermark, ai_intent)
        LOG.info("AI-enabled spec: %s", spec)
        
        # Use AI-suggested watermark if different
        if spec["processing_strategy"] == "full_reload":
            LOG.info("AI requested full reload")
            current_watermark = "1970-01-01"
        elif spec["lookback_days"] > 0:
            # Lookback mode - adjust watermark
            from datetime import timedelta
            lookback_date = datetime.now() - timedelta(days=spec["lookback_days"])
            current_watermark = lookback_date.strftime("%Y-%m-%d")
            LOG.info("AI lookback mode: %s", current_watermark)
    
    # Load incremental data
    df = load_data(current_watermark)
    
    if df.empty:
        LOG.info("No new records to process")
        return
    
    # Transform
    result = transform_data(df)
    
    # Save output
    output_path = OUTPUT_DIR / "caller_summary.csv"
    save_data(result, output_path)
    
    # Update watermark to latest processed date
    latest_date = df["call_date"].max()
    write_watermark(latest_date)
    LOG.info("Updated watermark to: %s", latest_date)
    
    LOG.info("Pipeline finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Telecom CDC Pipeline (optional --ai uses Ollama for watermark config)."
    )
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for CDC configuration.")
    parser.add_argument("--intent", default=None, help="Natural-language intent for AI.")
    args = parser.parse_args()
    
    use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
    run_pipeline(use_ai=use_ai, ai_intent=args.intent)
