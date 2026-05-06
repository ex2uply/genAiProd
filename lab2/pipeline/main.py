"""Healthcare medallion pipeline entrypoint with optional AI watermark."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from bronze.ingest import run_bronze
from gold.aggregate import run_gold
from silver.clean import run_silver

ROOT = Path(__file__).resolve().parent.parent


def load_config() -> dict:
    """Load YAML configuration from config/settings.yaml."""
    cfg_path = Path(__file__).resolve().parent / "config" / "settings.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_csv_header_line(csv_path: Path) -> str:
    """Read the first line (header) from CSV file."""
    with open(csv_path, encoding="utf-8") as f:
        return f.readline().strip()


def lab2_watermark_spec(
    csv_header_line: str,
    current_last_run_date: str,
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """Get watermark date from AI or use default.
    
    Args:
        csv_header_line: Header line from CSV for context
        current_last_run_date: Current watermark from config
        user_intent: Natural language intent for the watermark
        model: Ollama model to use
        
    Returns:
        Dict with last_run_date key
    """
    try:
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        intent = user_intent or os.environ.get(
            "PIPELINE_AI_INTENT",
            "Incremental bronze ingest: filter rows where visit_date > last_run_date.",
        )
        
        system_msg = (
            "You are a careful data pipeline planner. Reply with ONE JSON object only. "
            "No markdown fences, no commentary outside JSON."
        )
        user_msg = f"""PySpark medallion lab (bronze incremental filter).

CSV header line:
{csv_header_line}

Current YAML last_run_date:
{current_last_run_date}

Intent:
{intent}

Return JSON with exactly one key:
- last_run_date: string YYYY-MM-DD

If unsure, use "{current_last_run_date}".
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)
        val = spec.get("last_run_date", current_last_run_date)
        if not isinstance(val, str):
            val = current_last_run_date
        # Validate YYYY-MM-DD format
        parts = val.split("-")
        if len(parts) != 3 or len(parts[0]) != 4 or len(parts[1]) != 2 or len(parts[2]) != 2:
            val = current_last_run_date
        return {"last_run_date": val}
    except Exception:
        return {"last_run_date": current_last_run_date}


def execute(stages: list[str] | None = None, cfg_override: dict | None = None) -> None:
    """Execute the medallion pipeline stages.
    
    Args:
        stages: List of stages to run (bronze, silver, gold). None runs all.
        cfg_override: Optional config overrides from AI
    """
    cfg = dict(load_config())
    if cfg_override:
        cfg.update(cfg_override)
    if stages is None:
        stages = ["bronze", "silver", "gold"]

    spark = SparkSession.builder.appName(cfg["app_name"]).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    csv_path = ROOT / cfg["input_csv"]
    bronze_path = ROOT / cfg["bronze_path"]
    silver_path = ROOT / cfg["silver_path"]
    gold_path = ROOT / cfg["gold_path"]

    try:
        if "bronze" in stages:
            run_bronze(spark, csv_path, bronze_path, cfg["last_run_date"])
        if "silver" in stages:
            run_silver(spark, bronze_path, silver_path)
        if "gold" in stages:
            run_gold(spark, silver_path, gold_path)
        print("Stages completed:", ", ".join(stages))
        if cfg_override:
            print("Effective config overrides:", cfg_override)
    finally:
        spark.stop()


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def main() -> None:
    parser = argparse.ArgumentParser(description="Lab-2 medallion pipeline (optional --ai watermark JSON).")
    parser.add_argument(
        "--stage",
        action="append",
        choices=["bronze", "silver", "gold"],
        help="Run only selected stage(s). Repeat flag for multiple.",
    )
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for last_run_date (JSON-only).")
    parser.add_argument("--intent", default=None, help="Natural-language intent for watermark choice.")
    args = parser.parse_args()

    overlay = None
    use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
    if use_ai:
        cfg = load_config()
        csv_path = ROOT / cfg["input_csv"]
        hdr = read_csv_header_line(csv_path)
        overlay = lab2_watermark_spec(hdr, str(cfg["last_run_date"]), args.intent)
        print("AI-enabled overlay:", overlay)

    stages = args.stage if args.stage else None
    execute(stages, cfg_override=overlay)


if __name__ == "__main__":
    main()
