"""Part B: modular healthcare ETL — optional Ollama-driven cleaning parameters (JSON-only)."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

DATA_PATH = Path(__file__).resolve().parent / "data" / "patients.csv"
OUT_DIR = Path(__file__).resolve().parent / "output"

LABS_ROOT = Path(__file__).resolve().parent.parent
if str(LABS_ROOT) not in sys.path:
    sys.path.insert(0, str(LABS_ROOT))


def load_data(file_path: Path) -> pd.DataFrame:
    """Load patient data from CSV file."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        raise RuntimeError(f"Error loading data: {e}") from e


def clean_data(df: pd.DataFrame, ai_spec: dict | None = None) -> pd.DataFrame:
    """Clean dataset by removing duplicates and handling missing values."""
    df = df.drop_duplicates().copy()
    if ai_spec is None:
        df["age"] = df["age"].fillna(df["age"].mean())
        df["billing_amount"] = df["billing_amount"].fillna(0)
        return df

    strat = ai_spec.get("age_missing_strategy", "mean")
    fill_v = float(ai_spec.get("billing_missing_fill", 0))
    if strat == "median":
        df["age"] = df["age"].fillna(df["age"].median())
    else:
        df["age"] = df["age"].fillna(df["age"].mean())
    df["billing_amount"] = df["billing_amount"].fillna(fill_v)
    return df


def transform_data(df: pd.DataFrame):
    """Generate business metrics: billing per diagnosis and daily patient counts."""
    billing = df.groupby("diagnosis")["billing_amount"].sum().reset_index()
    daily = df.groupby("visit_date")["patient_id"].count().reset_index()
    daily = daily.rename(columns={"patient_id": "patient_count"})
    return billing, daily


def save_data(df: pd.DataFrame, file_path: Path) -> None:
    """Save DataFrame to CSV."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False)


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


def healthcare_cleaning_spec(
    summary: str,
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """Get cleaning parameters from Ollama or use defaults."""
    defaults = {"age_missing_strategy": "mean", "billing_missing_fill": 0.0}
    intent = user_intent or os.environ.get(
        "PIPELINE_AI_INTENT",
        "Healthcare billing aggregates; handle missing age and billing safely.",
    )

    try:
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        system_msg = (
            "You are a careful data pipeline planner. Reply with ONE JSON object only. "
            "No markdown fences, no commentary outside JSON."
        )
        user_msg = f"""Lab: healthcare patients CSV.

Dataset summary:
{summary}

User intent:
{intent}

Return JSON with exactly these keys:
- age_missing_strategy: string, one of "mean" or "median"
- billing_missing_fill: number (use 0 if unsure)

Suggested defaults: {defaults}
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)

        strat = spec.get("age_missing_strategy", defaults["age_missing_strategy"])
        if strat not in ("mean", "median"):
            strat = defaults["age_missing_strategy"]

        fill = spec.get("billing_missing_fill", defaults["billing_missing_fill"])
        try:
            fill_f = float(fill)
        except (TypeError, ValueError):
            fill_f = float(defaults["billing_missing_fill"])

        return {"age_missing_strategy": strat, "billing_missing_fill": fill_f}
    except Exception:
        return defaults


def run_pipeline(*, use_ai: bool = False, ai_intent: str | None = None) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        df = load_data(DATA_PATH)
        spec = None
        if use_ai:
            summary = dataframe_summary(df)
            spec = healthcare_cleaning_spec(summary, ai_intent)
            print("AI-enabled spec:", spec)
        df = clean_data(df, ai_spec=spec)
        billing, daily = transform_data(df)
        save_data(billing, OUT_DIR / "billing.csv")
        save_data(daily, OUT_DIR / "daily.csv")
        print("Pipeline completed successfully")
    except Exception as e:
        print("Pipeline failed:", e)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Healthcare ETL (optional --ai uses Ollama JSON spec).")
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for cleaning parameters (JSON-only).")
    parser.add_argument("--intent", default=None, help="Natural-language intent forwarded to the model.")
    args = parser.parse_args()
    use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
    run_pipeline(use_ai=use_ai, ai_intent=args.intent)
