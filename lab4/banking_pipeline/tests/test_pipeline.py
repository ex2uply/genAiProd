"""Smoke test for banking pipeline."""
import sys
from pathlib import Path

import pandas as pd

LAB_ROOT = Path(__file__).resolve().parent.parent.parent

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import main as banking_main  # noqa: E402


def test_data_loaded():
    """Verify input data loads correctly."""
    df = pd.read_csv(LAB_ROOT / "data" / "transactions.csv")
    assert df.shape[0] > 0


def test_transactions_csv_exists():
    """Verify transactions CSV file exists."""
    assert (LAB_ROOT / "data" / "transactions.csv").is_file()


def test_pipeline_writes_outputs(tmp_path, monkeypatch):
    """Verify pipeline creates expected outputs."""
    monkeypatch.setattr(banking_main, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(banking_main, "DATA_PATH", LAB_ROOT / "data" / "transactions.csv")

    banking_main.run_pipeline()

    clean_df = pd.read_csv(tmp_path / "clean.csv")
    agg_df = pd.read_csv(tmp_path / "agg.csv")

    assert "is_fraud" in clean_df.columns
    assert clean_df.duplicated().sum() == 0

    totals = dict(zip(agg_df["account_id"], agg_df["amount"]))
    assert totals[1001] == 500
    assert totals[1002] == 200
    assert totals[1003] == 1000


def test_fraud_detection():
    """Test fraud detection logic."""
    df = pd.DataFrame({
        "transaction_id": [1, 2, 3],
        "amount": [500, 1000, 200],
    })
    result = banking_main.detect_fraud(df, threshold=800)
    assert "is_fraud" in result.columns
    assert result["is_fraud"].tolist() == [False, True, False]


def test_clean_data():
    """Test data cleaning handles nulls and duplicates."""
    df = pd.DataFrame({
        "transaction_id": [1, 2, 2, 3],
        "amount": [100, None, None, 200],
    })
    cleaned = banking_main.clean_data(df, missing_fill=0)
    # Duplicate removed, nulls filled with 0
    assert len(cleaned) == 3
    assert cleaned["amount"].isna().sum() == 0


def test_ai_fraud_spec():
    """Test AI fraud spec returns valid parameters."""
    df = pd.DataFrame({
        "amount": [100, 500, 1000],
        "account_id": [1, 2, 3],
    })
    summary = banking_main.dataframe_summary(df)
    spec = banking_main.banking_fraud_spec(summary, "Flag transactions above 500 as fraud")
    
    assert "fraud_threshold" in spec
    assert "missing_fill" in spec
    assert isinstance(spec["fraud_threshold"], (int, float))
    assert isinstance(spec["missing_fill"], (int, float))
