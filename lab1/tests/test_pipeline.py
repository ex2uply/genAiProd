from pathlib import Path

import pandas as pd

import pipeline as healthcare_pipeline


LAB_ROOT = Path(__file__).resolve().parent.parent


def test_patients_csv_exists():
    assert (LAB_ROOT / "data" / "patients.csv").is_file()


def test_modular_pipeline_writes_expected_outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(healthcare_pipeline, "OUT_DIR", tmp_path)
    monkeypatch.setattr(healthcare_pipeline, "DATA_PATH", LAB_ROOT / "data" / "patients.csv")

    healthcare_pipeline.run_pipeline()

    billing = pd.read_csv(tmp_path / "billing.csv")
    daily = pd.read_csv(tmp_path / "daily.csv")

    billing_totals = dict(zip(billing["diagnosis"], billing["billing_amount"]))
    assert billing_totals["Diabetes"] == 500
    assert billing_totals["Cardiac"] == 500

    daily_counts = dict(zip(daily["visit_date"].astype(str), daily["patient_count"]))
    assert daily_counts["2024-01-01"] == 2
    assert daily_counts["2024-01-02"] == 1
    assert daily_counts["2024-01-03"] == 1


def test_incremental_pattern_example_filter():
    df = pd.read_csv(LAB_ROOT / "data" / "patients.csv")
    incremental = df[df["visit_date"] > "2024-01-01"]
    assert set(incremental["visit_date"].astype(str).tolist()) == {"2024-01-02", "2024-01-03"}


def test_ai_cleaning_spec():
    """Test that AI spec properly configures cleaning parameters."""
    spec = {"age_missing_strategy": "median", "billing_missing_fill": 100.0}
    df = pd.DataFrame({
        "age": [30, None, 50],
        "billing_amount": [200, 300, None],
    })
    df_cleaned = healthcare_pipeline.clean_data(df, ai_spec=spec)
    assert df_cleaned["age"].iloc[1] == 40.0  # median of [30, 50]
    assert df_cleaned["billing_amount"].iloc[2] == 100.0
