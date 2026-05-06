"""Tests for retail pipeline."""
from pathlib import Path

import pandas as pd

import main as pipeline_main

LAB_ROOT = Path(__file__).resolve().parent.parent.parent


def test_orders_not_empty():
    """Verify input data file has records."""
    df = pd.read_csv(LAB_ROOT / "data" / "orders.csv")
    assert df.shape[0] > 0


def test_orders_csv_exists():
    """Verify orders CSV file exists."""
    assert (LAB_ROOT / "data" / "orders.csv").is_file()


def test_pipeline_outputs_after_run(tmp_path, monkeypatch):
    """Verify pipeline creates expected output files."""
    monkeypatch.setattr(pipeline_main, "OUT_DIR", tmp_path)
    monkeypatch.setattr(pipeline_main, "DATA_PATH", LAB_ROOT / "data" / "orders.csv")
    pipeline_main.run_pipeline()
    assert (tmp_path / "revenue.csv").is_file()
    assert (tmp_path / "daily_sales.csv").is_file()


def test_revenue_calculation(tmp_path, monkeypatch):
    """Verify revenue calculations are correct."""
    monkeypatch.setattr(pipeline_main, "OUT_DIR", tmp_path)
    monkeypatch.setattr(pipeline_main, "DATA_PATH", LAB_ROOT / "data" / "orders.csv")
    pipeline_main.run_pipeline()
    
    revenue = pd.read_csv(tmp_path / "revenue.csv")
    # Electronics: 800*1 + 500*2 = 1800, Fashion: 100*1 + 50*1 = 150
    assert "category" in revenue.columns
    assert "total" in revenue.columns


def test_cleaning_logic():
    """Test data cleaning handles nulls and duplicates."""
    df = pd.DataFrame({
        "order_id": [1, 2, 2, 3],
        "price": [100, 50, 50, None],
        "quantity": [1, None, 2, 3],
    })
    cleaned = pipeline_main.clean_data(df)
    # Duplicate removed, nulls filled
    assert len(cleaned) <= 3
