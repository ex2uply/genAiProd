"""Tests for logistics pipeline (pure functions, no Prefect runtime needed)."""
import sys
from pathlib import Path

import pandas as pd

LAB_ROOT = Path(__file__).resolve().parent.parent

if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))

import pipeline as prefect_pipeline


def test_data_file_exists():
    """Verify input data file exists."""
    assert (LAB_ROOT / "data" / "shipments.csv").is_file()


def test_clean_shipments():
    """Test pure clean function."""
    df = pd.DataFrame({
        "shipment_id": [1, 2, 3],
        "origin": ["Delhi", "Mumbai", "Bangalore"],
        "destination": ["Mumbai", "Pune", "Chennai"],
        "status": ["Delivered", "Delivered", "Pending"],
        "delivery_time": [2.0, None, 1.0],
    })
    
    cleaned = prefect_pipeline.clean_shipments(df, null_fill=0.0)
    
    assert cleaned["delivery_time"].isna().sum() == 0
    assert cleaned["delivery_time"].iloc[1] == 0.0


def test_avg_delivery_by_destination():
    """Test pure transform function."""
    df = pd.DataFrame({
        "destination": ["Mumbai", "Mumbai", "Pune"],
        "delivery_time": [2.0, 4.0, 1.0],
    })
    
    result = prefect_pipeline.avg_delivery_by_destination(df)
    
    assert "destination" in result.columns
    assert "delivery_time" in result.columns
    
    # Mumbai avg = (2 + 4) / 2 = 3
    mumbai = result[result["destination"] == "Mumbai"]
    assert mumbai["delivery_time"].iloc[0] == 3.0


def test_full_flow():
    """Test complete pipeline flow with pure functions."""
    df = pd.DataFrame({
        "shipment_id": [1, 2, 3, 4],
        "origin": ["Delhi", "Bangalore", "Delhi", "Mumbai"],
        "destination": ["Mumbai", "Chennai", "Kolkata", "Pune"],
        "status": ["Delivered", "Delivered", "Pending", "Delivered"],
        "delivery_time": [2.0, 1.0, None, 1.0],
    })
    
    cleaned = prefect_pipeline.clean_shipments(df, null_fill=0.0)
    result = prefect_pipeline.avg_delivery_by_destination(cleaned)
    
    assert len(result) == 4  # 4 unique destinations
    assert all(result["delivery_time"] >= 0)


def test_ai_dag_spec():
    """Test AI DAG config spec."""
    spec = prefect_pipeline.dag_config_spec("Use mean for nulls and 5 retries for ingest")
    
    assert "null_fill" in spec
    assert "ingest_retries" in spec
    assert "clean_retries" in spec
    assert isinstance(spec["null_fill"], float)
