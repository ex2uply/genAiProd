"""Tests for telecom CDC pipeline."""
import sys
from pathlib import Path

import pandas as pd

LAB_ROOT = Path(__file__).resolve().parent.parent

if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))

import pipeline as cdc_pipeline


def test_watermark_read_write():
    """Test watermark file operations."""
    # Save original
    original = cdc_pipeline.read_watermark()
    
    # Test write and read
    cdc_pipeline.write_watermark("2024-01-15")
    assert cdc_pipeline.read_watermark() == "2024-01-15"
    
    # Restore original
    cdc_pipeline.write_watermark(original)


def test_data_files_exist():
    """Verify input data files exist."""
    assert (LAB_ROOT / "data" / "cdr_day1.csv").is_file()
    assert (LAB_ROOT / "data" / "cdr_day2.csv").is_file()


def test_load_data_filters_by_watermark():
    """Test that load_data filters records newer than watermark."""
    # With watermark 2024-01-01, should get all records from day1 and day2
    df = cdc_pipeline.load_data("2024-01-01")
    assert len(df) > 0
    assert all(df["call_date"] > "2024-01-01")


def test_transform_data():
    """Test data transformation."""
    df = pd.DataFrame({
        "call_id": [1, 2, 3, 4],
        "caller": [1001, 1002, 1001, 1003],
        "callee": [2001, 2002, 2003, 2004],
        "duration": [120, 60, 180, 90],
        "call_date": ["2024-01-02"] * 4,
    })
    
    result = cdc_pipeline.transform_data(df)
    
    assert "caller" in result.columns
    assert "total_duration" in result.columns
    assert "call_count" in result.columns
    
    # Check aggregation
    caller_1001 = result[result["caller"] == 1001]
    assert caller_1001["total_duration"].iloc[0] == 300  # 120 + 180
    assert caller_1001["call_count"].iloc[0] == 2


def test_pipeline_writes_output(tmp_path, monkeypatch):
    """Test full pipeline execution."""
    monkeypatch.setattr(cdc_pipeline, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(cdc_pipeline, "WATERMARK_FILE", tmp_path / "watermark.txt")
    
    # Set initial watermark
    cdc_pipeline.write_watermark("2024-01-01")
    
    cdc_pipeline.run_pipeline()
    
    assert (tmp_path / "caller_summary.csv").is_file()


def test_ai_watermark_spec():
    """Test AI watermark spec returns valid parameters."""
    df = pd.DataFrame({
        "call_id": [1, 2],
        "caller": [1001, 1002],
        "call_date": ["2024-01-02", "2024-01-03"],
    })
    summary = cdc_pipeline.dataframe_summary(df)
    spec = cdc_pipeline.cdc_watermark_spec(summary, "2024-01-01", "Process with 2 day lookback")
    
    assert "watermark_date" in spec
    assert "processing_strategy" in spec
    assert "lookback_days" in spec
