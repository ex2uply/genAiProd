"""Tests for PySpark optimization pipeline."""
import sys
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent

if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))

import pipeline as spark_pipeline


def test_data_files_exist():
    """Verify input data files exist."""
    assert (LAB_ROOT / "data" / "rides.csv").is_file()
    assert (LAB_ROOT / "data" / "drivers.csv").is_file()


def test_spark_session_creation():
    """Test Spark session can be created."""
    spark = spark_pipeline.create_spark_session("TestApp")
    assert spark is not None
    spark.stop()


def test_pipeline_runs():
    """Test full pipeline execution."""
    spark_pipeline.run_pipeline(target_city="Bangalore", use_broadcast=True, repartition_count=2)
    
    # Check output exists
    out_dir = LAB_ROOT / "output" / "partitioned_final"
    assert out_dir.exists()


def test_baseline_runs():
    """Test baseline pipeline execution."""
    spark_pipeline.run_baseline()
    
    # Check output exists
    out_dir = LAB_ROOT / "output" / "baseline"
    assert out_dir.exists()


def test_ai_optimization_spec():
    """Test AI optimization spec returns valid parameters."""
    spec = spark_pipeline.optimization_spec("Analyze Delhi with 8 partitions")
    
    assert "target_city" in spec
    assert "use_broadcast" in spec
    assert "repartition_count" in spec
    assert isinstance(spec["use_broadcast"], bool)
    assert isinstance(spec["repartition_count"], int)
