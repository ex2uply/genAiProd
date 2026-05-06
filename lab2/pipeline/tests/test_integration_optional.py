"""Optional integration tests requiring Spark run."""
import os
from pathlib import Path

import pytest

LAB_ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.mark.integration
def test_gold_parquet_exists_after_pipeline_run():
    """Verify gold parquet output exists (requires pipeline run first)."""
    if os.getenv("RUN_LAB2_SPARK_INTEGRATION") != "1":
        pytest.skip("Set env var RUN_LAB2_SPARK_INTEGRATION=1 after running `python main.py` once.")

    gold_dir = LAB_ROOT / "output" / "gold"
    parquet_files = list(gold_dir.glob("**/*.parquet"))
    if not parquet_files:
        pytest.fail("Gold parquet not found — run `python main.py` once from `pipeline` folder.")

    assert len(parquet_files) > 0
