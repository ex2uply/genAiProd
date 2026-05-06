"""Tests for schema evolution pipeline."""
import sys
from pathlib import Path

import pandas as pd

LAB_ROOT = Path(__file__).resolve().parent.parent

if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))

import pipeline as schema_pipeline


def test_day_snapshots_exist():
    """Verify all daily snapshots exist."""
    assert (LAB_ROOT / "data" / "products_day1.csv").is_file()
    assert (LAB_ROOT / "data" / "products_day2.csv").is_file()
    assert (LAB_ROOT / "data" / "products_day3.csv").is_file()


def test_merge_schema_unions_columns():
    """Test that merge_schema unions all columns."""
    df1 = schema_pipeline.load(LAB_ROOT / "data" / "products_day1.csv")
    df2 = schema_pipeline.load(LAB_ROOT / "data" / "products_day2.csv")
    df3 = schema_pipeline.load(LAB_ROOT / "data" / "products_day3.csv")
    
    merged = schema_pipeline.merge_schema([df1, df2, df3])
    cleaned = schema_pipeline.clean(merged)
    
    # Should have all columns from all days
    assert set(cleaned.columns) == {"product_id", "name", "category", "price", "brand", "discount"}
    
    # Missing values should be filled
    assert cleaned["brand"].isna().sum() == 0
    assert cleaned["discount"].isna().sum() == 0
    
    # Day 1 records should have Unknown brand and 0 discount
    day1_products = cleaned[cleaned["product_id"].isin([1, 2, 3])]
    assert all(day1_products["brand"] == "Unknown")
    assert all(day1_products["discount"] == 0.0)


def test_pipeline_writes_final_products_csv(tmp_path, monkeypatch):
    """Test full pipeline execution."""
    monkeypatch.setattr(schema_pipeline, "OUTPUT_DIR", tmp_path)
    
    schema_pipeline.run_pipeline()
    
    out = tmp_path / "final_products.csv"
    assert out.is_file()
    
    # Verify content
    df = pd.read_csv(out)
    assert len(df) == 6  # 3 + 2 + 1 products
    assert set(df.columns) == {"product_id", "name", "category", "price", "brand", "discount"}


def test_ai_schema_spec():
    """Test AI schema spec returns valid parameters."""
    dfs = [
        pd.DataFrame({"a": [1], "b": [2]}),
        pd.DataFrame({"a": [3], "c": [4]}),
    ]
    summary = schema_pipeline.dataframe_summary(dfs)
    spec = schema_pipeline.schema_evolution_spec(summary, "Use 'N/A' for brand, 0.5 for discount")
    
    assert "default_brand" in spec
    assert "default_discount" in spec
    assert isinstance(spec["default_discount"], float)
