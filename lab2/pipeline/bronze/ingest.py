"""Bronze: raw CSV ingestion to Parquet with incremental filtering."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def run_bronze(spark: SparkSession, csv_path: Path, bronze_path: Path, last_run_date: str) -> None:
    """Ingest CSV to bronze Parquet with incremental date filter.
    
    Args:
        spark: Active SparkSession
        csv_path: Path to input CSV file
        bronze_path: Output path for bronze parquet
        last_run_date: Watermark date (YYYY-MM-DD) for incremental filter
    """
    bronze_path.mkdir(parents=True, exist_ok=True)
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(str(csv_path))
    except Exception as e:
        raise RuntimeError(f"Error loading CSV into bronze: {e}") from e

    # Incremental filter: only records after last_run_date
    df_incremental = df.filter(col("visit_date") > last_run_date)
    
    # Overwrite mode for reproducible local runs
    df_incremental.write.mode("overwrite").parquet(str(bronze_path))
