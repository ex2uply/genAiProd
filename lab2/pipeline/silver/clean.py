"""Silver: clean bronze data with schema merge support."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean


def run_silver(spark: SparkSession, bronze_path: Path, silver_path: Path) -> None:
    """Clean bronze data and write to silver layer.
    
    Cleaning steps:
    - Fill missing billing_amount with 0
    - Fill missing age with mean age
    - Drop duplicates by patient_id
    - Partition by visit_date
    
    Args:
        spark: Active SparkSession
        bronze_path: Input bronze parquet path
        silver_path: Output silver parquet path
    """
    silver_path.mkdir(parents=True, exist_ok=True)
    
    # Read with schema merging for evolving schemas
    df = spark.read.option("mergeSchema", True).parquet(str(bronze_path))
    
    # Fill missing billing amounts with 0
    df_clean = df.fillna({"billing_amount": 0})
    
    # Fill missing age with mean
    age_mean_row = df_clean.select(mean(col("age")).alias("m")).collect()[0]["m"]
    if age_mean_row is not None:
        df_clean = df_clean.fillna({"age": age_mean_row})
    
    # Deduplicate by patient_id (keeps first occurrence)
    df_clean = df_clean.dropDuplicates(["patient_id"])
    
    # Write partitioned by visit_date for efficient querying
    df_clean.write.mode("overwrite").partitionBy("visit_date").parquet(str(silver_path))
