"""Smart agriculture medallion pipeline (PySpark + watermark + alerts + AI).

Features:
- Bronze: Watermark-based CDC with unionByName schema evolution
- Silver: Data cleaning, partitioning, mergeSchema
- Gold: Broadcast join with fields, AI-driven alert thresholds
- AI Integration: Runtime alert threshold configuration via Ollama
"""
import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, sum as spark_sum, when

# Add parent to path for genai imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from genai.ollama_client import ask_alert_config
except Exception:
    ask_alert_config = None

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def load_config() -> dict:
    cfg_path = Path(__file__).resolve().parent / "config.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_watermark(path: Path, default: str) -> None:
    if not path.is_file():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(default, encoding="utf-8")


def read_watermark(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def write_watermark(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def get_alert_config(ai_intent: str | None = None) -> dict:
    """Get alert thresholds from AI or defaults.

    Args:
        ai_intent: Natural language intent for AI configuration

    Returns:
        Dictionary with high_temp_threshold and low_moisture_threshold
    """
    default = {"high_temp_threshold": 35, "low_moisture_threshold": 40}

    if not ai_intent or not ask_alert_config:
        return default

    LOG.info("Requesting AI alert configuration...")
    try:
        return ask_alert_config(ai_intent, default)
    except Exception as e:
        LOG.warning(f"AI config failed, using defaults: {e}")
        return default


def run(cfg: dict, stages: list[str] | None = None, ai_intent: str | None = None) -> dict:
    """Run the medallion pipeline.

    Args:
        cfg: Configuration dictionary
        stages: List of stages to run (default: all)
        ai_intent: Natural language intent for AI-driven thresholds

    Returns:
        Dictionary with alert config used
    """
    if stages is None:
        stages = ["bronze", "silver", "gold"]

    # Get AI-driven alert configuration
    alert_config = get_alert_config(ai_intent)
    LOG.info(f"Alert thresholds - High Temp: {alert_config['high_temp_threshold']}, "
             f"Low Moisture: {alert_config['low_moisture_threshold']}")

    spark = SparkSession.builder.appName(cfg["app_name"]).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    data_dir = ROOT / cfg["data_dir"]
    bronze_path = ROOT / cfg["bronze_path"]
    silver_path = ROOT / cfg["silver_path"]
    gold_path = ROOT / cfg["gold_path"]
    watermark_path = ROOT / cfg["watermark_path"]

    ensure_watermark(watermark_path, cfg["initial_watermark"])

    try:
        if "bronze" in stages:
            LOG.info("Running Bronze stage...")
            bronze_path.mkdir(parents=True, exist_ok=True)
            df1 = spark.read.option("header", True).option("inferSchema", True).csv(str(data_dir / "sensor_day1.csv"))
            df2 = spark.read.option("header", True).option("inferSchema", True).csv(str(data_dir / "sensor_day2.csv"))
            # Schema evolution: unionByName with allowMissingColumns
            combined = df1.unionByName(df2, allowMissingColumns=True)
            last_wm = read_watermark(watermark_path)
            incremental = combined.filter(col("timestamp") > last_wm)
            incremental.write.mode("overwrite").parquet(str(bronze_path))
            LOG.info(f"Bronze: Wrote incremental data to {bronze_path}")

        if "silver" in stages:
            LOG.info("Running Silver stage...")
            silver_path.mkdir(parents=True, exist_ok=True)
            # mergeSchema for schema evolution
            df = spark.read.option("mergeSchema", True).parquet(str(bronze_path))
            # Clean data with defaults
            df_clean = df.fillna({"moisture": 0, "temperature": 0, "humidity": 0})
            df_clean = df_clean.filter(col("temperature") > 0)
            # Partition by field_id for query optimization
            df_clean.write.mode("overwrite").partitionBy("field_id").parquet(str(silver_path))
            LOG.info(f"Silver: Wrote cleaned data to {silver_path}")

        if "gold" in stages:
            LOG.info("Running Gold stage...")
            gold_path.mkdir(parents=True, exist_ok=True)
            df_clean = spark.read.parquet(str(silver_path))
            fields = spark.read.option("header", True).option("inferSchema", True).csv(str(data_dir / "fields.csv"))

            # Broadcast join for small dimension table
            enriched = df_clean.join(broadcast(fields), "field_id", "left")

            # AI-driven alert logic using thresholds
            high_temp = alert_config["high_temp_threshold"]
            low_moisture = alert_config["low_moisture_threshold"]

            alerts = enriched.withColumn(
                "alert",
                when(col("temperature") > high_temp, "High Temp")
                .when(col("moisture") < low_moisture, "Low Moisture")
                .otherwise("Normal"),
            )

            # Aggregate metrics
            summary = alerts.groupBy("field_id").agg(
                avg("temperature").alias("avg_temperature"),
                avg("moisture").alias("avg_moisture"),
                avg("humidity").alias("avg_humidity"),
                spark_sum(when(col("alert") != "Normal", 1).otherwise(0)).alias("alert_events"),
            )

            alerts.write.mode("overwrite").parquet(str(gold_path / "alerts"))
            summary.write.mode("overwrite").parquet(str(gold_path / "field_summary"))

            # Update watermark
            max_ts = enriched.selectExpr("max(timestamp) as m").collect()[0]["m"]
            if max_ts is not None:
                write_watermark(watermark_path, str(max_ts))

            LOG.info(f"Gold: Wrote alerts and summary to {gold_path}")
            alerts.show()
            summary.show()

        LOG.info(f"Completed stages: {', '.join(stages)}")
        return alert_config
    finally:
        spark.stop()


def main() -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(description="Smart Agriculture Medallion Pipeline")
    parser.add_argument("--stage", action="append", choices=["bronze", "silver", "gold"],
                        help="Specific stage(s) to run")
    parser.add_argument("--ai", dest="ai_intent", metavar="INTENT",
                        help="AI intent for alert thresholds (e.g., 'stricter monitoring')")
    args = parser.parse_args()
    run(cfg, args.stage, args.ai_intent)


if __name__ == "__main__":
    import sys

    cfg = load_config()
    ai_intent = os.getenv("PIPELINE_AI_INTENT")

    if "--stage" in sys.argv:
        main()
    else:
        run(cfg, None, ai_intent)
