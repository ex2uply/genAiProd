"""
Lab 7 — PySpark Optimization Pipeline (Ride-Sharing)

This pipeline demonstrates Spark optimization techniques:
- Broadcast join (for small lookup tables)
- Filter pushdown (filter before join)
- Repartitioning (for parallelism)
- Partitioning output (for query performance)
"""
import argparse
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, sum as spark_sum

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

LAB_ROOT = Path(__file__).resolve().parent
DATA_DIR = LAB_ROOT / "data"
OUTPUT_DIR = LAB_ROOT / "output"


def _env_truthy(name: str) -> bool:
    """Check if environment variable is truthy."""
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def create_spark_session(app_name: str = "RideAnalytics") -> SparkSession:
    """Create Spark session with optimization settings."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def run_pipeline(
    target_city: str = "Bangalore",
    use_broadcast: bool = True,
    repartition_count: int = 4,
) -> None:
    """
    Run optimized Spark pipeline.
    
    Args:
        target_city: City to filter for
        use_broadcast: Use broadcast join for small table
        repartition_count: Number of partitions for rides
    """
    LOG.info("Spark Optimization Pipeline started")
    LOG.info("Config: city=%s, broadcast=%s, partitions=%d", target_city, use_broadcast, repartition_count)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load data
        rides = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(DATA_DIR / "rides.csv"))
        
        drivers = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(DATA_DIR / "drivers.csv"))
        
        LOG.info("Loaded rides: %d rows", rides.count())
        LOG.info("Loaded drivers: %d rows", drivers.count())
        
        # Optimization 1: Repartition for parallelism
        rides = rides.repartition(repartition_count)
        
        # Optimization 2: Filter pushdown (filter before join)
        rides_filtered = rides.filter(rides.city == target_city)
        LOG.info("After city filter: %d rows", rides_filtered.count())
        
        # Optimization 3: Broadcast join (small table broadcasted to all nodes)
        if use_broadcast:
            joined = rides_filtered.join(broadcast(drivers), "driver_id")
            LOG.info("Used broadcast join")
        else:
            joined = rides_filtered.join(drivers, "driver_id")
            LOG.info("Used regular shuffle join")
        
        # Aggregate revenue by city
        revenue = joined.groupBy("city").agg(
            spark_sum("fare").alias("total_revenue"),
        )
        
        # Optimization 4: Partition output by city for query performance
        out_dir = OUTPUT_DIR / "partitioned_final"
        revenue.write \
            .mode("overwrite") \
            .partitionBy("city") \
            .parquet(str(out_dir))
        
        LOG.info("Saved partitioned output to %s", out_dir)
        
        # Show results
        revenue.show()
        
    finally:
        spark.stop()
    
    LOG.info("Pipeline finished")


def run_baseline() -> None:
    """Run baseline without optimizations for comparison."""
    LOG.info("Baseline Pipeline (no optimizations)")
    
    spark = create_spark_session("RideAnalyticsBaseline")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        rides = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(DATA_DIR / "rides.csv"))
        
        drivers = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(str(DATA_DIR / "drivers.csv"))
        
        # No optimizations: join first, then filter
        joined = rides.join(drivers, "driver_id")
        filtered = joined.filter(joined.city == "Bangalore")
        
        revenue = filtered.groupBy("city").agg(
            spark_sum("fare").alias("total_revenue"),
        )
        
        out_dir = OUTPUT_DIR / "baseline"
        revenue.write.mode("overwrite").parquet(str(out_dir))
        
        LOG.info("Saved baseline output to %s", out_dir)
        revenue.show()
        
    finally:
        spark.stop()


def optimization_spec(
    user_intent: str | None,
    model: str | None = None,
) -> dict:
    """
    Get Spark optimization config from AI.
    
    Args:
        user_intent: Natural language intent
        model: Ollama model
        
    Returns:
        Dict with target_city, use_broadcast, repartition_count
    """
    defaults = {
        "target_city": "Bangalore",
        "use_broadcast": True,
        "repartition_count": 4,
    }
    
    intent = user_intent or os.environ.get(
        "PIPELINE_AI_INTENT",
        "Optimize for Bangalore city analysis",
    )
    
    try:
        import sys
        sys.path.insert(0, str(LAB_ROOT))
        from genai.ollama_client import chat
        from genai.json_extract import extract_json_object

        system_msg = (
            "You are a Spark optimization expert. Reply with ONE JSON object only. "
            "No markdown fences, no commentary."
        )
        user_msg = f"""Lab: PySpark optimization for ride-sharing analytics.

Data: rides.csv (ride_id, driver_id, city, fare, ride_date)
      drivers.csv (driver_id, driver_name, vehicle_type)

User intent:
{intent}

Return JSON with exactly these keys:
- target_city: string (city to filter, default: "Bangalore")
- use_broadcast: boolean (use broadcast join for small drivers table)
- repartition_count: number (4 for local, 8+ for cluster)

Suggested defaults: {defaults}
"""
        raw = chat(
            [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            model=model,
        )
        spec = extract_json_object(raw)

        return {
            "target_city": spec.get("target_city", defaults["target_city"]),
            "use_broadcast": spec.get("use_broadcast", defaults["use_broadcast"]),
            "repartition_count": int(spec.get("repartition_count", defaults["repartition_count"])),
        }
    except Exception as e:
        LOG.warning("AI spec failed, using defaults: %s", e)
        return defaults


def run_pipeline_ai(*, use_ai: bool = False, ai_intent: str | None = None) -> None:
    """
    Run pipeline with optional AI configuration.
    
    Args:
        use_ai: Whether to use AI for config
        ai_intent: Natural language intent
    """
    if use_ai:
        spec = optimization_spec(ai_intent)
        LOG.info("AI-enabled spec: %s", spec)
        run_pipeline(
            target_city=spec["target_city"],
            use_broadcast=spec["use_broadcast"],
            repartition_count=spec["repartition_count"],
        )
    else:
        run_pipeline()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PySpark Optimization Pipeline (optional --ai uses Ollama)."
    )
    parser.add_argument("--ai", action="store_true", help="Ask Ollama for optimization config.")
    parser.add_argument("--intent", default=None, help="Natural-language intent for AI.")
    parser.add_argument("--baseline", action="store_true", help="Run baseline without optimizations.")
    args = parser.parse_args()
    
    if args.baseline:
        run_baseline()
    else:
        use_ai = args.ai or _env_truthy("USE_OLLAMA_PIPELINE")
        run_pipeline_ai(use_ai=use_ai, ai_intent=args.intent)
