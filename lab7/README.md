# Lab 7 — PySpark Optimization Pipeline (Ride-Sharing)

AI-powered solution for Lab 7, demonstrating Spark optimization techniques including broadcast joins, filter pushdown, and partitioning.

## Optimizations Demonstrated

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   rides.csv     │     │  drivers.csv    │     │   Filter First  │
│   (large)       │────▶│   (small)       │────▶│  (pushdown)     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Repartition    │     │  Broadcast Join │     │   Aggregate     │
│  (parallelism)  │────▶│  (no shuffle)   │────▶│   by city       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                          │
                                                          ▼
                                                 ┌─────────────────┐
                                                 │ Partitioned     │
                                                 │ Output (city)   │
                                                 └─────────────────┘
```

## Project Structure

```
lab7/
├── README.md
├── requirements.txt
├── data/
│   ├── rides.csv          # Ride data (large)
│   └── drivers.csv        # Driver lookup (small)
├── genai/                 # Self-contained Ollama module
│   ├── __init__.py
│   ├── ollama_client.py
│   └── json_extract.py
└── tests/
    └── test_pipeline.py
```

## Prerequisites

- Java JDK 11 or 17 (`java -version` should work)
- Python 3.10+

## Setup

```bash
cd lab7
pip install -r requirements.txt
```

## Run

### Optimized Pipeline
```bash
python pipeline.py
```

### Baseline (No Optimizations)
```bash
python pipeline.py --baseline
```

### AI-Enabled Mode
```bash
python pipeline.py --ai --intent "Analyze Delhi city with 8 partitions"
```

Or use environment variables:
```bash
export USE_OLLAMA_PIPELINE=1
export PIPELINE_AI_INTENT="Use 6 partitions for Mumbai analysis"
python pipeline.py
```

## Outputs

- `output/partitioned_final/` — Partitioned Parquet by city (optimized)
- `output/baseline/` — Non-partitioned Parquet (baseline)

## Optimization Techniques

1. **Repartition**: Distribute data across 4 partitions for parallelism
2. **Filter Pushdown**: Filter `city == "Bangalore"` before join (reduces data)
3. **Broadcast Join**: Broadcast small `drivers` table to all nodes (avoids shuffle)
4. **Output Partitioning**: Partition output by `city` for fast queries

## Performance Comparison

Remove optimizations and compare:

```python
# Remove broadcast
.join(drivers, "driver_id")  # instead of broadcast(drivers)

# Remove filter pushdown (join first, filter after)
joined = rides.join(drivers, "driver_id")
filtered = joined.filter(joined.city == "Bangalore")

# Remove repartition
# (comment out rides.repartition(4))
```

## AI Integration

The `--ai` flag enables AI-driven optimization config:
- **target_city**: Which city to analyze
- **use_broadcast**: Whether to use broadcast join
- **repartition_count**: Number of partitions

## Tests

```bash
python -m pytest tests -v
```

**Test Coverage:**
- Data file existence
- Spark session creation
- Optimized pipeline execution
- Baseline pipeline execution
- AI parameter application

**Note:** First test run may take 30-60s as Spark initializes.

## Key Features

- ✅ Broadcast join for small lookup tables
- ✅ Filter pushdown optimization
- ✅ Repartitioning for parallelism
- ✅ Output partitioning for query performance
- ✅ AI-configurable optimization parameters
- ✅ Baseline comparison mode
- ✅ Self-contained genai module
