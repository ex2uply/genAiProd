# Lab Solutions — AI-Powered Data Pipelines

This folder contains AI-powered solutions for **Week 2, Day 3** labs demonstrating modern data engineering patterns with optional **Ollama** integration for dynamic pipeline configuration.

---

## 📁 Structure

```
labSolution/
├── README.md                 # This file
├── lab1/                     # Lab 1: Healthcare ETL (Pandas)
│   ├── README.md
│   ├── pipeline.py           # Modular ETL with AI cleaning params
│   ├── requirements.txt
│   ├── data/
│   │   └── patients.csv
│   ├── genai/                # Ollama HTTP client & JSON utils
│   │   ├── __init__.py
│   │   ├── ollama_client.py
│   │   └── json_extract.py
│   ├── tests/
│   │   └── test_pipeline.py
│   └── output/               # Generated on run
│       ├── billing.csv
│       └── daily.csv
│
├── lab2/                     # Lab 2: Medallion Pipeline (PySpark)
│   ├── README.md
│   ├── requirements.txt
│   ├── data/
│   │   └── patients.csv
│   └── pipeline/
│       ├── main.py           # Entrypoint with AI watermark
│       ├── config/
│       │   └── settings.yaml
│       ├── bronze/           # Raw ingestion layer
│       ├── silver/           # Cleaned & partitioned layer
│       ├── gold/             # Aggregated metrics layer
│       ├── dags/             # Prefect orchestration
│       ├── genai/            # Self-contained Ollama module
│       └── tests/
│
└── lab3/                     # Lab 3: Retail Workflow (Pandas)
    ├── README.md
    ├── requirements.txt
    ├── data/
    │   └── orders.csv
    └── pipeline/
        ├── main.py           # Retail ETL with AI cleaning
        ├── genai/            # Self-contained Ollama module
        │   ├── __init__.py
        │   ├── ollama_client.py
        │   └── json_extract.py
        ├── tests/
        │   └── test_pipeline.py
        └── output/           # Generated on run
            ├── revenue.csv
            └── daily_sales.csv

├── lab4/                     # Lab 4: Banking Pipeline (Modular Scaffold)
│   ├── README.md
│   ├── requirements.txt
│   ├── data/
│   │   └── transactions.csv
│   └── banking_pipeline/
│       ├── src/              # Modular package structure
│       │   ├── main.py       # Entrypoint with AI fraud config
│       │   ├── load.py
│       │   ├── transform.py
│       │   ├── fraud.py
│       │   ├── save.py
│       │   └── genai/        # Self-contained Ollama module
│       ├── tests/
│       │   └── test_pipeline.py
│       └── output/
│           ├── clean.csv
│           └── agg.csv
│
├── lab5/                     # Lab 5: Telecom CDC + Watermark (Pandas)
│   ├── README.md
│   ├── requirements.txt
│   ├── watermark.txt         # Tracks last processed date
│   ├── data/
│   │   ├── cdr_day1.csv
│   │   └── cdr_day2.csv
│   ├── genai/                # Self-contained Ollama module
│   └── tests/
│
├── lab6/                     # Lab 6: Schema Evolution (Pandas)
│   ├── README.md
│   ├── requirements.txt
│   ├── data/
│   │   ├── products_day1.csv
│   │   ├── products_day2.csv
│   │   └── products_day3.csv
│   ├── genai/                # Self-contained Ollama module
│   └── tests/
│
├── lab7/                     # Lab 7: PySpark Optimization
│   ├── README.md
│   ├── requirements.txt
│   ├── data/
│   │   ├── rides.csv
│   │   └── drivers.csv
│   ├── genai/                # Self-contained Ollama module
│   └── tests/
│
└── lab8/                     # Lab 8: Prefect Auto DAG (Logistics)
    ├── README.md
    ├── requirements.txt
    ├── data/
    │   └── shipments.csv
    ├── genai/                # Self-contained Ollama module
    └── tests/
```

---

## 🚀 Quick Start

### Lab 1 — Healthcare ETL (Pandas)

```bash
cd lab1
pip install -r requirements.txt

# Standard mode
python pipeline.py

# AI-enabled mode (requires Ollama)
python pipeline.py --ai --intent "Use median for missing age"

# Run tests
python -m pytest tests -v
```

**Outputs:**
- `output/billing.csv` — Total billing by diagnosis
- `output/daily.csv` — Daily patient counts

---

### Lab 2 — Medallion Pipeline (PySpark)

```bash
cd lab2
pip install -r requirements.txt

cd pipeline

# Full pipeline: Bronze → Silver → Gold
python main.py

# Individual stages
python main.py --stage bronze
python main.py --stage silver
python main.py --stage gold

# AI-enabled watermark (requires Ollama)
python main.py --ai --intent "Only include visits after 2024-01-01"

# Prefect orchestration
python dags/prefect_flow.py

# Run tests
python -m pytest tests -v
```

**Prerequisites:** Java JDK 11 or 17 (for PySpark)

**Outputs:**
- `output/bronze/` — Raw ingested Parquet
- `output/silver/` — Cleaned data (partitioned by visit_date)
- `output/gold/` — Billing aggregates by diagnosis

---

### Lab 3 — Retail Workflow (Pandas)

```bash
cd lab3
pip install -r requirements.txt

cd pipeline

# Standard mode
python main.py

# AI-enabled mode (requires Ollama)
python main.py --ai --intent "Default fills stay 1 and 0; do not drop zero-price rows"

# Run tests
python -m pytest tests -v
```

**Outputs:**
- `output/revenue.csv` — Revenue by category
- `output/daily_sales.csv` — Daily order counts

---

### Lab 4 — Banking Pipeline (Modular Scaffold)

```bash
cd lab4
pip install -r requirements.txt

cd banking_pipeline/src

# Standard mode
python main.py

# AI-enabled mode (requires Ollama)
python main.py --ai --intent "Lower fraud threshold to 500 for stricter detection"

# Run tests
cd ..
python -m pytest tests -v
```

**Outputs:**
- `output/clean.csv` — Cleaned transactions with `is_fraud` flag
- `output/agg.csv` — Total amounts per account

---

### Lab 5 — Telecom CDC + Watermark (Pandas)

```bash
cd lab5
pip install -r requirements.txt

# Standard mode (incremental)
python pipeline.py

# AI-enabled mode (with Ollama)
python pipeline.py --ai --intent "Process with 2-day lookback for late data"

# Run tests
python -m pytest tests -v
```

**Outputs:**
- `output/caller_summary.csv` — Total duration and call count per caller
- `watermark.txt` — Updated with latest processed date

---

### Lab 6 — Schema Evolution (Pandas)

```bash
cd lab6
pip install -r requirements.txt

# Standard mode
python pipeline.py

# AI-enabled mode (with Ollama)
python pipeline.py --ai --intent "Use 'N/A' for missing brands"

# Run tests
python -m pytest tests -v
```

**Outputs:**
- `output/final_products.csv` — Merged products with unified schema

---

### Lab 7 — PySpark Optimization

```bash
cd lab7
pip install -r requirements.txt

# Optimized pipeline
python pipeline.py

# Baseline (no optimizations)
python pipeline.py --baseline

# AI-enabled mode
python pipeline.py --ai --intent "Analyze Delhi with 8 partitions"

# Run tests
python -m pytest tests -v
```

**Outputs:**
- `output/partitioned_final/` — Partitioned Parquet by city (optimized)
- `output/baseline/` — Non-partitioned Parquet (baseline)

**Prerequisites:** Java JDK 11 or 17

---

### Lab 8 — Prefect Auto DAG (Logistics)

```bash
cd lab8
pip install -r requirements.txt

# Run with Prefect
python pipeline.py

# AI-enabled mode
python pipeline.py --ai --intent "Use mean for nulls, 5 retries"

# Run tests (pure functions, no Prefect runtime)
python -m pytest tests -v
```

**Outputs:**
- `output/avg_delivery_by_destination.csv` — Mean delivery time by destination

---

## 🤖 AI Integration (Ollama)

All eight labs support **runtime AI configuration** via Ollama:

### How It Works

1. **Safe by Design**: Labs request **JSON parameters only** from the AI — never executing generated code
2. **Validated Responses**: All AI outputs are parsed, validated, and fall back to defaults if invalid
3. **Self-Contained**: Each lab has its own `genai/` module (no external dependencies)

### Lab 1 — AI Cleaning Parameters

The AI suggests data cleaning strategies:
- `age_missing_strategy`: `"mean"` or `"median"`
- `billing_missing_fill`: numeric value (default: 0)

### Lab 2 — AI Watermark

The AI suggests incremental filter dates:
- `last_run_date`: `"YYYY-MM-DD"` format for filtering `visit_date > watermark`

### Lab 3 — AI Retail Cleaning

The AI suggests retail data cleaning strategies:
- `quantity_missing_fill`: Number to fill null quantities (default: 1)
- `price_missing_fill`: Number to fill null prices (default: 0)
- `filter_non_positive_price`: Boolean to filter rows with price <= 0

### Lab 4 — AI Fraud Detection

The AI suggests fraud detection parameters:
- `fraud_threshold`: Number above which transactions are flagged (default: 800)
- `missing_fill`: Number to fill missing amounts (default: 0)

### Lab 5 — AI CDC Configuration

The AI suggests CDC parameters:
- `watermark_date`: Suggested new watermark after processing
- `processing_strategy`: `"incremental"` or `"full_reload"`
- `lookback_days`: Days to look back for late-arriving data

### Lab 6 — AI Schema Evolution

The AI suggests default values for schema evolution:
- `default_brand`: String for missing brand values
- `default_discount`: Number for missing discount values

### Lab 7 — AI Spark Optimization

The AI suggests Spark optimization parameters:
- `target_city`: Which city to filter and analyze
- `use_broadcast`: Whether to use broadcast join
- `repartition_count`: Number of partitions for parallelism

### Lab 8 — AI Prefect DAG Config

The AI suggests workflow parameters:
- `null_fill`: How to handle null delivery times
- `ingest_retries`: Retries for data ingestion
- `clean_retries`: Retries for data cleaning

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OLLAMA_HOST` | Ollama base URL | `http://127.0.0.1:11434` |
| `OLLAMA_MODEL` | Model to use | `llama3` |
| `USE_OLLAMA_PIPELINE` | Enable AI mode without `--ai` flag | `0` |
| `PIPELINE_AI_INTENT` | Default intent if `--intent` omitted | varies by lab |

### Setting Up Ollama

```bash
# Install Ollama: https://ollama.com

# Start the server
ollama serve

# Pull a model (in another terminal)
ollama pull llama3

# Test the integration
python pipeline.py --ai  # Lab 1
cd ../lab2/pipeline && python main.py --ai  # Lab 2
cd ../lab3/pipeline && python main.py --ai  # Lab 3
cd ../lab4/banking_pipeline/src && python main.py --ai  # Lab 4
cd ../lab5 && python pipeline.py --ai  # Lab 5
cd ../lab6 && python pipeline.py --ai  # Lab 6
cd ../lab7 && python pipeline.py --ai  # Lab 7
cd ../lab8 && python pipeline.py --ai  # Lab 8
```

---

## 🧪 Testing

### Lab 1 Tests
```bash
cd lab1
python -m pytest tests -v
```

**Test Coverage:**
- `test_patients_csv_exists()` — Data file verification
- `test_modular_pipeline_writes_expected_outputs()` — Full pipeline execution
- `test_incremental_pattern_example_filter()` — Date filtering logic
- `test_ai_cleaning_spec()` — AI parameter application

### Lab 2 Tests
```bash
cd lab2/pipeline
python -m pytest tests -v
```

**Test Coverage:**
- `test_config_exists()` — Config file presence
- `test_config_loads()` — YAML parsing
- `test_input_csv_exists()` — Data file verification
- `test_gold_parquet_exists_after_pipeline_run()` — Integration test (requires Spark run first)

### Lab 3 Tests
```bash
cd lab3/pipeline
python -m pytest tests -v
```

**Test Coverage:**
- `test_orders_not_empty()` — Input data verification
- `test_orders_csv_exists()` — Data file presence
- `test_pipeline_outputs_after_run()` — Full pipeline execution
- `test_revenue_calculation()` — Revenue calculations
- `test_cleaning_logic()` — Data cleaning validation

### Lab 4 Tests
```bash
cd lab4/banking_pipeline
python -m pytest tests -v
```

**Test Coverage:**
- `test_data_loaded()` — Input data verification
- `test_transactions_csv_exists()` — Data file presence
- `test_pipeline_writes_outputs()` — Full pipeline execution
- `test_fraud_detection()` — Fraud flag logic
- `test_clean_data()` — Data cleaning validation
- `test_ai_fraud_spec()` — AI parameter application

### Lab 5 Tests
```bash
cd lab5
python -m pytest tests -v
```

**Test Coverage:**
- `test_watermark_read_write()` — Watermark operations
- `test_data_files_exist()` — Data file presence
- `test_load_data_filters_by_watermark()` — CDC filtering
- `test_transform_data()` — Aggregation logic
- `test_pipeline_writes_output()` — Full execution
- `test_ai_watermark_spec()` — AI parameter application

### Lab 6 Tests
```bash
cd lab6
python -m pytest tests -v
```

**Test Coverage:**
- `test_day_snapshots_exist()` — Daily files presence
- `test_merge_schema_unions_columns()` — Schema union
- `test_pipeline_writes_final_products_csv()` — Full execution
- `test_ai_schema_spec()` — AI parameter application

### Lab 7 Tests
```bash
cd lab7
python -m pytest tests -v
```

**Test Coverage:**
- `test_data_files_exist()` — Data files presence
- `test_spark_session_creation()` — Spark setup
- `test_pipeline_runs()` — Optimized pipeline
- `test_baseline_runs()` — Baseline comparison
- `test_ai_optimization_spec()` — AI parameter application

**Note:** First run may take 30-60s for Spark initialization

### Lab 8 Tests
```bash
cd lab8
python -m pytest tests -v
```

**Test Coverage:**
- `test_data_file_exists()` — Data file presence
- `test_clean_shipments()` — Pure clean function
- `test_avg_delivery_by_destination()` — Pure transform function
- `test_full_flow()` — Full pipeline flow
- `test_ai_dag_spec()` — AI parameter application

**Note:** Tests validate pure functions only—no Prefect runtime required. Fast execution.

---

## 📚 Concepts Demonstrated

### Lab 1 — Modular ETL Patterns
- ✅ Function-based pipeline architecture
- ✅ Docstrings and type hints
- ✅ Error handling with context
- ✅ Duplicate removal
- ✅ Missing value imputation (mean/median)
- ✅ Business metric aggregation

### Lab 2 — Medallion Architecture
- ✅ **Bronze**: Raw ingestion with incremental filtering
- ✅ **Silver**: Data cleaning, deduplication, partitioning
- ✅ **Gold**: Business-level aggregations
- ✅ **Orchestration**: Prefect flow for stage management
- ✅ **Configuration**: YAML-based settings

### Lab 3 — Retail Workflow Patterns
- ✅ **Spec → Scaffold → Test** workflow
- ✅ Handling nulls in quantity and price
- ✅ Filtering non-positive prices (configurable)
- ✅ Revenue aggregation by category
- ✅ Daily sales reporting

### Lab 4 — Modular Package Architecture
- ✅ **Modular `src/` package** structure
- ✅ **Separation of concerns**: load, transform, fraud, save
- ✅ Fraud detection with configurable thresholds
- ✅ Aggregation by account
- ✅ AI-augmented fraud configuration

### Lab 5 — CDC + Watermark Patterns
- ✅ **Change Data Capture**: Incremental loading with watermark
- ✅ **Watermark tracking**: File-based date tracking
- ✅ **Lookback mode**: Configurable days for late-arriving data
- ✅ **Full reload support**: AI-configurable reset capability

### Lab 6 — Schema Evolution
- ✅ **Schema union**: Merging different column sets
- ✅ **Null handling**: Filling missing columns with defaults
- ✅ **Evolution tracking**: Day-by-day schema changes
- ✅ **AI default values**: Configurable fill strategies

### Lab 7 — Spark Optimization
- ✅ **Broadcast joins**: Avoid shuffle for small tables
- ✅ **Filter pushdown**: Reduce data before joins
- ✅ **Repartitioning**: Optimize parallelism
- ✅ **Output partitioning**: Query performance optimization
- ✅ **Baseline comparison**: Before/after optimization metrics

### Lab 8 — Prefect Orchestration
- ✅ **Task retries**: Automatic retry with exponential backoff
- ✅ **Pure functions**: Testable without orchestrator
- ✅ **Flow composition**: Clean DAG dependencies
- ✅ **AI retry config**: Configurable retry counts

### AI Integration Patterns
- ✅ Natural language → structured JSON
- ✅ Runtime parameter injection
- ✅ Safe AI usage (validated outputs, no code execution)
- ✅ Graceful fallbacks when AI unavailable

---

## 📖 Learning Path

1. **Start with Lab 1** — Understand modular Python ETL patterns
2. **Enable `--ai` flag** — See how AI augments configuration
3. **Move to Lab 2** — Explore PySpark medallion architecture
4. **Run Prefect flow** — Understand pipeline orchestration
5. **Try Lab 3** — Apply patterns to retail data workflow
6. **Move to Lab 4** — Understand modular package architecture
7. **Try Lab 5** — Master CDC with watermark patterns
8. **Explore Lab 6** — Handle schema evolution scenarios
9. **Optimize with Lab 7** — Learn Spark performance tuning
10. **Orchestrate with Lab 8** — Build resilient workflows with Prefect
11. **Review `genai/` modules** — Learn safe AI integration patterns

---

## 🔧 Troubleshooting

### Lab 1
| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: pandas` | `pip install -r requirements.txt` |
| Ollama connection error | Ensure `ollama serve` is running |

### Lab 2
| Issue | Solution |
|-------|----------|
| `JAVA_HOME` / Spark errors | Install JDK 11/17 and add to PATH |
| `java -version` fails | Verify Java installation |
| PySpark import error | `pip install -r requirements.txt` |

### Lab 3
| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: pandas` | `pip install -r requirements.txt` |
| Revenue values seem off | Check if zero-price rows are being filtered |

### Lab 4
| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: pandas` | `pip install -r requirements.txt` |
| `ImportError` for genai | Ensure running from `banking_pipeline/src` directory |
| Fraud threshold not changing | Verify AI intent is being passed or use `--ai` flag |

### Lab 5
| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: pandas` | `pip install -r requirements.txt` |
| Watermark not updating | Check file permissions on `watermark.txt` |
| No records processed | Verify `call_date` format is YYYY-MM-DD |

### Lab 6
| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: pandas` | `pip install -r requirements.txt` |
| Schema union fails | Ensure all CSVs have consistent data types |
| Missing columns | Check that all day files are present |

### Lab 7
| Issue | Solution |
|-------|----------|
| `JAVA_HOME` / Spark errors | Install JDK 11/17 and add to PATH |
| `java -version` fails | Verify Java installation |
| PySpark import error | `pip install -r requirements.txt` |
| First run slow | Normal—Spark takes 30-60s to initialize |

### Lab 8
| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: pandas` | `pip install -r requirements.txt` |
| Prefect import error | Install prefect: `pip install prefect>=2.14.0` |
| Prefect API server timeout | Wait for first run to complete (starts temp server) |
| Task retry not working | Check Prefect version is 2.14+ |

---

## 📝 Requirements

### Lab 1
- Python 3.10+
- pandas >= 2.0.0
- pytest >= 7.0.0
- Ollama (optional, for AI features)

### Lab 2
- Python 3.10+
- PySpark >= 3.4.0
- pandas >= 2.0.0
- prefect >= 2.14.0
- pytest >= 7.0.0
- PyYAML >= 6.0
- Java JDK 11 or 17
- Ollama (optional, for AI features)

### Lab 3
- Python 3.10+
- pandas >= 2.0.0
- pytest >= 7.0.0
- Ollama (optional, for AI features)

### Lab 4
- Python 3.10+
- pandas >= 2.0.0
- pytest >= 7.0.0
- Ollama (optional, for AI features)

### Lab 5
- Python 3.10+
- pandas >= 2.0.0
- pytest >= 7.0.0
- Ollama (optional, for AI features)

### Lab 6
- Python 3.10+
- pandas >= 2.0.0
- pytest >= 7.0.0
- Ollama (optional, for AI features)

### Lab 7
- Python 3.10+
- PySpark >= 3.4.0
- pandas >= 2.0.0
- pytest >= 7.0.0
- Java JDK 11 or 17
- Ollama (optional, for AI features)

### Lab 8
- Python 3.10+
- pandas >= 2.0.0
- prefect >= 2.14.0
- pytest >= 7.0.0
- Ollama (optional, for AI features)

---

## 🎯 Key Takeaways

1. **AI-Augmented Pipelines**: Use AI for configuration, not code generation
2. **Safety First**: Always validate AI outputs; provide sensible defaults
3. **Modular Design**: Separate concerns (ingest, clean, aggregate)
4. **Layered Architecture**: Bronze/Silver/Gold pattern for data quality progression
5. **Test Coverage**: Unit tests for logic, integration tests for end-to-end

---

## 📂 Source Labs

These solutions are based on the original labs at:
```
/Users/as-mac-1191/genAi/githubdir/SIG-GENAI-May-2026/Week-2/Day-3/Labs/
├── lab-1/   # Healthcare ETL (Pandas)
├── lab-2/   # Medallion Pipeline (PySpark)
├── lab-3/   # Retail Pandas Workflow
├── lab-4/   # Banking Modular Scaffold
├── lab-5/   # Telecom Watermark + CDC
├── lab-6/   # Schema Evolution
├── lab-7/   # PySpark Optimization
├── lab-8/   # Prefect DAG (Logistics)
└── genai/   # Shared Ollama utilities
```

---

**Created for:** Week 2, Day 3 — AI-Powered Data Pipelines  
**Topics:** PySpark, Pandas, Prefect, Ollama, Medallion Architecture, ETL Patterns
