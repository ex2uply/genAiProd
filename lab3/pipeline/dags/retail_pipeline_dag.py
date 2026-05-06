"""
Airflow DAG for Lab 3 Retail Pipeline.

This DAG orchestrates the retail ETL pipeline:
1. Load orders data
2. Clean data (handle nulls, duplicates)
3. Transform (calculate revenue and daily sales)
4. Save outputs

Requirements:
- pip install apache-airflow pandas

To use:
1. Copy this file to your Airflow dags folder (~/airflow/dags/)
2. Update DATA_PATH in the dag_config to point to your data file
3. Start Airflow: airflow standalone
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG configuration
dag_config = {
    'data_path': '/Users/as-mac-1191/genAi/githubdir/SIG-GENAI-May-2026/Week-2/Day-3/labSolution/lab3/data/orders.csv',
    'output_dir': '/Users/as-mac-1191/genAi/githubdir/SIG-GENAI-May-2026/Week-2/Day-3/labSolution/lab3/output',
}


def load_orders_task(**kwargs):
    """Load orders from CSV."""
    import sys
    from pathlib import Path
    
    # Add pipeline to path
    pipeline_dir = Path(__file__).resolve().parent.parent
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))
    
    import main as pipeline_main
    
    data_path = Path(dag_config['data_path'])
    df = pipeline_main.load_data(data_path)
    
    # Push to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    return f"Loaded {len(df)} rows from {data_path}"


def clean_data_task(**kwargs):
    """Clean orders data."""
    import sys
    from pathlib import Path
    import pandas as pd
    
    pipeline_dir = Path(__file__).resolve().parent.parent
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))
    
    import main as pipeline_main
    
    # Get data from XCom
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids='load_orders', key='raw_data')
    df = pd.read_json(raw_json)
    
    cleaned = pipeline_main.clean_data(df)
    
    # Push to XCom
    ti.xcom_push(key='cleaned_data', value=cleaned.to_json())
    return f"Cleaned data: {len(cleaned)} rows after removing duplicates and nulls"


def transform_data_task(**kwargs):
    """Transform data to revenue and daily sales."""
    import sys
    from pathlib import Path
    import pandas as pd
    
    pipeline_dir = Path(__file__).resolve().parent.parent
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))
    
    import main as pipeline_main
    
    # Get cleaned data from XCom
    ti = kwargs['ti']
    cleaned_json = ti.xcom_pull(task_ids='clean_data', key='cleaned_data')
    df = pd.read_json(cleaned_json)
    
    revenue, daily = pipeline_main.transform_data(df)
    
    # Push to XCom
    ti.xcom_push(key='revenue', value=revenue.to_json())
    ti.xcom_push(key='daily_sales', value=daily.to_json())
    
    return f"Generated revenue ({len(revenue)} categories) and daily sales ({len(daily)} days)"


def save_outputs_task(**kwargs):
    """Save output files."""
    import sys
    from pathlib import Path
    import pandas as pd
    
    pipeline_dir = Path(__file__).resolve().parent.parent
    if str(pipeline_dir) not in sys.path:
        sys.path.insert(0, str(pipeline_dir))
    
    import main as pipeline_main
    
    # Get data from XCom
    ti = kwargs['ti']
    revenue_json = ti.xcom_pull(task_ids='transform_data', key='revenue')
    daily_json = ti.xcom_pull(task_ids='transform_data', key='daily_sales')
    
    revenue = pd.read_json(revenue_json)
    daily = pd.read_json(daily_json)
    
    # Save outputs
    output_dir = Path(dag_config['output_dir'])
    pipeline_main.save_data(revenue, output_dir / 'revenue.csv')
    pipeline_main.save_data(daily, output_dir / 'daily_sales.csv')
    
    return f"Saved outputs to {output_dir}"


# Define the DAG
with DAG(
    'retail_pipeline_lab3',
    default_args=default_args,
    description='Retail ETL Pipeline DAG for Lab 3',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['retail', 'etl', 'lab3'],
) as dag:
    
    # Task 1: Load data
    load_task = PythonOperator(
        task_id='load_orders',
        python_callable=load_orders_task,
    )
    
    # Task 2: Clean data
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data_task,
    )
    
    # Task 3: Transform data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_task,
    )
    
    # Task 4: Save outputs
    save_task = PythonOperator(
        task_id='save_outputs',
        python_callable=save_outputs_task,
    )
    
    # Define task dependencies
    load_task >> clean_task >> transform_task >> save_task
