"""
Spotify Data Pipeline DAG

Orchestrates the complete ETL pipeline for Spotify track analytics:
1. Downloads data from Kaggle
2. Loads raw data to PostgreSQL
3. Runs dbt transformations
4. Executes data quality tests
5. Generates quality reports

Author: Data Engineering Team
Created: 2024
"""

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import os


# =============================================================================
# DAG Configuration
# =============================================================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-alerts@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Environment Variables
DBT_PROJECT_DIR = os.environ.get('DBT_PROFILES_DIR', '/opt/airflow/dbt')
POSTGRES_CONN_ID = 'spotify_postgres'
KAGGLE_DATASET = 'rohiteng/spotify-music-analytics-dataset-20152025'


# =============================================================================
# Python Callables
# =============================================================================
def download_kaggle_dataset(**context: Any) -> str:
    """Download Spotify dataset from Kaggle using kagglehub."""
    import kagglehub

    try:
        path = kagglehub.dataset_download(KAGGLE_DATASET)
        context['ti'].xcom_push(key='dataset_path', value=path)
        print(f"Dataset downloaded successfully to: {path}")
        return path
    except Exception as e:
        raise Exception(f"Failed to download dataset: {str(e)}")


def load_to_postgres(**context: Any) -> int:
    """Load CSV data to PostgreSQL raw schema."""
    import pandas as pd
    from sqlalchemy import create_engine, text
    import os

    dataset_path = context['ti'].xcom_pull(key='dataset_path')

    # Find CSV file in the dataset directory
    csv_files = [f for f in os.listdir(dataset_path) if f.endswith('.csv')]
    if not csv_files:
        raise Exception(f"No CSV files found in {dataset_path}")

    csv_path = os.path.join(dataset_path, csv_files[0])
    print(f"Loading data from: {csv_path}")

    # Read CSV
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} records from CSV")

    # Add metadata columns
    df['_loaded_at'] = datetime.now()
    df['_source_file'] = csv_path

    # Connect to PostgreSQL
    pg_host = os.environ.get('POSTGRES_HOST', 'postgres')
    pg_port = os.environ.get('POSTGRES_PORT', '5432')
    pg_user = os.environ.get('POSTGRES_USER', 'spotify')
    pg_pass = os.environ.get('POSTGRES_PASSWORD', 'spotify_password')
    pg_db = os.environ.get('POSTGRES_DB', 'spotify_warehouse')

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # Drop existing table with CASCADE to remove dependent views
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw.spotify_tracks_raw CASCADE"))
    print("Dropped existing table with CASCADE")

    # Load to raw schema in chunks (use 'fail' since we just dropped it)
    # Using smaller chunks and no 'multi' method to avoid memory issues
    df.to_sql(
        'spotify_tracks_raw',
        engine,
        schema='raw',
        if_exists='fail',
        index=False,
        chunksize=1000
    )
    print(f"Successfully loaded {len(df)} records to raw.spotify_tracks_raw")
    context['ti'].xcom_push(key='row_count', value=len(df))
    return len(df)


def check_data_quality_gate(**context: Any) -> str:
    """Branch based on data quality test results."""
    ti = context['ti']
    test_output = ti.xcom_pull(task_ids='dbt_tasks.dbt_test')

    # Check if tests passed (simplified - actual implementation parses dbt output)
    if test_output and 'FAIL' not in str(test_output).upper():
        print("Data quality tests passed - proceeding to reporting")
        return 'quality_passed'
    else:
        print("Data quality tests failed - skipping reporting")
        return 'quality_failed'


# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id='spotify_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Spotify track analytics with data quality checks',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spotify', 'etl', 'data-quality', 'dbt'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # Start/End Tasks
    # =========================================================================
    start = EmptyOperator(task_id='start')

    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
    )

    # =========================================================================
    # Extract Phase
    # =========================================================================
    with TaskGroup(group_id='extract') as extract_group:
        download_data = PythonOperator(
            task_id='download_kaggle_data',
            python_callable=download_kaggle_dataset,
            provide_context=True,
        )

    # =========================================================================
    # Load Phase
    # =========================================================================
    with TaskGroup(group_id='load') as load_group:
        ensure_raw_schema = PostgresOperator(
            task_id='ensure_raw_schema',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="CREATE SCHEMA IF NOT EXISTS raw;",
        )

        load_data = PythonOperator(
            task_id='load_to_postgres',
            python_callable=load_to_postgres,
            provide_context=True,
        )

        ensure_raw_schema >> load_data

    # =========================================================================
    # Transform Phase (dbt)
    # =========================================================================
    with TaskGroup(group_id='dbt_tasks') as dbt_group:
        dbt_deps = BashOperator(
            task_id='dbt_deps',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}',
        )

        dbt_seed = BashOperator(
            task_id='dbt_seed',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROJECT_DIR} --full-refresh',
        )

        dbt_run_staging = BashOperator(
            task_id='dbt_run_staging',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} --select staging',
        )

        dbt_run_intermediate = BashOperator(
            task_id='dbt_run_intermediate',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} --select intermediate',
        )

        dbt_run_marts = BashOperator(
            task_id='dbt_run_marts',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} --select marts',
        )

        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR} --store-failures || true',
        )

        # Task dependencies within dbt group
        dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test

    # =========================================================================
    # Quality Gate
    # =========================================================================
    quality_gate = BranchPythonOperator(
        task_id='quality_gate',
        python_callable=check_data_quality_gate,
        provide_context=True,
    )

    quality_passed = EmptyOperator(task_id='quality_passed')
    quality_failed = EmptyOperator(task_id='quality_failed')

    # =========================================================================
    # Reporting Phase
    # =========================================================================
    with TaskGroup(group_id='reporting') as reporting_group:
        generate_dq_report = BashOperator(
            task_id='generate_dq_report',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} --select dq_metrics_summary',
        )

        generate_docs = BashOperator(
            task_id='generate_docs',
            bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROJECT_DIR}',
        )

        [generate_dq_report, generate_docs]

    # =========================================================================
    # DAG Dependencies
    # =========================================================================
    start >> extract_group >> load_group >> dbt_group >> quality_gate
    quality_gate >> [quality_passed, quality_failed]
    quality_passed >> reporting_group >> end
    quality_failed >> end
