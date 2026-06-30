import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingestion import run_ingestion
from src.transformations import run_transformations

with DAG(
    'fraude_bancario_pipeline',
    start_date=datetime(2026, 6, 26),
    schedule_interval='@daily',
    catchup=False
) as dag:

    default_args = {
        'retries': 2,
        'retry_delay': 300,
    }

    task_ingestion = PythonOperator(
        task_id='ingestion_bronze',
        python_callable=run_ingestion,
        retries=2,
        retry_delay=300,
    )

    task_transform = PythonOperator(
        task_id='transform_silver_gold',
        python_callable=run_transformations,
        retries=2,
        retry_delay=300,
    )

    task_ingestion >> task_transform
