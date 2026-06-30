from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

with DAG(
    'fraude_bancario_pipeline',
    start_date=datetime(2026, 6, 26),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline diario de detección de fraude (Bronze → Silver → Gold)',
) as dag:

    task_ingestion = GlueJobOperator(
        task_id='ingestion_bronze',
        job_name='fraude-ingestion-bronze',
        region_name='us-east-1',
        wait_for_completion=True,
    )

    task_transform = GlueJobOperator(
        task_id='transform_silver_gold',
        job_name='fraude-transform-silver-gold',
        region_name='us-east-1',
        wait_for_completion=True,
    )

    task_ingestion >> task_transform
