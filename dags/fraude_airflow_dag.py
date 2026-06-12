# -*- coding: utf-8 -*-
"""
MDS UDD 2026 - Big Data y Cloud Computing
Orquestación en AWS MWAA (Managed Workflows for Apache Airflow)
Integrantes: Adrián y Ricardo
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'adrian_y_ricardo_aws',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'aws_mwaa_fraude_bancario_pipeline',
    default_args=default_args,
    description='Orquestación completa del pipeline en AWS EMR/Glue utilizando patrón Medallion Delta Lake',
    schedule_interval='@daily',
    catchup=False,
    tags=['AWS', 'MWAA', 'EMR', 'DeltaLake'],
) as dag:

    # 1. Comprobar el entorno de ejecución
    verify_spark_env = BashOperator(
        task_id='verificar_entorno_aws_spark',
        bash_command='spark-submit --version',
    )

    # 2. Paso Bronze
    def trigger_bronze_ingestion():
        import sys
        sys.path.append('/usr/local/airflow/dags/src') # Ajustado al path interno de AWS MWAA
        from ingestion import get_spark_session, generate_synthetic_transactions, save_to_bronze_layer
        
        spark = get_spark_session()
        df = generate_synthetic_transactions(spark, total_rows=15000000)
        save_to_bronze_layer(df)
        spark.stop()

    task_bronze = PythonOperator(
        task_id='aws_ingesta_a_s3_bronze',
        python_callable=trigger_bronze_ingestion,
    )

    # 3. Paso Silver
    def trigger_silver_transformation():
        import sys
        sys.path.append('/usr/local/airflow/dags/src')
        from ingestion import get_spark_session
        from transformations import build_silver_layer
        
        spark = get_spark_session()
        build_silver_layer(spark, "/tmp/datalake/bronze/transactions", "/tmp/datalake/silver/transactions")
        spark.stop()

    task_silver = PythonOperator(
        task_id='aws_transformacion_y_seguridad_s3_silver',
        python_callable=trigger_silver_transformation,
    )

    # 4. Paso Gold
    def trigger_gold_analytics():
        import sys
        sys.path.append('/usr/local/airflow/dags/src')
        from ingestion import get_spark_session
        from transformations import build_gold_layer
        
        spark = get_spark_session()
        build_gold_layer(spark, "/tmp/datalake/silver/transactions", "/tmp/datalake/gold/user_daily_features")
        spark.stop()

    task_gold = PythonOperator(
        task_id='aws_analitica_particionada_s3_gold',
        python_callable=trigger_gold_analytics,
    )

    # Flujo de dependencias del orquestador en la nube
    verify_spark_env >> task_bronze >> task_silver >> task_gold
