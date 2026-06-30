# AGENTS.md — Fase 3 Fraude

## Stack
- AWS Glue 5.0 (PySpark) + Delta Lake + EventBridge Scheduler + Lambda
- Buckets: `bucket-origen-adrianespinoza` (raw), `datalake-adrianespinoza` (bronze/silver/gold)
- Glue Catalog DB: `fraude_datalake`
- Tablas: `bronze_transactions`, `silver_transactions`, `gold_risk_profile`

## Pipeline
1. `scripts/deploy_glue.sh` → jobs + run + poll
2. `scripts/deploy_scheduler.sh` → Lambda + EventBridge schedule

## Local dev
- `delta.configure_spark_with_delta_pip()` + **explicit configs** required for delta-spark 4.3.0 + PySpark 4.x:
  ```python
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  ```
- `notebooks/demo.ipynb` — demo local (50K rows)
- WARN MemoryManager al escribir Delta/Parquet con heap default en local es normal.

## AWS Glue jobs (PySpark 3.4 no Delta Catalog config)
- NO usar `spark.sql.extensions` ni `spark.sql.catalog.spark_catalog` — Glue 5.0 maneja Delta nativamente con `--datalake-formats delta`.
- Usar `spark = SparkSession.builder.getOrCreate()` sin config extras.

## IAM
- `GlueFraudeRole`: s3 full access a `datalake-adrianespinoza/*`
- `LambdaGlueOrchestratorRole`: glue:startJobRun, glue:getJobRun, logs
- `EventBridgeSchedulerRole`: lambda:InvokeFunction

## MWAA
- Bloqueado: `SubscriptionRequiredException`. Activar desde AWS Console.
- DAG listo: `dags/fraude_mwaa_dag.py`
