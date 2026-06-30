# Pipeline de Detección de Fraude — Proyecto Integrador MDS UDD

Arquitectura Serverless en AWS para el procesamiento de transacciones bancarias. El pipeline implementa el patrón Medallion (Bronze/Silver/Gold) utilizando **Delta Lake** para transacciones ACID y **AWS Glue (Spark)** para el procesamiento distribuido.

## Stack

| Componente | Tecnología | Rol |
|---|---|---|
| Procesamiento | **AWS Glue 5.0 (PySpark)** | Ejecución distribuida serverless (5 DPU G.1X) |
| Almacenamiento | **Delta Lake / Parquet** sobre S3 | Capas Silver y Gold con ACID y Time Travel |
| Orquestación | **EventBridge Scheduler + Lambda** | Disparo diario @ 03:00 UTC con polling secuencial |
| Consultas | **Amazon Athena** (engine v3) | SQL serverless sobre Glue Catalog |
| Alternativa | **MWAA** (Airflow) | DAG preparado para cuando la cuenta active el servicio |

## Pipeline Flow

```
Raw JSON (S3) ──▶ Bronze (Parquet) ──▶ Silver (Delta, particionado) ──▶ Gold (Delta)
```

## Estructura del Proyecto

```
├── src/
│   ├── __init__.py
│   ├── ingestion.py              # Capa Bronze: JSON → Parquet + linaje
│   └── transformations.py        # Capas Silver + Gold: calidad, PII, Delta Lake
├── dags/
│   ├── fraude_dag.py             # DAG Airflow local (PythonOperator)
│   └── fraude_mwaa_dag.py        # DAG MWAA (GlueJobOperator)
├── scripts/
│   ├── generate_data.py          # Generador 10M filas sintéticas
│   ├── glue_orchestrator_lambda.py  # Lambda orquestadora (polling Glue jobs)
│   ├── deploy_scheduler.sh       # Deploy orquestación EventBridge + Lambda
│   ├── deploy_glue.sh            # Deploy Glue jobs completo
│   ├── setup_s3.sh               # Creación buckets y subida datos
│   └── athena_queries.sql        # 6 consultas Athena para Silver/Gold
├── notebooks/
│   └── demo.ipynb                # Notebook demo (50K registros, Spark local)
├── tests/
│   └── test_transformations.py   # Tests unitarios (3 casos)
├── docs/
│   ├── propuesta_arquitectonica.md   # Informe técnico + diagramas C4
│   ├── adrs.md                       # 3 Architecture Decision Records
│   ├── optimizacion.md               # Optimización Spark + FinOps
│   ├── presentacion_outline.md       # Outline para defensa 3 julio
│   ├── iam_policies.json             # Políticas IAM (Glue, Athena, S3)
│   └── media/                        # Diagramas de arquitectura
├── AGENTS.md                  # Contexto para sesiones OpenCode
├── GAP_ANALYSIS.md            # Brechas vs requisitos del enunciado
└── requirements.txt           # Dependencias Python
```

## Datos de Entrada y Salida

### Entrada (Bronze)

Formato: **JSON Lines** (`.json`), un objeto por línea.

| Campo | Tipo | Descripción |
|---|---|---|
| `transaction_id` | string | Identificador único de transacción |
| `user_id` | string | UUID del cliente |
| `card` | string | Número de tarjeta (16 dígitos) |
| `amount` | float | Monto de la transacción |
| `transaction_date` | date | Fecha de la transacción (`YYYY-MM-DD`) |
| `timestamp` | datetime | Timestamp completo |
| `merchant` | string | Nombre del comercio |
| `currency` | string | Código de moneda (USD, EUR, CLP, etc.) |

Ubicación: `s3://bucket-origen-adrianespinoza/raw-data/`

### Salida Silver (Delta Lake)

Particionado por `transaction_date`. La columna `card` se reemplaza por `card_masked` (`4532-XXXX-XXXX-0367`).

Ubicación: `s3://datalake-adrianespinoza/silver/transactions/`
Filas: **9,800,000**

### Salida Gold (Delta Lake)

Perfil de riesgo diario por usuario. Heurística: `is_high_risk = True` cuando `tx_count > 12`.

| Campo | Tipo | Descripción |
|---|---|---|
| `user_id` | string | UUID del cliente |
| `transaction_date` | date | Fecha de agregación |
| `tx_count` | long | N° de transacciones en el día |
| `daily_total` | double | Suma de montos del día |
| `is_high_risk` | boolean | `true` si `tx_count > 12` |

Ubicación: `s3://datalake-adrianespinoza/gold/risk_profile/`
Filas: **1,801,955** | Usuarios únicos: **10,000** | High risk: **50.24%**

## Instalación y Ejecución

### Requisitos

- Python 3.11+
- Apache Spark 3.5+ (local) o entorno AWS Glue
- Delta Lake jars configurados en la SparkSession
- AWS CLI configurada (para despliegue en AWS)

### Instalación local

```bash
pip install -r requirements.txt
```

### Generar datos sintéticos

```bash
python scripts/generate_data.py
```

Genera 10M transacciones en `data/raw/` (archivos JSON Lines). Para subir a S3:

```bash
aws s3 cp data/raw/ s3://bucket-origen-adrianespinoza/raw-data/ --recursive
```

### Ejecutar notebook demo

```bash
jupyter notebook notebooks/demo.ipynb
```

Ejecuta el pipeline completo (Bronze → Silver → Gold) sobre 50K transacciones sintéticas usando Spark local.

### Ejecutar en AWS Glue

Opción 1 — Script automatizado:

```bash
bash scripts/deploy_glue.sh
```

Opción 2 — Manual:
1. Subir scripts: `aws s3 cp src/ s3://bucket-origen-adrianespinoza/scripts/ --recursive`
2. Configurar Glue Job con:
   - **Script**: `s3://bucket-origen-adrianespinoza/scripts/ingestion.py`
   - **Parámetro**: `--datalake-formats` = `delta`
   - **Glue version**: 5.0
3. Ejecutar `fraude-ingestion-bronze`, luego `fraude-transform-silver-gold`

### Métricas de Producción (10M registros, Glue 5.0)

| Etapa | Runtime | Workers | DPU-Seconds |
|---|---|---|---|
| Ingesta Bronze | 109 s | 5 × G.1X | 546 |
| Transform Silver-Gold | 163 s | 5 × G.1X | 817 |
| **Pipeline total** | **~4.5 min** | **5 DPU** | **1,363** |

## Orquestación

### Producción: EventBridge Scheduler + Lambda

El pipeline se orquesta diariamente a las 03:00 UTC sin necesidad de MWAA:

```
EventBridge Scheduler (cron: 0 3 * * ? *)
  └── Lambda (glue-fraude-orchestrator)
        ├── StartGlueJob(fraude-ingestion-bronze) → poll → SUCCEEDED
        └── StartGlueJob(fraude-transform-silver-gold) → poll → SUCCEEDED
```

**Despliegue** (1 comando):

```bash
bash scripts/deploy_scheduler.sh
```

**Prueba manual**:

```bash
aws lambda invoke --function-name glue-fraude-orchestrator /tmp/test-output.json
cat /tmp/test-output.json
```

**Costo**: ~$1/mes (vs ~$38/mes de MWAA).

### Alternativa: Airflow (MWAA)

La cuenta AWS no tiene MWAA habilitado (requiere suscripción vía consola). El DAG `dags/fraude_mwaa_dag.py` está listo para desplegar cuando se active el servicio.

### Desarrollo local: Airflow

```bash
airflow standalone  # requiere Apache Airflow instalado
```

DAG: `dags/fraude_dag.py` — usa `PythonOperator` para ejecutar los scripts localmente.

## Consultas Athena

Las tablas Bronze, Silver y Gold están registradas en el Glue Catalog (`fraude_datalake`) como tablas Parquet/Hive. Consultas de ejemplo en `scripts/athena_queries.sql`.

```sql
-- Total filas por capa
SELECT 'Silver' AS capa, COUNT(*) FROM fraude_datalake.silver_transactions
UNION ALL
SELECT 'Gold', COUNT(*) FROM fraude_datalake.gold_risk_profile;
```

### Resultados de las 6 consultas

| Consulta | Resultado |
|---|---|
| Top 10 usuarios más activos | Máx: 20 transacciones/día ($927) |
| Usuarios alto riesgo (>12 tx/día) | 7,003 combinaciones |
| Volumen diario últimos 7 días | Funcional con `date_parse` |
| Métricas de riesgo global | 10K usuarios, 50.24% high risk |
| Transacciones por moneda | USD 50%, EUR 20%, CLP 15%, BRL 10%, MXN 5% |
| Total filas | Silver: 9,800,000 / Gold: 1,801,955 |

## Costos

| Componente | Costo/mes |
|---|---|
| Glue (5 DPU × ~0.075 h × 30 días) | $4.95 |
| S3 (~11 GB almacenamiento + solicitudes) | $0.30 |
| EventBridge Scheduler + Lambda | $1.00 |
| Athena (~10 TB escaneados) | $5.00 |
| **Total** | **~$10.25/mes** |

## Documentación Adicional

- [Propuesta Arquitectónica](docs/propuesta_arquitectonica.md) — Informe técnico con diagramas C4, métricas de producción, costos
- [Architecture Decision Records](docs/adrs.md) — 3 ADRs (Glue, Delta Lake, MWAA/EventBridge)
- [Optimización y FinOps](docs/optimizacion.md) — Spark UI benchmarks + costos reales
- [Gap Analysis](GAP_ANALYSIS.md) — Checklist de brechas contra requisitos del enunciado
- [Presentación Defensa](docs/presentacion_outline.md) — Outline 13 slides para el 3 julio
- [Instrucciones para IA](AGENTS.md) — Contexto para sesiones OpenCode

## Créditos

| Rol | Nombre |
|---|---|
| **Programa** | Magíster en Data Science, Universidad del Desarrollo |
| **Curso** | BIGDATA MDSC3S141-1 |
| **Profesor** | Prof. Luis Castillo Faune |
| **Alumnos** | Adrián Espinoza A., Ricardo Castro V. |
