# Architecture Decision Records (ADRs)

> Anexo al Informe Técnico — Proyecto Integrador Fase 3
> MDS UDD 2026

---

## ADR-001: Migración a AWS Glue Jobs (Serverless)

| Campo | Detalle |
|---|---|
| **ID** | ADR-001 |
| **Título** | Migración a AWS Glue Jobs como motor de procesamiento |
| **Estado** | Aceptado |
| **Fecha** | 2026-06-26 |

### Contexto

El pipeline debe procesar datasets históricos de 15M+ transacciones (6.2 GB) con una ingesta incremental de ~2M transacciones/día. Se requiere un motor distribuido que maneje cargas variables sin provisionar infraestructura fija, minimizando la carga operativa del equipo.

### Decisión

Adoptar **AWS Glue Jobs (Apache Spark)** como motor de procesamiento serverless. Los scripts PySpark se ejecutan sobre un cluster Spark administrado, pagando solo por los DPUs consumidos durante la ejecución.

### Alternativas Consideradas

- **EMR Serverless**: Mayor flexibilidad de configuración, pero introduce complejidad adicional en redes y seguridad (VPC, subnets, security groups) sin aportar ventajas significativas para un pipeline batch.
- **AWS Lambda**: Límite de 10 GB de memoria y 15 minutos de ejecución. Inviable para procesar 15M+ filas con transformaciones distribuidas.
- **Amazon SageMaker Processing**: Orientado a cargas de ML, no a pipelines de datos transaccionales. Mayor costo por hora de instancia.

### Consecuencias

- **Positivas**: Integración nativa con Glue Data Catalog, eliminando la gestión manual del metastore. Escalamiento automático sin intervención del equipo. Costo variable alineado con volumen de datos procesados.
- **Negativas**: Dependencia de librerías externas (Delta Lake jars) que deben configurarse como parámetros del job. Depuración limitada comparada con un cluster EMR tradicional.

---

## ADR-002: Adopción de Formato Delta Lake (ACID)

| Campo | Detalle |
|---|---|
| **ID** | ADR-002 |
| **Título** | Adopción de formato Delta Lake para capas Silver y Gold |
| **Estado** | Aceptado |
| **Fecha** | 2026-06-26 |

### Contexto

El pipeline debe cumplir con normativas de protección de datos (Ley 21.719 / GDPR) que requieren capacidad de borrado y actualización selectiva de registros. Además, se necesita trazabilidad de cambios (linaje) para auditoría.

### Decisión

Implementar **Delta Lake** como formato de almacenamiento en las capas Silver y Gold. Bronze permanece en Parquet simple por ser una capa de staging inmutable. Delta Lake se integra vía `delta-spark` con SparkSession configurada con `DeltaSparkSessionExtension` y `DeltaCatalog`.

### Alternativas Consideradas

- **Parquet plano**: No soporta operaciones ACID (UPSERT, DELETE, MERGE). Requiere reescritura completa de particiones para actualizar registros, lo que hace impracticable el cumplimiento de GDPR.
- **Apache Iceberg**: Funcionalidad equivalente a Delta Lake. Se descartó por menor madurez en el ecosistema Spark en el momento de la decisión y menor disponibilidad de documentación operativa.

### Consecuencias

- **Positivas**: Time Travel para auditoría (`VERSION AS OF`). Operaciones de Merge para upsert eficiente. Particionamiento por `transaction_date` compatible con partition pruning en Athena.
- **Negativas**: Dependencia adicional de jars de Delta Lake en los jobs de Glue. Mayor tamaño de almacenamiento por el transaction log. La capa Bronze en Parquet no se beneficia de estas capacidades.

---

## ADR-003: Orquestación mediante MWAA (Airflow)

| Campo | Detalle |
|---|---|
| **ID** | ADR-003 |
| **Título** | Orquestación del pipeline mediante AWS MWAA (Managed Workflows for Apache Airflow) |
| **Estado** | Aceptado |
| **Fecha** | 2026-06-26 |

### Contexto

El pipeline se compone de dos etapas secuenciales (ingestión Bronze → transformación Silver+Gold) que deben ejecutarse en orden, con manejo de errores, reintentos, y monitoreo centralizado. Se requiere un orquestador que modele estas dependencias de forma declarativa.

### Decisión

Utilizar **AWS MWAA (Managed Workflows for Apache Airflow)** para orquestar las tareas del pipeline. El DAG `fraude_bancario_pipeline` define la dependencia `ingestion_bronze >> transform_silver_gold`, ejecutándose con frecuencia diaria.

### Alternativas Consideradas

- **AWS Step Functions**: Adecuado para workflows simples, pero limitado para pipelines de datos que requieren lógica condicional compleja, loops, o integración con el ecosistema Python/Spark.
- **Apache Airflow autogestionado en EC2**: Mayor control de configuración, pero introduce carga operativa significativa (mantención del scheduler, workers, base de datos) que no se justifica para un pipeline de 2 tareas.

### Consecuencias

- **Positivas**: Gestión nativa de reintentos, logging en CloudWatch, y dependencias entre tareas. Integración con Glue via `GlueJobOperator` para futuras iteraciones. Interfaz web para monitoreo.
- **Negativas**: Costo fijo base por entorno MWAA independientemente del volumen de ejecuciones. Curva de aprendizaje del ecosistema Airflow (variables, pools, conexiones).
