# Pipeline de Detección de Fraude en Tarjetas de Crédito (Sector Financiero) - AWS Architecture
## MDS UDD 2026 - Big Data y Cloud Computing
**Profesor:** Luis Castillo Faune  
**Integrantes del Equipo:** Adrián y Ricardo  
**Hito:** Fase 2 - Implementación y Optimización  
**Fecha de Entrega:** 12 de junio de 2026  

---

## 1. Descripción de la Solución e Infraestructura en la Nube (AWS)
Este repositorio contiene la implementación del pipeline de datos de extremo a extremo (End-to-End) para el procesamiento de transacciones financieras y detección de anomalías en el sector bancario utilizando los servicios de **Amazon Web Services (AWS)**. De acuerdo con la estrategia analítica seleccionada (Estrategia C), la solución simula el procesamiento de un histórico masivo de **15 millones de transacciones (~6.2 GB de datos crudos)** y un flujo incremental diario de **2 millones de operaciones transaccionales**.

El pipeline está construido bajo un patrón modular de **Medallion Architecture (Bronze -> Silver -> Gold)** en PySpark utilizando **Delta Lake** como formato de tabla transaccional ACID, montado directamente sobre buckets de **Amazon S3**.

---

## 2. Estructura Organizada del Repositorio
Cumpliendo rigurosamente la rúbrica de la Fase 2, el repositorio mantiene la siguiente jerarquía de componentes limpios:

```text
proyecto-fraude-bancario-aws/
├── README.md                  # Documentación principal, instrucciones AWS y análisis FinOps
├── notebooks/
│   └── 01_pipeline_demo.ipynb # Notebook ejecutable con el pipeline completo (Bronze-Silver-Gold)
├── src/
│   ├── ingestion.py           # Funciones de generación masiva sintética e ingesta en Amazon S3
│   └── transformations.py     # Lógica de limpieza, cumplimiento de seguridad PII y agregaciones
├── dags/
│   └── fraude_airflow_dag.py  # Orquestador compatible con MWAA (Managed Workflows for Apache Airflow)
└── docs/
    ├── spark_ui_screenshots/  # Capturas para evidenciar optimizaciones con métricas cuantitativas
    └── lineage_diagram.txt    # Documentación formal y estructurada del Linaje de Datos
```

---

## 3. Instrucciones de Reproducción Paso a Paso

### Requisitos Previos
- **Entorno local o en la nube:** Google Colab, Amazon EMR Notebooks, o una instancia de AWS SageMaker Studio.
- **Python:** Versión >= 3.8.
- **Librerías Core:** PySpark y Delta Lake.

### Ejecución
1. Clone el repositorio en su espacio de trabajo de desarrollo:
   ```bash
   git clone https://github.com/adrian-ricardo/proyecto-fraude-bancario-aws.git
   cd proyecto-fraude-bancario-aws
   ```
2. Instale los binarios necesarios del ecosistema Spark:
   ```bash
   pip install pyspark==3.4.1 delta-spark==2.4.0
   ```
3. Abra el notebook ejecutable localizado en `notebooks/01_pipeline_demo.ipynb` y ejecute todas las celdas secuencialmente. Por defecto, para asegurar reproducibilidad sin credenciales expuestas en la entrega, las escrituras iniciales se configuran en el directorio local `/tmp/datalake/`, replicando exactamente la estructura de rutas que se despliega en **Amazon S3** (`s3://bucket-banco-bronze/`, `s3://bucket-banco-silver/`, `s3://bucket-banco-gold/`).

---

## 4. Pipeline de Datos Medallion sobre AWS S3

El flujo de procesamiento distribuye las responsabilidades en las siguientes capas de almacenamiento desacoplado:

| Capa | Formato de Tabla | Equivalente Físico en AWS | Reglas Técnicas Aplicadas |
| :--- | :--- | :--- | :--- |
| **Bronze** | Delta Lake | `s3://banco-data-bronze/transactions/` | Almacenamiento crudo e inmutable (Append-only). Datos altamente sensibles sin enmascarar. |
| **Silver** | Delta Lake | `s3://banco-data-silver/transactions/` | Datos limpios y securizados. Filtrado de montos erróneos e **inyección de regex para enmascaramiento estricto PII (PCI-DSS)**. |
| **Gold** | Delta Lake | `s3://banco-data-gold/user_features/` | Tablas agregadas analíticas orientadas al negocio. **Optimizada mediante particionamiento físico por fecha**. |

---

## 5. Evidencia de Optimización Cuantitativa (Spark UI)

El pipeline incluye un escenario de pruebas para evaluar el impacto de las decisiones de arquitectura utilizando 15 millones de filas:
* **Métrica Sin Optimizar (Full Table Scan sobre archivos lineales):** `124.32 segundos`.
* **Métrica Optimizada (Uso de Delta Lake con Partition Pruning por fecha + `cache()` en memoria):** `14.15 segundos`.
* **Ganancia de Rendimiento:** **Reducción drástica del 88.6% del tiempo de procesamiento**. Esto optimiza directamente los costos de cómputo en la nube por segundo.

*Las evidencias visuales de la Spark UI que justifican este comportamiento (DAGs, almacenamiento en caché y lectura de particiones omitidas) se alojan en `docs/spark_ui_screenshots/`.*

---

## 6. Gobierno, Seguridad Financiera y Cumplimiento de Normativas

### Enmascaramiento de Datos Sensibles (PII)
Para dar cumplimiento a la normativa internacional de seguridad de tarjetas de pago (**PCI-DSS**), el pipeline mitiga riesgos interceptando el campo `card_number` en la transición de la capa Bronze a la Silver. Se aplica un reemplazo mediante expresiones regulares (`regexp_replace`) aislando los dígitos centrales: un registro como `4152889912345678` se convierte en `4152-XXXX-XXXX-5678`. El dato real expuesto es eliminado del flujo analítico subsiguiente.

### Control de Acceso y Trazabilidad (AWS IAM)
Se simula y documenta el principio de privilegios mínimos mediante políticas de AWS IAM:
1. **Roles de Ingesta:** Solo tienen acceso de escritura `PutObject` en el bucket S3 Bronze.
2. **Rol del Job de Spark (EMR/Glue):** Permisos de lectura/escritura transversales sobre Bronze, Silver y Gold buckets.
3. **Roles Analíticos (Data Science / Athena / QuickSight):** Acceso exclusivo de lectura `GetObject` restringido estrictamente al bucket **S3 Gold**, impidiendo el acceso a los datos sensibles iniciales.

El linaje detallado paso a paso se describe formalmente en `docs/lineage_diagram.txt`.

---

## 7. Análisis FinOps y Modelo de Costos en AWS

Utilizando la calculadora oficial de AWS (**AWS Pricing Calculator**), estimamos el costo financiero mensual para la operación de este pipeline de fraude bancario a escala de producción:

### Configuración del Cómputo: Amazon EMR (Elastic MapReduce) Cluster
- **1 Nodo Master:** `m5.xlarge` (4 vCPUs, 16 GB RAM) $ightarrow$ Costo base EC2 + EMR: ~$0.248 USD/hora.
- **2 Nodos Workers:** `m5.xlarge` (4 vCPUs, 16 GB RAM c/u) $ightarrow$ Costo base EC2 + EMR: ~$0.496 USD/hora combinados.
- **Estrategia Operativa:** Clúster Efímero (se crea automáticamente vía orquestador, procesa el lote diario de 2 millones de filas en ~30 minutos y se autodestruye).

### Desglose Mensual de Costos (USD)
1. **Cómputo Amazon EMR (0.5 horas/día x 30 días = 15 horas al mes):**
   - Costo mensual total de ejecución de clúster: **$11.16 USD**
2. **Almacenamiento Amazon S3 (Standard Storage - Estimación acumulada de 300 GB totales):**
   - Costo mensual S3 ($0.023 USD por GB): **$6.90 USD**
3. **Costo Mensual Total Estimado de la Solución en AWS:** **$18.06 USD / mes**

*Conclusión FinOps:* El uso de una arquitectura basada en clústeres efímeros junto con el almacenamiento de bajo costo en Amazon S3 proporciona una solución empresarial escalable con un gasto operacional extremadamente controlado, validando el criterio técnico-económico del curso.
