# -*- coding: utf-8 -*-
"""
MDS UDD 2026 - Big Data y Cloud Computing
Módulo de Ingesta y Generación de Datos Sintéticos (AWS S3 Optimized)
Integrantes: Adrián y Ricardo
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr, when

def get_spark_session(app_name="AWSS3FraudIngestion"):
    """
    Inicializa una SparkSession configurada para soportar Delta Lake nativo.
    En AWS, este entorno corre sobre Amazon EMR o AWS Glue, apuntando los paths a s3a://
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

def generate_synthetic_transactions(spark, total_rows=15000000):
    """
    Genera eficientemente 15 millones de registros en paralelo estructurando datos analíticos de fraude.
    Cumple con el tamaño y volumen acordado para la Fase 2 del Proyecto.
    """
    print(f"[AWS-INGESTA] Generando concurrentemente {total_rows} registros financieros sintéticos...")
    
    df = spark.range(0, total_rows) \
        .withColumn("transaction_id", expr("uuid()")) \
        .withColumn("user_id", (rand() * 50000).cast("int")) \
        .withColumn("card_number", expr("concat('4152', lpad(cast(rand() * 1000000000000 as bigint), 12, '0'))")) \
        .withColumn("amount", (rand() * 4500 + 5).cast("decimal(10,2)")) \
        .withColumn("merchant_category", expr("case cast(rand()*5 as int) when 0 then 'Retail' when 1 then 'Banca' when 2 then 'Restaurantes' when 3 then 'Viajes' else 'E-Commerce' end")) \
        .withColumn("transaction_date", expr("date_add(current_date(), -cast(rand() * 45 as int))")) \
        .withColumn("is_fraud_suspect", when(rand() > 0.97, 1).otherwise(0))
        
    return df

def save_to_bronze_layer(df, path="/tmp/datalake/bronze/transactions"):
    """
    Persiste los datos crudos en la capa Bronze. En producción en AWS, 
    la ruta mapea a 's3://banco-data-bronze/transactions/'
    """
    print(f"[AWS-BRONZE] Almacenando registros crudos en formato Delta en: {path}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(path)
    print("[AWS-BRONZE] Ingesta completada con éxito.")
