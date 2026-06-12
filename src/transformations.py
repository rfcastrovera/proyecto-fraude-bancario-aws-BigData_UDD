# -*- coding: utf-8 -*-
"""
MDS UDD 2026 - Big Data y Cloud Computing
Módulo de Transformaciones, Calidad, Enmascaramiento PII y Capas Silver/Gold (AWS Optimized)
Integrantes: Adrián y Ricardo
"""

from pyspark.sql.functions import col, regexp_replace, sum, count, max, when

def build_silver_layer(spark, bronze_path, silver_path):
    """
    Extrae la capa Bronze desde el almacenamiento (S3), aplica controles sanitarios
    y anonimiza datos conforme a los requerimientos de cumplimiento PCI-DSS exigidos.
    """
    print(f"[AWS-SILVER] Leyendo capa Bronze desde: {bronze_path}")
    df_bronze = spark.read.format("delta").load(bronze_path)
    
    # Aplicación de reglas de Gobierno y Privacidad Financiera (PII)
    print("[AWS-SILVER] Aplicando máscara regex sobre números de tarjetas expuestos...")
    df_silver = df_bronze \
        .filter(col("amount") > 0) \
        .withColumn("card_number_masked", regexp_replace("card_number", r"^(\d{4})\d{8}(\d{4})$", "$1-XXXX-XXXX-$2")) \
        .drop("card_number") # Remoción física inmediata de la columna crítica
        
    print(f"[AWS-SILVER] Guardando capa Silver segura en: {silver_path}")
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)
    print("[AWS-SILVER] Datos procesados con éxito en Capa Silver.")
    return df_silver

def build_gold_layer(spark, silver_path, gold_path):
    """
    Lee la capa Silver refinada, calcula agregaciones clave de negocio y guarda
    aplicando OPTIMIZACIÓN por Particionamiento, reduciendo costos de consulta en AWS Athena/QuickSight.
    """
    print(f"[AWS-GOLD] Leyendo capa Silver desde: {silver_path}")
    df_silver = spark.read.format("delta").load(silver_path)
    
    print("[AWS-GOLD] Extrayendo features agregadas diarias de comportamiento financiero...")
    df_gold = df_silver.groupBy("user_id", "transaction_date") \
        .agg(
            sum("amount").alias("total_monto_diario"),
            count("transaction_id").alias("cantidad_transacciones_diarias"),
            max("is_fraud_suspect").alias("contiene_sospecha_fraude")
        ) \
        .withColumn("perfil_alto_riesgo", when(col("cantidad_transacciones_diarias") > 12, 1).otherwise(0))
        
    print(f"[AWS-GOLD] Escribiendo capa Gold con particionamiento por campo temporal en: {gold_path}")
    # Optimizamos mediante particionamiento físico en S3/Disco
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .save(gold_path)
    print("[AWS-GOLD] Capa Gold optimizada correctamente.")
    return df_gold
