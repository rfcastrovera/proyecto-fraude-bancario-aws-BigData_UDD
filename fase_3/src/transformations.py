import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

def run_transformations():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("Transformations_Silver_Gold") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        logger.info("Leyendo Bronze desde s3://datalake-adrianespinoza/bronze/transactions/")
        df = spark.read.parquet("s3://datalake-adrianespinoza/bronze/transactions/")

        df_silver = df.filter(F.col("amount") > 0) \
                      .withColumn("card_masked", F.concat(F.substring(F.col("card"), 1, 4), F.lit("-XXXX-XXXX-"), F.substring(F.col("card"), -4, 4))) \
                      .drop("card")

        df_silver.write.format("delta").mode("overwrite") \
                 .partitionBy("transaction_date").save("s3://datalake-adrianespinoza/silver/transactions/")
        logger.info("Capa Silver escrita exitosamente.")

        df_gold = df_silver.groupBy("user_id", "transaction_date") \
                           .agg(F.count("*").alias("tx_count"),
                                 F.sum("amount").alias("daily_total")) \
                           .withColumn("is_high_risk", F.col("tx_count") > 12)

        df_gold.write.format("delta").mode("overwrite").save("s3://datalake-adrianespinoza/gold/risk_profile/")
        logger.info("Capa Gold escrita exitosamente.")

    except Exception as e:
        logger.error("Error en transformaciones Silver/Gold: %s", e)
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    run_transformations()
