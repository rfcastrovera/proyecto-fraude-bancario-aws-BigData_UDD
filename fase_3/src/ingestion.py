import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def run_ingestion():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("Ingestion_Bronze") \
            .getOrCreate()

        df = spark.read.json("s3://bucket-origen-adrianespinoza/raw-data/")

        df_bronze = df.withColumn("_ingestion_ts", F.current_timestamp()) \
                      .withColumn("_source_file", F.input_file_name())

        df_bronze.write.mode("overwrite").parquet("s3://datalake-adrianespinoza/bronze/transactions/")
        logger.info("Ingesta Bronze completada.")

    except Exception as e:
        logger.error("Error en ingesta Bronze: %s", e)
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    run_ingestion()
