from pyspark.sql import SparkSession
import os

def get_spark_session(app_name):
    """
    Returns a SparkSession configured with both Apache Iceberg 
    and Spark-Kafka connectors for real-time streaming.
    """
    # Aligned for Spark 3.5.1 and Scala 2.12
    ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
    KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    
    PACKAGES = f"{ICEBERG_PKG},{KAFKA_PKG}"
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {PACKAGES} pyspark-shell'
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
