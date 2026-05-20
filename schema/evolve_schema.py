from pyspark.sql import SparkSession
import os

def main():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 pyspark-shell'
    
    spark = SparkSession.builder \
        .appName("TradeActivityLakehouse-SchemaEvolution") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("""
        ALTER TABLE local.db.trades
        ADD COLUMN broker_id STRING
    """)
    print("Schema evolved: 'broker_id' column added.")

if __name__ == '__main__':
    main()
