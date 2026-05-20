from config.iceberg_config import get_spark_session

def main():
    spark = get_spark_session("TradeActivityLakehouse-SchemaEvolution")
    spark.sparkContext.setLogLevel("ERROR")

    print("Evolving Iceberg schema to add 'broker_id'...")
    spark.sql("""
        ALTER TABLE local.db.trades
        ADD COLUMN broker_id STRING
    """)
    print("Schema evolved: 'broker_id' column added with zero downtime.")

if __name__ == '__main__':
    main()
