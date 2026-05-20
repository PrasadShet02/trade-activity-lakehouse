from config.iceberg_config import get_spark_session

def main():
    spark = get_spark_session("TradeActivityLakehouse-SchemaEvolution")
    spark.sparkContext.setLogLevel("ERROR")

    print("Initiating zero-downtime schema evolution...")

    existing = [row['col_name'] for row in spark.sql("DESCRIBE local.db.trades").collect()]

    if 'broker_id' not in existing:
        spark.sql("ALTER TABLE local.db.trades ADD COLUMN broker_id STRING")
        print("Column 'broker_id' added.")

    if 'status' not in existing:
        spark.sql("ALTER TABLE local.db.trades ADD COLUMN status STRING")
        print("Column 'status' added.")

    if 'exchange' not in existing:
        spark.sql("ALTER TABLE local.db.trades ADD COLUMN exchange STRING")
        print("Column 'exchange' added.")

    print("Schema evolution complete. Updated schema:")
    spark.sql("DESCRIBE local.db.trades").show(truncate=False)

if __name__ == '__main__':
    main()
