from config.iceberg_config import get_spark_session

def main():
    spark = get_spark_session("TradeActivityLakehouse-CheckData")
    spark.sparkContext.setLogLevel("ERROR")

    print("Validating final ledger state in 'local.db.trades'...")
    df = spark.sql("SELECT * FROM local.db.trades ORDER BY event_time DESC")
    df.show(truncate=False)
    
    print(f"Data validation complete. Total records: {df.count()}")

if __name__ == '__main__':
    main()
