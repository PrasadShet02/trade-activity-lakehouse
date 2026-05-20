from config.iceberg_config import get_spark_session

def main():
    spark = get_spark_session("TradeActivityLakehouse-SchemaInit")
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.trades (
            trade_id STRING,
            symbol STRING,
            trade_type STRING,
            quantity INT,
            price DOUBLE,
            event_time TIMESTAMP,
            is_current BOOLEAN,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (symbol)
    """)
    print("Iceberg table 'local.db.trades' created successfully.")

if __name__ == '__main__':
    main()
