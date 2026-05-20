from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from config.iceberg_config import get_spark_session

def process_batch(df, epoch_id):
    """Processes a micro-batch for SCD Type 2 updates using a single MERGE statement."""
    if df.isEmpty():
        return
        
    df.createOrReplaceTempView("incoming_trades")
    
    merge_query = """
    MERGE INTO local.db.trades t
    USING (
       SELECT trade_id as merge_key, * FROM incoming_trades
       UNION ALL
       SELECT NULL as merge_key, * FROM incoming_trades
    ) s
    ON t.trade_id = s.merge_key AND t.is_current = true
    WHEN MATCHED THEN
        UPDATE SET t.is_current = false, t.end_ts = current_timestamp()
    WHEN NOT MATCHED AND s.merge_key IS NULL THEN
        INSERT (trade_id, symbol, trade_type, quantity, price, event_time, is_current, start_ts, end_ts)
        VALUES (s.trade_id, s.symbol, s.trade_type, s.quantity, s.price, s.event_time, true, current_timestamp(), null)
    """
    df.sparkSession.sql(merge_query)
    print(f"Batch {epoch_id} merged successfully.")

def main():
    spark = get_spark_session("TradeActivityLakehouse-Consumer")
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("trade_id", StringType()),
        StructField("symbol", StringType()),
        StructField("trade_type", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("event_time", TimestampType())
    ])

    print("Starting PySpark Kafka Consumer...")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "trades") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "./checkpoints") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()
    
    print("Batch processing completed. Verifying results...")
    spark.sql("SELECT * FROM local.db.trades").show()

if __name__ == '__main__':
    main()
