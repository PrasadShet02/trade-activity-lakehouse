from config.iceberg_config import get_spark_session

def main():
    spark = get_spark_session("TradeActivityLakehouse-Optimize")
    spark.sparkContext.setLogLevel("ERROR")

    print("Running Iceberg maintenance procedures on 'local.db.trades'...")
    
    # 1. Compact small data files
    print("Compacting small data files...")
    spark.sql("CALL local.system.rewrite_data_files(table => 'local.db.trades')").show(truncate=False)
    
    # 2. Expire old snapshots
    print("Expiring old snapshots...")
    # Retaining the last 1 snapshot for safety in production environments
    spark.sql("CALL local.system.expire_snapshots(table => 'local.db.trades', retain_last => 1)").show(truncate=False)

    # 3. Clean up orphaned files
    print("Cleaning up orphaned files...")
    spark.sql("CALL local.system.remove_orphan_files(table => 'local.db.trades')").show(truncate=False)

    print("Iceberg storage compaction and maintenance automated successfully.")

if __name__ == '__main__':
    main()
