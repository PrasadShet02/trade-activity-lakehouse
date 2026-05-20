from config.iceberg_config import get_spark_session

def main():
    spark = get_spark_session("TradeActivityLakehouse-Inspector")
    spark.sparkContext.setLogLevel("ERROR")

    print("--- LAKEHOUSE AUDIT REPORT ---")

    # 1. Snapshot history: every committed micro-batch creates a new snapshot
    print("\nSnapshot History (Last 5 commits):")
    spark.sql("""
        SELECT committed_at, snapshot_id, operation
        FROM local.db.trades.snapshots
        ORDER BY committed_at DESC
        LIMIT 5
    """).show(truncate=False)

    # 2. Physical file inventory: exposes small-file bloat before compaction
    print("\nPhysical Data Files:")
    spark.sql("""
        SELECT file_path, file_size_in_bytes, record_count
        FROM local.db.trades.files
        LIMIT 10
    """).show(truncate=False)

    # 3. SCD Type 2 validation: active ledger count vs. full historical count
    print("\nActive Records (is_current = true):")
    spark.sql("SELECT COUNT(*) as active_count FROM local.db.trades WHERE is_current = true").show()

    print("Total Records (full historical ledger):")
    spark.sql("SELECT COUNT(*) as total_count FROM local.db.trades").show()

if __name__ == '__main__':
    main()
