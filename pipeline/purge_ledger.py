from config.iceberg_config import get_spark_session
from datetime import datetime, timedelta

def main():
    spark = get_spark_session("TradeActivityLakehouse-ForcePurge")
    spark.sparkContext.setLogLevel("ERROR")

    # Set the expiry threshold slightly in the future to capture all files
    # created during this session, overriding Iceberg's default safety window
    now_ms = int((datetime.now() + timedelta(minutes=5)).timestamp() * 1000)

    print("--- INITIATING FORCED PHYSICAL PURGE ---")

    print("\nStep 1: Expiring all snapshots, retaining only the latest...")
    spark.sql("""
        CALL local.system.expire_snapshots(
            table => 'local.db.trades',
            retain_last => 1
        )
    """).show(truncate=False)

    print(f"\nStep 2: Physically deleting orphaned files older than timestamp {now_ms}...")
    spark.sql(f"""
        CALL local.system.remove_orphan_files(
            table => 'local.db.trades',
            older_than => {now_ms}
        )
    """).show(truncate=False)

    print("\nForced purge complete. Storage layer is clean.")

if __name__ == '__main__':
    main()
