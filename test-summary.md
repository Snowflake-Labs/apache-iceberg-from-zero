
### Setup — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 0 | `# Initialize Spark session for data setup` |  |
| ✅ OK | 2 | `# Create the taxi namespace in Polaris catalog` | DataFrame[] |
| ✅ OK | 4 | `# Download NYC taxi data and upload to MinIO` | Downloading yellow_tripdata_2023-06.parquet... |
| ✅ OK | 6 | `# Copy raw data to taxi/trips_snapshot/data/ for snapshot example` | Copying raw/yellow_tripdata_2023-06.parquet -> taxi/trips_sn |
| ✅ OK | 8 | `# Verify raw data location` | Raw data: 18,747,273 rows |


### E1.1 - OpenLakehouse — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 5 | `with open('/home/jovyan/.sparkconf/spark-defaults.conf', 'r') as f:` | # Iceberg Spark Dependencies |
| ✅ OK | 7 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 9 | `# Create namespace (like a schema in traditional databases)` | Namespace 'demo' created! |
| ✅ OK | 11 | `spark.sql("""` | Sample data inserted! |
| ✅ OK | 13 | `spark.sql("""` | +---+-------+-----------+ |
| ✅ OK | 14 | `spark.sql("""` | +-----------+--------------+ |
| ✅ OK | 17 | `from trino.dbapi import connect` | Querying via Trino: |


### E1.2 - DataModeling — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 2 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 4 | `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.taxi")` | Namespace 'taxi' created! |
| ✅ OK | 6 | `import urllib.request` | Downloading yellow_tripdata_2023-06.parquet (~45MB)... |
| ✅ OK | 8 | `# Read all parquet files from MinIO` | Reading from: s3a://warehouse/raw/ |
| ✅ OK | 9 | `# Show sample data` | Sample records: |
| ✅ OK | 10 | `# Get date range and basic statistics` | Date range: |
| ✅ OK | 13 | `# Create unpartitioned table using CTAS` | Unpartitioned table created! |
| ✅ OK | 15 | `# Check the files metadata table` | Files in unpartitioned table: |
| ✅ OK | 16 | `# The entries metadata table exposes manifest-level entries (one row per data fi` | +------------+----------+ |
| ✅ OK | 17 | `# Check snapshots` | Snapshots: |
| ✅ OK | 20 | `spark.sql("""` | Monthly partitioned table created! |
| ✅ OK | 22 | `print("Files in monthly partitioned table:")` | Files in monthly partitioned table: |
| ✅ OK | 23 | `# Check partition statistics` | Partition statistics: |
| ✅ OK | 26 | `# Create daily partitioned table` | Daily partitioned table created! |
| ✅ OK | 28 | `# Check partition statistics - note we have many more partitions now` | Partition statistics (showing first 10): |
| ✅ OK | 29 | `# Count total partitions` | Total partitions in daily partitioned table: 189 |
| ✅ OK | 30 | `# Check file distribution across partitions` | File count per partition (sample): |
| ✅ OK | 34 | `# Create monthly partitioned table with sort order on pickup location` | Table structure created with sort order configured |
| ✅ OK | 36 | `# Check file metadata with bounds` | File statistics (showing readable metrics including min/max  |
| ✅ OK | 39 | `# Query without any filters - full scan` | Full scan query results: |
| ✅ OK | 42 | `# Query with date filter - partition pushdown` | Partition pushdown query results (August 2023 only): |
| ✅ OK | 45 | `# Query with location filter - metric pushdown` | Metric pushdown query results (specific locations): |
| ✅ OK | 48 | `# Query with both month and location filters` | Combined pushdown query results (August 2023, specific locat |
| ✅ OK | 51 | `import time` |  |
| ✅ OK | 54 | `# Example 1: 3 days + specific location` | Query: 3 days in mid-August, pickup location 237 |
| ✅ OK | 57 | `# Example 2: Full month` | Query: All of July 2023 |
| ✅ OK | 60 | `# Example 3: Single day + location range` | Query: Single day (Aug 15), 3 specific locations |
| ✅ OK | 63 | `# Example 4: Location only (no time filter)` | Query: All trips from location 237 (no time filter) |
| ✅ OK | 66 | `# Your custom query here!` |  |


### E2.1 - MovingExistingTables — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 2 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 4 | `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.migration")` | Namespace 'migration' created! |
| ✅ OK | 6 | `import boto3` | Downloading yellow_tripdata_2023-06.parquet (~45MB)... |
| ✅ OK | 9 | `# Create a sample CSV file and upload to MinIO` | CSV file uploaded to s3://warehouse/migration/csv_landing/cu |
| ✅ OK | 11 | `# Read CSV file from MinIO` | CSV Schema: |
| ✅ OK | 13 | `spark.sql("DROP TABLE IF EXISTS polaris.migration.customers_from_csv")` | Table created from CSV! |
| ✅ OK | 14 | `# Query the new Iceberg table` | +-----------+-------------+-----------+---------------+ |
| ✅ OK | 17 | `# View files in the table` | +----------------------------------------------------------- |
| ✅ OK | 20 | `# my_table_name = "polaris.migration.???"` |  |
| ✅ OK | 23 | `# Read the June Parquet file from our downloaded data` | Schema: |
| ✅ OK | 25 | `spark.sql("DROP TABLE IF EXISTS polaris.migration.taxi_from_ctas")` | Creating Iceberg table from Parquet using CTAS... |
| ✅ OK | 26 | `# Verify row count` | Iceberg table row count: 3,307,234 |
| ✅ OK | 29 | `# Helper: drop an Iceberg table via the Polaris REST API` | Cleaned up 12 existing files from migration/taxi_snapshot/ |
| ✅ OK | 31 | `# The snapshot procedure expects a registered table as input, not a raw file pat` | Running snapshot procedure... |
| ✅ OK | 32 | `# Verify the snapshot table` | +-----------+-------------------+-------------------+ |
| ✅ OK | 33 | `print("Snapshot table files:")` | Snapshot table files: |
| ✅ OK | 36 | `# snapshotted_table = "polaris.migration.taxi_from_snapshot"` |  |
| ✅ OK | 38 | `print("=" * 60)` | ============================================================ |
| ✅ OK | 40 | `# Copy July data from raw/ into the snapshot table's data directory` | Copying July data into snapshot table location... |
| ✅ OK | 41 | `# Check current state before adding files` | Before add_files: |
| ✅ OK | 43 | `# Use add_files to register the new Parquet file without rewriting data` | Adding July data using add_files procedure... |
| ✅ OK | 44 | `# Verify the addition` | After add_files: |
| ✅ OK | 45 | `# View all files now in the snapshot table` | +----------------------------------------------------------- |
| ✅ OK | 48 | `# INSERT the same July data into the CTAS table for comparison` | Inserting 2,907,108 rows using INSERT INTO... |
| ✅ OK | 50 | `# month = "08"` |  |
| ✅ OK | 52 | `# View snapshots for the snapshot table` | Snapshot table history: |
| ✅ OK | 53 | `# View snapshots for the CTAS table` | CTAS table history: |
| ✅ OK | 55 | `# Optional: Drop the tables if you want to start fresh` |  |


### E2.2 - BranchingAndTagging — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 2 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 4 | `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.timetravel")` | Namespace 'timetravel' created! |
| ✅ OK | 6 | `import boto3` | yellow_tripdata_2023-06.parquet already in MinIO, skipping d |
| ✅ OK | 9 | `spark.sql("DROP TABLE IF EXISTS polaris.timetravel.nyc_taxi")` | Snapshot 1: Loaded June 2023, 3,307,234 trips |
| ✅ OK | 11 | `spark.sql("""` | Snapshot 2: Added July 2023, 6,214,342 total trips |
| ✅ OK | 13 | `bad_fares = spark.sql("""` | Snapshot 3: Cleaned bad fares. 6,149,699 trips remaining (64 |
| ✅ OK | 15 | `print("Snapshot history:")` | Snapshot history: |
| ✅ OK | 18 | `snapshot_1 = snapshots_df.collect()[0]['snapshot_id']` | Snapshot 1 ID: 6281783365883038986 |
| ✅ OK | 20 | `snapshot_2 = snapshots_df.collect()[1]['snapshot_id']` | Snapshot 2 ID: 8969251371576074918 |
| ✅ OK | 22 | `print("Before cleanup (Snapshot 2) vs After cleanup (current):")` | Before cleanup (Snapshot 2) vs After cleanup (current): |
| ✅ OK | 24 | `# Pick two snapshots to compare (snapshot_1 and snapshot_2 are already defined a` |  |
| ✅ OK | 27 | `current_snapshot = snapshots_df.collect()[-1]['snapshot_id']` | Tagging snapshot 3988454544342882136 as 'post-cleanup' |
| ✅ OK | 29 | `print("Query using tag 'post-cleanup':")` | Query using tag 'post-cleanup': |
| ✅ OK | 31 | `print(f"Tagging snapshot {snapshot_2} as 'pre-cleanup'")` | Tagging snapshot 8969251371576074918 as 'pre-cleanup' |
| ✅ OK | 33 | `spark.sql("""` | +------------+------+-------------------+ |
| ✅ OK | 36 | `good_snapshot = snapshots_df.collect()[-1]['snapshot_id']` | Good snapshot ID: 3988454544342882136 |
| ✅ OK | 38 | `print(f"Rolling back to snapshot {good_snapshot}...")` | Rolling back to snapshot 3988454544342882136... |
| ✅ OK | 40 | `all_snapshots = spark.sql("""` | All snapshots (nothing was erased): |
| ✅ OK | 41 | `print(f"Jumping forward to the 'mistake' snapshot ({delete_snapshot})...")` | Jumping forward to the 'mistake' snapshot (87237690278749486 |
| ✅ OK | 42 | `print(f"Jumping back to the good snapshot ({good_snapshot})...")` | Jumping back to the good snapshot (3988454544342882136)... |
| ✅ OK | 44 | `# my_tag_name = "???"` |  |
| ✅ OK | 46 | `spark.sql("""` | WAP enabled on nyc_taxi table |
| ✅ OK | 48 | `pre_wap_count = spark.sql("SELECT COUNT(*) FROM polaris.timetravel.nyc_taxi").co` | Staged cleanup of zero-distance trips with wap.id = 'zero-di |
| ✅ OK | 49 | `prod_count = spark.sql("SELECT COUNT(*) FROM polaris.timetravel.nyc_taxi").colle` | Before WAP write: 6,149,699 trips |
| ✅ OK | 51 | `wap_snapshot_id = spark.sql("""` | WAP Snapshot ID: 6017144552921319069 |
| ✅ OK | 53 | `spark.sql("""` | WAP changes published! 6,060,101 trips in production |
| ✅ OK | 55 | `spark.conf.set("spark.wap.id", "bad-cleanup")` | Staged a bad cleanup with wap.id = 'bad-cleanup' |
| ✅ OK | 56 | `bad_snapshot_id = spark.sql("""` | Bad WAP Snapshot ID: 1995845756523205653 |
| ✅ OK | 57 | `spark.sql(f"""` | Bad WAP snapshot discarded! Production unchanged: 6,060,101  |
| ✅ OK | 58 | `spark.sql("""` | WAP disabled on nyc_taxi table |
| ✅ OK | 60 | `# my_wap_id_name = "???"` |  |
| ✅ OK | 63 | `spark.sql("""` | Branch 'august_load' created! |
| ✅ OK | 65 | `spark.sql("""` | Added August 2023 data to the 'august_load' branch |
| ✅ OK | 66 | `branch_count = spark.sql("""` | Branch 'august_load': 8,884,310 trips (June + July + August) |
| ✅ OK | 68 | `spark.sql("""` | +--------------+-------------------+------------------+ |
| ✅ OK | 69 | `merged_count = spark.sql("SELECT COUNT(*) FROM polaris.timetravel.nyc_taxi").col` | Main branch after merge: 8,884,310 trips (June + July + Augu |
| ✅ OK | 71 | `# my_branch = "???"` |  |
| ✅ OK | 73 | `print("Complete snapshot history:")` | Complete snapshot history: |
| ✅ OK | 75 | `# Optional: Drop table to start fresh` |  |


### E2.3 - SchemaAndPartitionEvolution — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 2 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 4 | `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.evolution")` | Namespace 'evolution' created! |
| ✅ OK | 6 | `def load_iceberg_table(table_name):` | Iceberg Java API helpers loaded! |
| ✅ OK | 8 | `import boto3` | yellow_tripdata_2023-06.parquet already in MinIO, skipping d |
| ✅ OK | 11 | `spark.sql("DROP TABLE IF EXISTS polaris.evolution.nyc_taxi")` | Table created with 3,307,234 trips (format-version 3) |
| ✅ OK | 12 | `print("Initial schema:")` | Initial schema: |
| ✅ OK | 14 | `start = time.time()` | Column added in 0.122 seconds (metadata-only operation!) |
| ✅ OK | 15 | `print("Schema after adding tip_percentage:")` | Schema after adding tip_percentage: |
| ✅ OK | 16 | `print("Old rows have NULL for the new column:")` | Old rows have NULL for the new column: |
| ✅ OK | 18 | `spark.sql("""` | July data inserted with tip_percentage populated! |
| ✅ OK | 19 | `print("New rows have tip_percentage, old rows still have NULL:")` | New rows have tip_percentage, old rows still have NULL: |
| ✅ OK | 21 | `Types = spark._jvm.org.apache.iceberg.types.Types` | Column 'surge_multiplier' added with initial default = 1.0 |
| ✅ OK | 22 | `print("Compare with tip_percentage (added via Spark SQL, no default):")` | Compare with tip_percentage (added via Spark SQL, no default |
| ✅ OK | 24 | `spark.sql("ALTER TABLE polaris.evolution.nyc_taxi DROP COLUMN surge_multiplier")` | Dropped surge_multiplier (cleanup for next section) |
| ✅ OK | 26 | `# my_column = "???"` |  |
| ✅ OK | 29 | `print("Schema with Iceberg field IDs:")` | Schema with Iceberg field IDs: |
| ✅ OK | 31 | `spark.sql("""` | store_and_fwd_flag column dropped! |
| ✅ OK | 32 | `print("Schema after dropping store_and_fwd_flag (note: field ID 7 is now retired` | Schema after dropping store_and_fwd_flag (note: field ID 7 i |
| ✅ OK | 34 | `spark.sql("""` | store_and_fwd_flag column re-added! |
| ✅ OK | 35 | `print("Field IDs after re-adding store_and_fwd_flag:")` | Field IDs after re-adding store_and_fwd_flag: |
| ✅ OK | 39 | `# my_column = "extra"  # pick any column` |  |
| ✅ OK | 41 | `spark.sql("""` | Column renamed from 'fare_amount' to 'base_fare' |
| ✅ OK | 42 | `print("Schema after rename (field ID unchanged, only the name mapping changed):"` | Schema after rename (field ID unchanged, only the name mappi |
| ✅ OK | 43 | `print("Query using new name:")` | Query using new name: |
| ✅ OK | 46 | `# old_name = "tip_amount"` |  |
| ✅ OK | 49 | `spark.sql("DROP TABLE IF EXISTS polaris.evolution.taxi_evolving")` | Unpartitioned table created with 3,307,234 trips |
| ✅ OK | 50 | `print("Files (unpartitioned):")` | Files (unpartitioned): |
| ✅ OK | 52 | `spark.sql("""` | Day partitioning added! New writes will be partitioned by da |
| ✅ OK | 54 | `spark.sql("""` | July data inserted (will be day-partitioned)! |
| ✅ OK | 55 | `print("Files after adding day partitioning (mix of spec 0 and spec 1):")` | Files after adding day partitioning (mix of spec 0 and spec  |
| ✅ OK | 58 | `spark.sql("""` | Partition changed from days to months! |
| ✅ OK | 59 | `spark.sql("""` | August data inserted (will be month-partitioned)! |
| ✅ OK | 60 | `print("Files with three partition specs (5 per spec):")` | Files with three partition specs (5 per spec): |
| ✅ OK | 62 | `spark.sql("""` | Switched back to day partitioning! |
| ✅ OK | 63 | `spark.sql("""` | September data inserted (day-partitioned again)! |
| ✅ OK | 64 | `print("Files after reverting to day partitioning (5 per spec):")` | Files after reverting to day partitioning (5 per spec): |
| ✅ OK | 67 | `# my_transform = "hours(tpep_pickup_datetime)"  # or bucket(16, PULocationID), e` |  |
| ✅ OK | 69 | `# Optional: Drop tables to start fresh` |  |


### E3.1 - Ingestion — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 2 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 4 | `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.ingestion")` | Namespace 'ingestion' created! |
| ✅ OK | 6 | `import boto3` | yellow_tripdata_2023-06.parquet already in MinIO, skipping d |
| ✅ OK | 9 | `spark.sql("DROP TABLE IF EXISTS polaris.ingestion.taxi_trips")` | Taxi trips table created! |
| ✅ OK | 10 | `june_df = spark.read.parquet("s3a://warehouse/raw/yellow_tripdata_2023-06.parque` | Batch loading 3,307,234 June trips... |
| ✅ OK | 11 | `print("Snapshots after batch write:")` | Snapshots after batch write: |
| ✅ OK | 13 | `import shutil, os` | Streaming helper loaded! |
| ✅ OK | 15 | `streaming_snapshots = run_streaming_ingest(` | Staging 2,907,108 rows as 10 files... |
| ✅ OK | 16 | `snapshot_count = spark.sql("""` | Total snapshots: 7 |
| ✅ OK | 19 | `# Try with many small micro-batches (more snapshots, more files):` |  |
| ✅ OK | 22 | `for mode_suffix, mode_value in [('cow', 'copy-on-write'), ('mor', 'merge-on-read` | COW and MOR tables created with 3,307,234 rows each (~49.7 M |
| ✅ OK | 24 | `start = time.time()` | COW UPDATE took 9.156 seconds |
| ✅ OK | 25 | `start = time.time()` | MOR UPDATE took 2.628 seconds |
| ✅ OK | 27 | `print("COW data files:")` | COW data files: |
| ✅ OK | 28 | `print("MOR data and delete files:")` | MOR data and delete files: |
| ✅ OK | 31 | `scan_query = "SELECT SUM(fare_amount) FROM {}"` | COW READ: 0.262s |
| ✅ OK | 33 | `def benchmark_cow_vs_mor(update_pct):` | benchmark_cow_vs_mor() helper ready |
| ✅ OK | 34 | `# benchmark_cow_vs_mor(update_pct=???)  # <-- Try: 1, 10, 50, 90` |  |
| ✅ OK | 37 | `spark.sql("DROP TABLE IF EXISTS polaris.ingestion.taxi_partitioned")` | Partitioned table created with 3,307,234 trips |
| ✅ OK | 38 | `bad_fares = spark.sql("""` | Deleted 32,502 bad-fare trips in 2.400 seconds |
| ✅ OK | 40 | `spark.sql("DROP TABLE IF EXISTS polaris.ingestion.taxi_meta_delete")` | Table created: 3,307,234 trips across 35 day-partitions (one |
| ✅ OK | 42 | `print("File-level readable metrics for tpep_pickup_datetime:\n")` | File-level readable metrics for tpep_pickup_datetime: |
| ✅ OK | 44 | `rows_before = spark.sql("SELECT COUNT(*) FROM polaris.ingestion.taxi_meta_delete` | Deleted 123,258 rows (entire June 1 partition) in 0.186s |
| ✅ OK | 45 | `print("Snapshot summary for the delete operation:\n")` | Snapshot summary for the delete operation: |
| ✅ OK | 48 | `spark.sql("DROP TABLE IF EXISTS polaris.ingestion.taxi_corrections")` | Corrections table created with 1,000 trips |
| ✅ OK | 49 | `corrections = spark.sql("""` | Prepared 100 fare corrections (5% increase) |
| ✅ OK | 50 | `spark.sql("""` | MERGE completed! |
| ✅ OK | 51 | `print("Corrections table after MERGE:")` | Corrections table after MERGE: |
| ✅ OK | 53 | `# Step 1: Create a corrections view. Change the column and factor` |  |
| ✅ OK | 56 | `from concurrent.futures import ThreadPoolExecutor, as_completed` | run_concurrent_sqls() helper ready |
| ✅ OK | 58 | `print("Concurrently updating trips on two different days...\n")` | Concurrently updating trips on two different days... |
| ✅ OK | 61 | `print("Concurrently updating the SAME partition (June 1) from two writers...\n")` | Concurrently updating the SAME partition (June 1) from two w |
| ✅ OK | 64 | `spark.sql("DROP TABLE IF EXISTS polaris.ingestion.taxi_retry_demo")` | Table created with commit.retry.num-retries = 1 |
| ✅ OK | 66 | `spark.sql("""` | Set commit.retry.num-retries = 20 |
| ✅ OK | 69 | `spark.sql("DROP TABLE IF EXISTS polaris.ingestion.taxi_merge_demo")` | Merge demo table and source view ready |
| ✅ OK | 71 | `print("Case 1: File-scoping predicate IN the ON clause\n")` | Case 1: File-scoping predicate IN the ON clause |
| ✅ OK | 73 | `print("Case 2: No file-scoping predicate in the ON clause (only in WHEN MATCHED)` | Case 2: No file-scoping predicate in the ON clause (only in  |
| ✅ OK | 76 | `from pyspark.sql.functions import col` | Unpartitioned table created, sorted by fare_amount into 5 fi |
| ✅ OK | 77 | `# Step 1: Pick two fare ranges that each fall within a DIFFERENT file` |  |
| ✅ OK | 79 | `# Optional: Drop tables` |  |


### E3.2 - MaintenanceProcedures — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 2 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 4 | `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.maintenance")` | Namespace 'maintenance' created! |
| ✅ OK | 6 | `import boto3` | yellow_tripdata_2023-06.parquet already in MinIO, skipping d |
| ✅ OK | 8 | `spark.sql("DROP TABLE IF EXISTS polaris.maintenance.taxi_trips")` | Taxi trips table created! |
| ✅ OK | 9 | `june_df = spark.read.parquet("s3a://warehouse/raw/yellow_tripdata_2023-06.parque` | Writing 3,307,234 rows in 100 small batches (33,072 rows eac |
| ✅ OK | 11 | `print("File statistics BEFORE compaction:")` | File statistics BEFORE compaction: |
| ✅ OK | 12 | `print("Manifest statistics:")` | Manifest statistics: |
| ✅ OK | 13 | `snapshot_count = spark.sql("""` | Total snapshots: 100 |
| ✅ OK | 17 | `print("Manifest statistics BEFORE compaction:")` | Manifest statistics BEFORE compaction: |
| ✅ OK | 19 | `planning_query = "SELECT COUNT(*) FROM polaris.maintenance.taxi_trips WHERE fare` | Planning query (0 matching files) with 100 manifests: |
| ✅ OK | 21 | `print("Compacting manifests...")` | Compacting manifests... |
| ✅ OK | 22 | `print("Manifest statistics AFTER compaction:")` | Manifest statistics AFTER compaction: |
| ✅ OK | 23 | `plan_times_after = []` | Same planning query with 1 manifest: |
| ✅ OK | 26 | `current_manifests = spark.sql("SELECT COUNT(*) FROM polaris.maintenance.taxi_tri` | Manifests in current snapshot: 1 |
| ✅ OK | 31 | `file_count_before = spark.sql("SELECT COUNT(*) FROM polaris.maintenance.taxi_tri` | Full scan with 600 small files: |
| ✅ OK | 32 | `print("Compacting data files...")` | Compacting data files... |
| ✅ OK | 34 | `print("File statistics AFTER compaction:")` | File statistics AFTER compaction: |
| ✅ OK | 36 | `file_count_after = spark.sql("SELECT COUNT(*) FROM polaris.maintenance.taxi_trip` | Full scan with 7 compacted files: |
| ✅ OK | 38 | `# Note: The micro-batch pattern above uses .limit() which pulls from the head of` | Files per partition after compaction: |
| ✅ OK | 40 | `current_files = spark.sql("SELECT COUNT(*) FROM polaris.maintenance.taxi_trips.f` | Data files in current snapshot: 7 |
| ✅ OK | 43 | `# my_target_size = '5242880'  # 5 MB. Try '52428800' (50 MB) too` |  |
| ✅ OK | 46 | `before_count = spark.sql("""` | Snapshots BEFORE expiration: 102 |
| ✅ OK | 47 | `current_files = spark.sql("SELECT COUNT(*) FROM polaris.maintenance.taxi_trips.f` | Current Snapshot   All Snapshots |
| ✅ OK | 50 | `print("Expiring all snapshots except the most recent one...")` | Expiring all snapshots except the most recent one... |
| ✅ OK | 51 | `after_count = spark.sql("""` | Snapshots AFTER expiration: 1 |
| ✅ OK | 52 | `post_all_files = spark.sql("SELECT COUNT(*) FROM polaris.maintenance.taxi_trips.` | Before Expire   After Expire    Removed |
| ✅ OK | 55 | `orphan_key = 'maintenance/taxi_trips/data/orphan-file-12345.parquet'` | Created fake orphan file: s3://warehouse/maintenance/taxi_tr |
| ✅ OK | 57 | `from datetime import datetime, timedelta` | Removing orphan files... |
| ✅ OK | 58 | `try:` | Orphan file was successfully removed! |
| ✅ OK | 60 | `tbl = "polaris.maintenance.taxi_trips"` | ============================================================ |
| ✅ OK | 62 | `# my_table = "polaris.maintenance.my_test_table"` |  |
| ✅ OK | 64 | `# Optional: Drop table` |  |


### E3.3 - TableModelingAndIngestion — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 2 | `from pyspark.sql import SparkSession` | Spark 4.0.1 initialized! |
| ✅ OK | 4 | `spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.modeling")` | Namespace 'modeling' created! |
| ✅ OK | 6 | `import boto3` | yellow_tripdata_2023-06.parquet already in MinIO, skipping d |
| ✅ OK | 7 | `taxi_df = spark.read.parquet(` | Loaded 11,885,273 taxi trips (June-September 2023) for testi |
| ✅ OK | 10 | `print("Creating UNPARTITIONED table...")` | Creating UNPARTITIONED table... |
| ✅ OK | 11 | `print("Unpartitioned file statistics:")` | Unpartitioned file statistics: |
| ✅ OK | 13 | `print("Creating DAILY PARTITIONED table...")` | Creating DAILY PARTITIONED table... |
| ✅ OK | 14 | `print("Daily partition file statistics:")` | Daily partition file statistics: |
| ✅ OK | 16 | `print("Creating MONTHLY PARTITIONED table...")` | Creating MONTHLY PARTITIONED table... |
| ✅ OK | 17 | `print("Monthly partition file statistics:")` | Monthly partition file statistics: |
| ✅ OK | 19 | `query = """` | Query: All trips on June 15, 2023 |
| ✅ OK | 20 | `query2 = """` | Query: Trips on June 15, 2-3 PM |
| ✅ OK | 22 | `print("=" * 60)` | ============================================================ |
| ✅ OK | 25 | `# my_query = """` |  |
| ✅ OK | 26 | `for t in ['taxi_unpartitioned', 'taxi_daily', 'taxi_monthly']:` | Part 1 tables cleaned up! |
| ✅ OK | 29 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_unsorted")` | Unsorted table created! |
| ✅ OK | 31 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_sorted")` | Sorted table created (sorted by PULocationID)! |
| ✅ OK | 33 | `query = """` | Query: Trips from location 132 on June 15 |
| ✅ OK | 35 | `# my_sort_key = "fare_amount"  # or trip_distance, DOLocationID, etc.` |  |
| ✅ OK | 36 | `for t in ['taxi_unsorted', 'taxi_sorted']:` | Part 2 tables cleaned up! |
| ✅ OK | 39 | `from pyspark.sql.functions import to_date, min as spark_min, max as spark_max` | Date range per source parquet file: |
| ✅ OK | 40 | `print("Trips per day across the dataset (first and last 5 days shown):")` | Trips per day across the dataset (first and last 5 days show |
| ✅ OK | 43 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_dist_hash")` | Writing with HASH distribution mode (bucketed by PULocationI |
| ✅ OK | 44 | `print("Hash distribution file stats:")` | Hash distribution file stats: |
| ✅ OK | 46 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_dist_range")` | Writing with RANGE distribution mode (bucketed + sorted by f |
| ✅ OK | 47 | `print("Range distribution file stats:")` | Range distribution file stats: |
| ✅ OK | 49 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_dist_none")` | Writing with NONE distribution mode (bucketed by PULocationI |
| ✅ OK | 50 | `print("None distribution file stats:")` | None distribution file stats: |
| ✅ OK | 52 | `hash_stats = spark.sql("SELECT COUNT(*) as f FROM polaris.modeling.taxi_dist_has` | ============================================================ |
| ✅ OK | 55 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_dist_hash_day")` | Writing with HASH distribution mode (daily partition)... |
| ✅ OK | 56 | `print("Hash distribution (daily) file stats:")` | Hash distribution (daily) file stats: |
| ✅ OK | 58 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_dist_none_day")` | Writing with NONE distribution mode (daily partition, data i |
| ✅ OK | 59 | `print("None distribution (daily, pre-sorted) file stats:")` | None distribution (daily, pre-sorted) file stats: |
| ✅ OK | 61 | `hash_day_stats = spark.sql("SELECT COUNT(*) as f FROM polaris.modeling.taxi_dist` | ============================================================ |
| ✅ OK | 64 | `spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_sort_decay")` | Created unpartitioned table sorted by fare_amount (8 MB targ |
| ✅ OK | 65 | `benchmark_query = """` | Inserting June data repeatedly and benchmarking... |
| ✅ OK | 67 | `print("Compacting with sort order...")` | Compacting with sort order... |
| ✅ OK | 70 | `# spark.sql("DROP TABLE IF EXISTS polaris.modeling.taxi_combined")` |  |
| ✅ OK | 72 | `taxi_df.unpersist()` | Cleanup complete! |


### Module1_Apache_Iceberg_Fundamentals — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 3 | `# Initialize Spark session for working with Iceberg tables` |  |
| ✅ OK | 7 | `# Display Spark configuration showing how to connect to Catalog (Polaris) and St` | # Iceberg Spark Dependencies |
| ✅ OK | 9 | `# Create an unpartitioned Iceberg table from the NYC taxi Parquet files` | DataFrame[] |
| ✅ OK | 10 | `# Query the first 3 columns to verify the table was created successfully` | +--------+--------------------+---------------------+ |
| ✅ OK | 12 | `# Use the .files metadata table to inspect data files and aggregated table stati` | +----------------------------------------------------------- |
| ✅ OK | 14 | `# Create a table with hidden partitioning using the days() transform` | DataFrame[] |
| ✅ OK | 15 | `# Query with timestamp filter - Iceberg automatically applies partition pruning` | +-----------+--------+-------------------+------------------ |
| ✅ OK | 17 | `# View column metrics (min/max bounds) stored in manifest files for predicate pu` | +----------------------------------------------------------- |
| ✅ OK | 19 | `(empty)` |  |


### Module2_Taking_Advantage_of_Iceberg_Tables — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 3 | `# Initialize Spark session for Module 2 examples` |  |
| ✅ OK | 5 | `# Configure MinIO client for S3 operations` |  |
| ✅ OK | 7 | `# Clean up any November data to ensure it's not present during snapshot` | Deleted November data from trips_snapshot/data/ |
| ✅ OK | 9 | `# Create temporary table in session catalog, then snapshot it to create Iceberg ` | +--------------------+ |
| ✅ OK | 11 | `# Verify the snapshot created successfully and view file metadata` | +-----------+-------------------+-------------------+ |
| ✅ OK | 13 | `# Download November 2023 taxi data for add_files demonstration` | Downloading yellow_tripdata_2023-11.parquet... |
| ✅ OK | 14 | `# Use add_files to incrementally add new Parquet files to an existing Iceberg ta` | +-----------------+-----------------------+ |
| ✅ OK | 15 | `# Check the files metadata table to see the new files` | +--------+ |
| ✅ OK | 16 | `# Verify the incremental file additions worked correctly` | +-----------+-------------------+-------------------+ |
| ✅ OK | 18 | `import tempfile` |  |
| ✅ OK | 19 | `# Migrate CSV data to Iceberg using CTAS with proper schema transformation` | DataFrame[] |
| ✅ OK | 20 | `# View our CSV Data now in an Iceberg Table` | +--------+--------------------+---------------------+------- |
| ✅ OK | 22 | `# Create trips_by_month table with WAP enabled for Write-Audit-Publish pattern` | +--------+ |
| ✅ OK | 23 | `# Add a new column to the schema` | DataFrame[] |
| ✅ OK | 24 | `# Enable Write-Audit-Publish (WAP) and perform UPDATE operation` |  |
| ✅ OK | 25 | `# Query the production table WITHOUT the WAP snapshot - correct_fare should be N` | Data in production table (WITHOUT WAP changes - correct_fare |
| ✅ OK | 26 | `# Find the snapshot_id that corresponds to our wap.id` | WAP Snapshot ID: 2865662166882524531 |
| ✅ OK | 27 | `# Option 1: Drop the WAP snapshot if validation fails` |  |
| ✅ OK | 28 | `# Option 2: Publish the WAP snapshot to make it visible to all readers` | +--------------------+------------+------------+ |
| ✅ OK | 29 | `# Drop the correct_fare column before branching` | DataFrame[] |
| ✅ OK | 30 | `# Create a branch for experimentation` | DataFrame[] |
| ✅ OK | 31 | `# Insert data to the branch only` | DataFrame[] |
| ✅ OK | 32 | `# Query the branch to verify changes - shows the 3 new rows` | Branch (experiment_branch) with new data: |
| ✅ OK | 33 | `# Merge the branch back to main` | +--------------+-------------------+-------------------+ |
| ✅ OK | 34 | `# Query the branch to verify changes - shows the 3 new rows` | Branch (experiment_branch) : |
| ✅ OK | 35 | `# Create a tag to mark an important snapshot` | DataFrame[] |
| ✅ OK | 36 | `# Query using the tag` | +----------+ |
| ✅ OK | 37 | `# Rollback to a previous snapshot for disaster recovery` | +-----------------------+-------------------+---------+ |
| ✅ OK | 38 | `# Rollback to the snapshot before the branch merge` | Rolling back to snapshot: 2865662166882524531 |
| ✅ OK | 40 | `# Setup: Create a fresh trips_by_day table for schema evolution demonstrations` | DataFrame[] |
| ✅ OK | 41 | `# Add a column to the table` | DataFrame[] |
| ✅ OK | 42 | `# Rename a column` | DataFrame[] |
| ✅ OK | 43 | `# Drop a column` | DataFrame[] |
| ✅ OK | 45 | `# Example: Change partition spec on an existing table` | DataFrame[] |
| ✅ OK | 46 | `# View current partition spec` | +--------------------+--------------------+-------+ |
| ✅ OK | 47 | `# Add day partitioning to the table` | DataFrame[] |
| ✅ OK | 48 | `# View current partition spec` | +--------------------+--------------------+-------+ |
| ✅ OK | 49 | `# Insert new data - it will use the new partition spec` | DataFrame[] |
| ✅ OK | 50 | `# View partitions now - mix of unpartitioned and day-partitioned data` | +------------+ |
| ✅ OK | 51 | `# Partition reduction: Remove a partition transform` | DataFrame[] |
| ✅ OK | 52 | `# Partition addition: Add multiple partition transforms` | DataFrame[] |
| ✅ OK | 53 | `# Insert new data - it will use the new partition spec` | DataFrame[] |
| ✅ OK | 54 | `spark.sql("REFRESH TABLE polaris.taxi.trips_evolving")` | DataFrame[] |
| ✅ OK | 55 | `# View partitions now - mix of unpartitioned and day-partitioned data and month ` | +------------------+ |
| ✅ OK | 57 | `# Demonstrate automatic predicate transformation` | +----------+-------------------+-------------------+ |


### Module3_Operating_and_Optimizing_Apache_Iceberg — ✅ PASS

| Status | Cell | Source | Output |
|--------|------|--------|--------|
| ✅ OK | 3 | `from pyspark.sql import SparkSession` | Spark version: 4.0.1 |
| ✅ OK | 6 | `# Setup: Create trips_by_day table with a few days of data for demonstration` | DataFrame[] |
| ✅ OK | 7 | `# Insert operation - adds new rows to the table` | DataFrame[] |
| ✅ OK | 8 | `# Delete operation with partition filter` | DataFrame[] |
| ✅ OK | 10 | `# Overwrite operation - atomic delete + insert` | DataFrame[] |
| ✅ OK | 11 | `# Copy-on-Write (COW) mode update` | DataFrame[] |
| ✅ OK | 12 | `# View the result of COW update` | +--------------------+------------+-----------+ |
| ✅ OK | 13 | `# Merge-on-Read (MOR) mode update` | DataFrame[] |
| ✅ OK | 14 | `# View the result of MOR update` | +--------------------+------------+----------+ |
| ✅ OK | 16 | `# View delete files in metadata` | +---------+------------+--------------------+ |
| ✅ OK | 18 | `# Setup: Create trips_by_month_maintenance table with multiple snapshots` | CTAS Complete |
| ✅ OK | 19 | `# View all snapshots - should show 5 snapshots (1 CTAS + 4 INSERT operations)` | +-----------------------+-------------------+---------+----- |
| ✅ OK | 20 | `# Expire old snapshots to prevent metadata.json bloat` | +------------------------+---------------------------------- |
| ✅ OK | 21 | `# View remaining snapshots after expiration - should only show last 2 snapshots` | +-----------------------+-------------------+---------+ |
| ✅ OK | 23 | `# View manifests before compaction` | +-----------------------------------------------------+----- |
| ✅ OK | 24 | `# Rewrite manifests to consolidate small manifest files` | +-------------------------+---------------------+ |
| ✅ OK | 25 | `# View manifests after compaction` | +----------------------------------------------------------- |
| ✅ OK | 26 | `# Rewrite Small Files so Example is Clearer` | +--------------------------+----------------------+--------- |
| ✅ OK | 27 | `# Perform several small writes to create many small files` | Completed 7 small daily inserts |
| ✅ OK | 28 | `# View small files before compaction` | +----------------+-----------------+ |
| ✅ OK | 29 | `# Rewrite data files to compact small files into optimally-sized files` | +--------------------------+----------------------+--------- |
| ✅ OK | 31 | `# Verify no small files remain after compaction` | +----------------+ |
| ✅ OK | 32 | `# Remove orphan files - only run when necessary` | +--------------------+ |
| ✅ OK | 34 | `# spark.sql("""` |  |
| ✅ OK | 36 | `# Define a sort order on a table` | DataFrame[] |
| ✅ OK | 37 | `# Insert data into the sorted table` | DataFrame[] |
| ✅ OK | 38 | `# View min/max bounds for sorted columns` | +------------+------------+----------------+---------------- |
| ✅ OK | 39 | `# Example: Partition by month (low cardinality)` | DataFrame[] |
| ✅ OK | 40 | `# View partition statistics for monthly partitioning` | +----------+-------------+----------------+ |
| ✅ OK | 41 | `# Example: Partition by day (high cardinality)` | DataFrame[] |
| ✅ OK | 43 | `# View partition statistics for daily partitioning` | +----------+-------------+----------------+ |
| ✅ OK | 44 | `# Compare overall file statistics` | +--------------+-----------+----------------+ |
| ✅ OK | 45 | `(empty)` |  |

