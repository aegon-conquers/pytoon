from spark_config import get_spark_session
from pyspark.sql.functions import regexp_replace, col, when, spark_partition_id
from pyspark.sql.types import DecimalType, DoubleType, FloatType, StringType, IntegerType
import subprocess
import time
import os
from datetime import datetime
from multiprocessing import Pool
from functools import partial
from pyspark.sql import functions as F

# Job-specific configuration
APP_NAME = "HiveToSingleStoreEdgeNodeOptimized"

# SingleStore table name
singlestore_table = "supplier"

# Path to MySQL configuration file (adjust as needed)
mysql_config_file = "/tmp/mysql.cnf"

# Output directory (local path instead of HDFS)
output_dir = "/user/me/data/usecase1/singlestore"

# Error handling configuration
max_retries = 3
retry_delay = 10  # seconds
log_file = "failed_loads.log"

# Parallelization configuration
num_parallel_processes_initial = 16  # For initial load (based on 48 cores)
num_parallel_processes_retry = 4     # For retries (conservative)

# Initialize SparkSession
spark = get_spark_session(APP_NAME)

# Step 1: Pull a small dataset from Hive for testing (limit to 1000 rows)
data_extract_query = """
    SELECT ap_file_id, vendor_id, supplier_id, sia_agg_id
    FROM your_hive_table
    WHERE ap_batch_id = '231775'
    LIMIT 1000  -- Small dataset for testing
"""
supplier_df_pre = spark.sql(data_extract_query)

# Step 2: Clean and transform data
selected_columns = supplier_df_pre.columns
supplier_df_clean = supplier_df_pre.select(*selected_columns)
for column in supplier_df_clean.columns:
    if isinstance(supplier_df_clean.schema[column].dataType, (DecimalType, DoubleType, FloatType)):
        supplier_df_clean = supplier_df_clean.withColumn(column, col(column).cast("double"))
    elif isinstance(supplier_df_clean.schema[column].dataType, StringType):
        supplier_df_clean = supplier_df_clean.withColumn(
            column, regexp_replace(col(column), "[~^]", " ")
        )
        supplier_df_clean = supplier_df_clean.withColumn(
            column, regexp_replace(col(column), "[^\\w\\s-]", "")
        )
    supplier_df_clean = supplier_df_clean.withColumn(
        column, when(col(column).isNull(), "").otherwise(col(column))
    )

# Step 3: Determine the number of partitions (small dataset, so use fewer partitions)
total_records = supplier_df_clean.count()
print(f"Total records: {total_records}")

num_partitions = min(total_records, 10)  # Use 10 partitions for small dataset
print(f"Number of partitions: {num_partitions}")

# Step 4: Repartition by supplier_id
supplier_df_repartitioned = supplier_df_clean.repartition(num_partitions, "supplier_id")

# Step 5: Transform DataFrame to RDD and format with ^ delimiter
def format_row(row):
    return "^".join(str(row[col]) if row[col] is not None else "NULL" for col in selected_columns)

rdd_formatted = supplier_df_repartitioned.rdd.map(lambda row: format_row(row))

# Step 6: Delete existing output directory if it exists
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)  # Remove existing directory
os.makedirs(output_dir, exist_ok=True)

# Step 7: Write to multiple files locally
start_write_time = time.time()
rdd_formatted.saveAsTextFile(output_dir)
write_time = time.time() - start_write_time
print(f"Writing files took {write_time} seconds")

# Step 8: Verify generated files
local_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith("part-")]
print(f"Generated {len(local_files)} data files in {output_dir}")

# Step 9: Define a function to run a LOAD DATA command
def run_load_command(file_path, attempt=1, mysql_config_file=mysql_config_file, 
                    singlestore_table=singlestore_table, selected_columns=selected_columns):
    load_command = [
        "mysql",
        f"--defaults-file={mysql_config_file}",
        "--local-infile=1",
        "-e",
        f"LOAD DATA LOCAL INFILE '{file_path}' INTO TABLE {singlestore_table} "
        f"FIELDS TERMINATED BY '^' ENCLOSED BY '' LINES TERMINATED BY '\\n' "
        f"({', '.join(selected_columns)}) "
        f"SET @@bulk_insert_buffer_size = 1024 * 1024 * 256"  # 256 MB buffer
    ]
    try:
        result = subprocess.run(load_command, check=True, capture_output=True, text=True)
        return (file_path, True, attempt, result.stdout, result.stderr if result.stderr else "")
    except subprocess.CalledProcessError as e:
        error_msg = f"File: {file_path}, Attempt: {attempt}, Error: {str(e)}, Stderr: {e.stderr}"
        log_message(error_msg)
        return (file_path, False, attempt, str(e), e.stderr if e.stderr else "")

# Step 10: Define a function to log messages
def log_message(message):
    with open(log_file, "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")

# Step 11: Initial load attempt (parallel)
start_load_time = time.time()
failed_files = []  # List of (file_path, attempt_count)
success_count = 0

with Pool(processes=num_parallel_processes_initial) as pool:
    load_func = partial(run_load_command, attempt=1)
    results = pool.map(load_func, local_files)

# Process initial load results
for file_path, success, attempt, stdout, stderr in results:
    if success:
        success_count += 1
        print(f"Successfully loaded {file_path}")
    else:
        failed_files.append((file_path, 1))  # (file_path, attempt_count)
        print(f"Failed to load {file_path} (attempt 1), will retry later")

# Step 12: Retry failed files (parallel)
retry_round = 1
while failed_files and retry_round <= max_retries:
    print(f"Retry round {retry_round} for {len(failed_files)} failed files")
    new_failed_files = []
    with Pool(processes=num_parallel_processes_retry) as pool:
        load_func = partial(run_load_command, attempt=retry_round + 1)
        results = pool.map(load_func, [file_path for file_path, _ in failed_files])
    
    # Process retry results
    for file_path, success, attempt, stdout, stderr in results:
        if success:
            success_count += 1
            print(f"Successfully loaded {file_path} on retry {retry_round}")
        else:
            new_attempt_count = attempt
            if new_attempt_count <= max_retries:
                new_failed_files.append((file_path, new_attempt_count))
                print(f"Failed to load {file_path} (attempt {new_attempt_count}), will retry again")
            else:
                log_message(f"File: {file_path}, Attempt: {new_attempt_count}, Status: Permanent Failure, Error: {stdout}")
                print(f"Permanently failed to load {file_path} after {max_retries} attempts")
    failed_files = new_failed_files
    retry_round += 1

# Step 13: Final reporting
load_time = time.time() - start_load_time
print(f"Loaded {success_count * (total_records // len(local_files))} rows into {singlestore_table} in {load_time} seconds")
print(f"Total files processed: {len(local_files)}")
print(f"Successfully loaded: {success_count}")
print(f"Permanently failed: {len(failed_files)}")
if failed_files:
    print("Permanently failed files:")
    for file_path, _ in failed_files:
        print(f"  {file_path}")

# Step 14: Clean up local directory
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

# Stop the SparkSession
spark.stop()
