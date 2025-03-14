from spark_config import get_spark_session
from pyspark.sql.functions import regexp_replace, col, when, spark_partition_id
from pyspark.sql.types import DecimalType, DoubleType, FloatType, StringType, IntegerType
import subprocess
import time
from pyspark.sql import functions as F

# Job-specific configuration
APP_NAME = "HiveToSingleStoreEdgeNodeOptimized"

# SingleStore table name and connection details
singlestore_table = "supplier"
singlestore_host = "your_singlestore_host"
singlestore_port = 3306
singlestore_user = "your_username"
singlestore_password = "your_password"
singlestore_database = "your_database"

# Output directory in HDFS (or NFS/S3)
output_dir = "hdfs://namenode:8021/user/spark/singlestore_load_data"  # Adjust path

# Initialize SparkSession
spark = get_spark_session(APP_NAME)

# Step 1: Pull data from Hive
data_extract_query = """
    SELECT ap_file_id, vendor_id, supplier_id, sia_agg_id
    FROM your_hive_table
    WHERE ap_batch_id = '231775'
"""
supplier_df_pre = spark.sql(data_extract_query)

# Step 2: Clean and transform data
selected_columns = supplier_df_pre.columns
supplier_df_clean = supplier_df_pre.select(*selected_columns)
for column in supplier_df_clean.columns:
    if isinstance(supplier_df_clean.schema[column].dataType, (DecimalType, DoubleType, FloatType)):
        supplier_df_clean = supplier_df_clean.withColumn(column, col(column).cast("double"))
    elif isinstance(supplier_df_clean.schema[column].dataType, StringType):
        supplier_df_clean = supplier_df_clean.withColumn(column, regexp_replace(col(column), "[^\\w\\s-]", ""))
    supplier_df_clean = supplier_df_clean.withColumn(column, when(col(column).isNull(), "").otherwise(col(column)))

# Step 3: Determine the number of partitions dynamically
total_records = supplier_df_clean.count()
print(f"Total records: {total_records}")

unique_supplier_ids = supplier_df_clean.select("supplier_id").distinct().count()
print(f"Number of unique supplier_id values: {unique_supplier_ids}")

num_executors = spark.sparkContext._conf.get("spark.executor.instances", "50")
num_cores_per_executor = spark.sparkContext._conf.get("spark.executor.cores", "4")
total_cores = int(num_executors) * int(num_cores_per_executor)
print(f"Total cores in cluster: {total_cores}")

target_partitions_per_core = 2
num_partitions = min(max(total_cores * target_partitions_per_core, unique_supplier_ids), total_records // 1000)
num_partitions = max(num_partitions, 1)
print(f"Number of partitions: {num_partitions}")

# Step 4: Repartition by supplier_id
supplier_df_repartitioned = supplier_df_clean.repartition(num_partitions, "supplier_id")

# Debug: Check partition distribution
print("Records per partition:")
partition_sizes = supplier_df_repartitioned.groupBy(spark_partition_id().alias("partition_id")).count().collect()
for row in partition_sizes:
    print(f"Partition {row['partition_id']}: {row['count']} records")

# Step 5: Transform DataFrame to RDD and format with | delimiter
def format_row(row):
    return "|".join(str(row[col]) if row[col] is not None else "" for col in selected_columns)

# Convert DataFrame to RDD and map to pipe-delimited strings
rdd_formatted = supplier_df_repartitioned.rdd.map(lambda row: format_row(row))

# Step 6: Write to files using saveAsTextFile
start_write_time = time.time()
rdd_formatted.saveAsTextFile(output_dir)
write_time = time.time() - start_write_time
print(f"Writing files took {write_time} seconds")

# Step 7: Verify generated files
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
output_files = [f"{output_dir}/part-{str(i).zfill(5)}" for i in range(num_partitions) 
                if fs.exists(spark._jvm.org.apache.hadoop.fs.Path(f"{output_dir}/part-{str(i).zfill(5)}"))]
print(f"Generated {len(output_files)} data files in {output_dir}")

# Step 8: Construct and execute LOAD DATA shell command with NULL handling
set_clause = ", ".join(f"{col} = NULLIF({col}, '')" for col in selected_columns)
load_command = [
    "mysql",
    f"-h{singlestore_host}",
    f"-P{singlestore_port}",
    f"-u{singlestore_user}",
    f"-p{singlestore_password}",
    singlestore_database,
    "-e",
    f"LOAD DATA INFILE '{output_dir}/part-*' INTO TABLE {singlestore_table} "
    f"FIELDS TERMINATED BY '|' ENCLOSED BY '' LINES TERMINATED BY '\\n' "
    f"({', '.join(selected_columns)}) SET {set_clause}"
]

start_load_time = time.time()
try:
    result = subprocess.run(load_command, check=True, capture_output=True, text=True)
    print("LOAD DATA command output:", result.stdout)
    if result.stderr:
        print("LOAD DATA command errors:", result.stderr)
except subprocess.CalledProcessError as e:
    print(f"LOAD DATA command failed with error: {e}")
    print("Error output:", e.stderr)

load_time = time.time() - start_load_time
print(f"Loaded {total_records} rows into {singlestore_table} in {load_time} seconds")

# Step 9: Clean up HDFS directory
fs.delete(spark._jvm.org.apache.hadoop.fs.Path(output_dir), True)

# Stop the SparkSession
spark.stop()
