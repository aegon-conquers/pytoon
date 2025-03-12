from spark_config import get_spark_session
import singlestoredb as s2
from sconf import conf2
from pyspark.sql.functions import regexp_replace, col, when, collect_list, struct, explode, count
from pyspark.sql.types import DecimalType, DoubleType, FloatType, StringType, IntegerType
from pyspark.sql.functions import udf
import time

# Job-specific configuration
APP_NAME = "HiveToSingleStoreEdgeNodeOptimized"

# SingleStore connection configuration
singlestore_config = conf2

# SingleStore table name
singlestore_table = "supplier"

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

# Step 3: Group by ap_file_id and aggregate with dynamic struct
columns_to_struct = supplier_df_clean.columns
supplier_df_agg = (supplier_df_clean.groupBy("ap_file_id")
                   .agg(collect_list(struct(*columns_to_struct)).alias("insert_data")))

# Debug: Check if supplier_df_agg is empty
print("supplier_df_agg count:", supplier_df_agg.count())

# Step 4: Compute tuple counts by exploding insert_data
tuple_counts_df = (supplier_df_agg.select("ap_file_id", explode("insert_data").alias("tuple"))
                  .groupBy("ap_file_id")
                  .agg(count("tuple").alias("tuple_count")))

# Debug: Show the tuple counts DataFrame
print("Tuple counts DataFrame:")
tuple_counts_df.show()

# Collect to driver for assignment
tuple_counts = tuple_counts_df.collect()
print("Tuple counts collected, type:", type(tuple_counts))
if not tuple_counts:
    raise ValueError("No tuple counts collected. Check the input data or aggregation.")
for row in tuple_counts:
    print(row)

total_tuples = sum(row["tuple_count"] for row in tuple_counts)
print(f"Total tuples: {total_tuples}")
target_tuples_per_partition = total_tuples // 1000  # Target 1000 partitions for better parallelism
print(f"Target tuples per partition: {target_tuples_per_partition}")

# Step 5: Assign ap_file_id to partitions
partition_assignments = {}
current_partition = 0
current_tuples = 0
print("Assigning partitions, tuple_counts type:", type(tuple_counts))
for row in tuple_counts:
    ap_file_id = row["ap_file_id"]
    tuple_count = row["tuple_count"]
    if current_tuples + tuple_count > target_tuples_per_partition and current_tuples > 0:
        current_partition += 1
        current_tuples = 0
    partition_assignments[ap_file_id] = current_partition
    current_tuples += tuple_count

# Step 6: Add partition assignment to DataFrame
broadcast_assignments = spark.sparkContext.broadcast(partition_assignments)
udf_assign_partition = udf(lambda ap_file_id: broadcast_assignments.value.get(ap_file_id, 0), IntegerType())
supplier_df_agg = supplier_df_agg.withColumn("partition_id", udf_assign_partition(col("ap_file_id")))

# Step 7: Repartition based on custom partition_id
supplier_df_agg = supplier_df_agg.repartition(1000, "partition_id")  # Increase to 1000 partitions

# Debug: Print schema and sample data
print("Number of partitions: ", supplier_df_agg.rdd.getNumPartitions())
supplier_df_agg.printSchema()
supplier_df_agg.show(5, truncate=False)

# Step 8: Define a function to process each partition with ON DUPLICATE KEY UPDATE
def insert_partition_with_upsert(partition_rows):
    connection = None
    cursor = None
    try:
        connection = s2.connect(**singlestore_config)
        cursor = connection.cursor()

        # Define the insert query with ON DUPLICATE KEY UPDATE
        # Assuming (ap_file_id, vendor_id) is the unique key; adjust based on your table
        unique_key_columns = ["ap_file_id", "vendor_id"]  # Define your unique key columns
        update_columns = [col for col in columns_to_struct if col not in unique_key_columns]
        update_set_clause = ", ".join([f"{col} = VALUES({col})" for col in update_columns])

        insert_query = f"""
            INSERT INTO {singlestore_table} ({", ".join(columns_to_struct)})
            VALUES ({", ".join(["%s"] * len(columns_to_struct))})
            ON DUPLICATE KEY UPDATE {update_set_clause}
        """

        # Batch size for executemany
        batch_size = 10000  # Adjust based on performance testing
        batch = []

        # Process rows in the partition
        for row in partition_rows:
            apFileId = row["ap_file_id"]
            apData = row["insert_data"]

            # Convert struct list to list of tuples
            insertAPData = [tuple(apd.asDict().values()) for apd in apData] if apData else []

            if not insertAPData:
                continue

            batch.extend(insertAPData)

            # If batch is full, execute insert
            if len(batch) >= batch_size:
                cursor.executemany(insert_query, batch)
                connection.commit()
                batch = []  # Reset batch

        # Insert remaining rows in the batch
        if batch:
            cursor.executemany(insert_query, batch)
            connection.commit()

    except s2.Error as e:
        print(f"Error in partition: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Step 9: Process partitions in parallel using foreachPartition
start_time = time.time()
supplier_df_agg.foreachPartition(insert_partition_with_upsert)
end_time = time.time()

print(f"Inserted/Updated {total_tuples} rows into {singlestore_table} in {end_time - start_time} seconds")

# Stop the SparkSession
spark.stop()
