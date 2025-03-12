from spark_config import get_spark_session
import singlestoredb as s2
from sconf import conf2
from pyspark.sql.functions import regexp_replace, col, when, collect_list, struct, count, lit
from pyspark.sql.types import DecimalType, DoubleType, FloatType, StringType, IntegerType
import time

# Job-specific configuration
APP_NAME = "HiveToSingleStoreEdgeNodeOptimized"

# SingleStore connection configuration
singlestore_config = conf2

# SingleStore table name
singlestore_table = "supplier"

# Initialize SparkSession
spark = get_spark_session(APP_NAME)

# Step 1: Pull data from Hive (4 columns)
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

# Step 3: Count tuples per ap_file_id
tuple_counts_df = supplier_df_clean.groupBy("ap_file_id").agg(count(lit(1)).alias("tuple_count"))
tuple_counts = tuple_counts_df.collect()  # Collect to driver for assignment
total_tuples = sum(row["tuple_count"] for row in tuple_counts)
target_tuples_per_partition = total_tuples // 100  # Target 100 partitions

# Step 4: Assign ap_file_id to partitions
partition_assignments = {}
current_partition = 0
current_tuples = 0
for row in sorted(tuple_counts, key=lambda x: x["tuple_count"], reverse=True):  # Sort by tuple count (largest first)
    ap_file_id = row["ap_file_id"]
    tuple_count = row["tuple_count"]
    if current_tuples + tuple_count > target_tuples_per_partition and current_tuples > 0:
        current_partition += 1
        current_tuples = 0
    partition_assignments[ap_file_id] = current_partition
    current_tuples += tuple_count

# Step 5: Add partition assignment to DataFrame
broadcast_assignments = spark.sparkContext.broadcast(partition_assignments)
udf_assign_partition = udf(lambda ap_file_id: broadcast_assignments.value.get(ap_file_id, 0), IntegerType())
supplier_df_agg = (supplier_df_clean.groupBy("ap_file_id")
                   .agg(collect_list(struct("ap_file_id", "vendor_id", "supplier_id", "sia_agg_id")).alias("insert_data"))
                   .withColumn("partition_id", udf_assign_partition(col("ap_file_id"))))

# Step 6: Repartition based on custom partition_id
supplier_df_agg = supplier_df_agg.repartition(100, "partition_id")

# Debug: Print schema and sample data
print("Number of partitions: ", supplier_df_agg.rdd.getNumPartitions())
supplier_df_agg.printSchema()
supplier_df_agg.show(5, truncate=False)

# Debug: Collect a small sample to inspect the data
sample_rows = supplier_df_agg.limit(2).collect()
print("Sample rows from supplier_df_agg:")
for row in sample_rows:
    print(row)

# Step 7: Insert into SingleStore from edge node by iterating over partitions
total_rows = 0
connection = None
cursor = None

try:
    # Open SingleStore connection on edge node
    connection = s2.connect(**singlestore_config)
    cursor = connection.cursor()

    # Define the insert query
    insert_query = """
        INSERT INTO supplier (ap_file_id, vendor_id, supplier_id, sia_agg_id)
        VALUES (%s, %s, %s, %s)
    """

    # Use glom() to collect rows by partition on the driver
    partition_iterator = supplier_df_agg.rdd.glom().collect()

    for partition_idx, partition_rows in enumerate(partition_iterator):
        print(f"Processing partition {partition_idx}")
        start_time = time.time()

        # Process all rows in this partition
        for row in partition_rows:
            # Debug: Inspect the type and content of the row
            print(f"row Type: {type(row)}")
            print(f"row: {row}")

            # Access ap_file_id and insert_data from the Row object
            apFileId = row["ap_file_id"]
            apData = row["insert_data"]

            # Convert struct list to list of tuples in one step
            insertAPData = [tuple(apd.asDict().values()) for apd in apData] if apData else []

            if not insertAPData:
                continue

            # Debug: Log the batch size and sample data
            print(f"insertAPData: {insertAPData}")

            try:
                cursor.executemany(insert_query, insertAPData)
                connection.commit()
                total_rows += len(insertAPData)
            except s2.Error as e:
                print(f"Error inserting batch for ap_file_id={apFileId}: {e} | Batch size: {len(insertAPData)} | Sample: {insertAPData[:5]}")
                connection.rollback()

        print(f"Processing partition {partition_idx} inserted successfully in {(time.time() - start_time)} seconds")

except s2.Error as e:
    print(f"Error connecting to SingleStore: {e}")
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()

print(f"Inserted {total_rows} rows into {singlestore_table} with {supplier_df_agg.rdd.getNumPartitions()} partitions")

# Stop the SparkSession
spark.stop()
