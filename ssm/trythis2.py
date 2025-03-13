from spark_config import get_spark_session
from pyspark.sql.functions import regexp_replace, col, when
from pyspark.sql.types import DecimalType, DoubleType, FloatType, StringType, IntegerType
from pyspark.sql.functions import udf
import sys
import os
import time

# Job-specific configuration
APP_NAME = "HiveToSingleStoreEdgeNodeOptimized"

# SingleStore table name
singlestore_table = "supplier"

# Initialize SparkSession
spark = get_spark_session(APP_NAME)

# Step 1: Distribute singlestore_helper.py to all executors
singlestore_helper_path = "singlestore_helper.py"
spark.sparkContext.addFile(singlestore_helper_path)

# Step 2: Pull data from Hive
data_extract_query = """
    SELECT ap_file_id, vendor_id, supplier_id, sia_agg_id
    FROM your_hive_table
    WHERE ap_batch_id = '231775'
"""
supplier_df_pre = spark.sql(data_extract_query)

# Step 3: Clean and transform data
selected_columns = supplier_df_pre.columns
supplier_df_clean = supplier_df_pre.select(*selected_columns)
for column in supplier_df_clean.columns:
    if isinstance(supplier_df_clean.schema[column].dataType, (DecimalType, DoubleType, FloatType)):
        supplier_df_clean = supplier_df_clean.withColumn(column, col(column).cast("double"))
    elif isinstance(supplier_df_clean.schema[column].dataType, StringType):
        supplier_df_clean = supplier_df_clean.withColumn(column, regexp_replace(col(column), "[^\\w\\s-]", ""))
    supplier_df_clean = supplier_df_clean.withColumn(column, when(col(column).isNull(), "").otherwise(col(column)))

# Step 4: Repartition by supplier_id (no aggregation)
columns_to_insert = supplier_df_clean.columns  # All columns to insert
supplier_df_repartitioned = supplier_df_clean.repartition(1000, "supplier_id")  # Repartition with 1000 partitions

# Debug: Check total records and partition distribution
total_records = supplier_df_repartitioned.count()
print(f"Total records: {total_records}")
print("Records per partition:")
supplier_df_repartitioned.rdd.glom().map(len).collect()

# Debug: Print schema and sample data
print("Number of partitions: ", supplier_df_repartitioned.rdd.getNumPartitions())
supplier_df_repartitioned.printSchema()
supplier_df_repartitioned.show(5, truncate=False)

# Step 5: Define a function to process each partition with ON DUPLICATE KEY UPDATE
def insert_partition_with_upsert(partition_rows):
    from pyspark.files import SparkFiles
    singlestore_helper_path_on_executor = SparkFiles.get("singlestore_helper.py")
    sys.path.append(os.path.dirname(singlestore_helper_path_on_executor))
    import singlestore_helper

    connection = None
    cursor = None
    try:
        connection = singlestore_helper.getConnection()
        cursor = connection.cursor()

        # Define the insert query with ON DUPLICATE KEY UPDATE
        unique_key_columns = ["ap_file_id", "vendor_id"]  # Adjust based on your table
        update_columns = [col for col in columns_to_insert if col not in unique_key_columns]
        update_set_clause = ", ".join([f"{col} = VALUES({col})" for col in update_columns])

        insert_query = f"""
            INSERT INTO {singlestore_table} ({", ".join(columns_to_insert)})
            VALUES ({", ".join(["%s"] * len(columns_to_insert))})
            ON DUPLICATE KEY UPDATE {update_set_clause}
        """

        # Batch size for executemany
        batch_size = 10000  # Adjust based on performance testing
        batch = []

        # Process rows in the partition
        for row in partition_rows:
            values = tuple(row[col] for col in columns_to_insert)
            batch.append(values)

            # If batch is full, execute insert
            if len(batch) >= batch_size:
                cursor.executemany(insert_query, batch)
                connection.commit()
                batch = []  # Reset batch

        # Insert remaining rows in the batch
        if batch:
            cursor.executemany(insert_query, batch)
            connection.commit()

    except Exception as e:
        print(f"Error in partition: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Step 6: Process partitions in parallel using foreachPartition
start_time = time.time()
supplier_df_repartitioned.foreachPartition(insert_partition_with_upsert)
end_time = time.time()

print(f"Inserted/Updated {total_records} rows into {singlestore_table} in {end_time - start_time} seconds")

# Stop the SparkSession
spark.stop()
