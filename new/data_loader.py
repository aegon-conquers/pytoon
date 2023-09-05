from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from pyspark.sql.window import Window
import logging
import os
from datetime import datetime

# Define the log file location and filename with the current date
log_directory = "/path/to/log/directory"  # Replace with your desired directory path
current_date = datetime.now().strftime("%Y-%m-%d")
log_filename = f"your_log_{current_date}.log"
log_file_path = os.path.join(log_directory, log_filename)

# Configure the logging settings
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO, WARNING, ERROR, etc.)
    format='%(asctime)s [%(levelname)s] - %(message)s',  # Define the log message format
    filename=log_file_path,  # Specify the log file path with the current date
    filemode='a'  # Choose the file mode ('a' for append)
)

# Log an informational message
logging.info("This is an informational message.")
# Log a warning message
logging.warning("This is a warning message.")
# Log an error message
logging.error("This is an error message.")


# Initialize Spark session
spark = SparkSession.builder.appName("DataTransformation").enableHiveSupport().getOrCreate()

# Read data from Hive tables
file_data = spark.table("file_data")
file_analytics_data = spark.table("file_analytics_data")
processed_data = spark.table("processed_data")

# Step 1: Join "file_data" and "file_analytics_data" tables
joined_data = file_data.join(file_analytics_data, ["tracking_id", "record_id"], "inner")

# Step 2: Select required columns
output_data = joined_data.select(
    col("file_id"),
    col("record_id"),
    col("record_notes"),
    col("record_products")
)

# Step 3: Validate that every "file_id" has the exact number of records ingested
validation_result = file_data.groupBy("file_id").agg(
    count("record_id").alias("record_count")
)
validation_result = validation_result.withColumn(
    "validation",
    (col("record_count") == col("file_id_rec_count")).cast("int")
)

# Step 4: Compare with "processed_data" to identify new and processed "file_id" values
new_file_ids = output_data.join(
    processed_data,
    output_data["file_id"] == processed_data["tracking_id"],
    "leftanti"
).select(
    col("file_id")
)

# Step 5: Ingest data into MemSQL using MERGE INTO
# You can use MemSQL connectors and bulk loading techniques as needed

# Optional: Log important information
output_data.write.mode("overwrite").parquet("output_data.parquet")

# Stop Spark session
spark.stop()
