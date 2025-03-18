from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField
import yaml
import os
import sys
import shutil
import mysql.connector
from configparser import ConfigParser

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Configuration constants
delimiter = config["delimiter"]
output_base_dir = config["output_base_dir"]
special_chars_to_remove = config["special_chars_to_remove"]
default_string_replacements = config["default_string_replacements"]
column_type_mappings = config["column_type_mappings"]
mysql_config_file = config["mysql_config_file"]
optional_columns = config.get("optional_columns", {})  # Default to empty dict if not present

# Get table name from command line argument
if len(sys.argv) != 2:
    print("Usage: spark-submit generate_data.py <singlestore_table_name>")
    sys.exit(1)
singlestore_table_name = sys.argv[1]

# Step 1: Parse MySQL configuration file to extract connection details
config_parser = ConfigParser()
config_parser.read(mysql_config_file)
if "client" not in config_parser:
    print(f"Error: [client] section not found in {mysql_config_file}")
    sys.exit(1)

mysql_config = {
    "host": config_parser["client"].get("host", "localhost"),
    "port": config_parser["client"].get("port", 3306),
    "user": config_parser["client"].get("user"),
    "password": config_parser["client"].get("password"),
    "database": config_parser["client"].get("database")
}

# Step 2: Connect to SingleStore and retrieve table schema
try:
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()
    query = """
        SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query, (singlestore_table_name,))
    singlestore_columns = [(row[0], row[1], row[2]) for row in cursor.fetchall()]  # (column_name, is_nullable, data_type)
    if not singlestore_columns:
        print(f"Error: Table {singlestore_table_name} not found in SingleStore")
        sys.exit(1)
    cursor.close()
    connection.close()
except mysql.connector.Error as e:
    print(f"Error connecting to SingleStore: {e}")
    sys.exit(1)

singlestore_column_names = [col[0] for col in singlestore_columns]
required_columns = [col[0] for col in singlestore_columns if col[1] == "NO"]
singlestore_column_types = {col[0]: col[2] for col in singlestore_columns}  # Map column name to data type

# Get optional columns for this table (if defined)
table_optional_columns = optional_columns.get(singlestore_table_name, [])

# Define Spark session
spark = SparkSession.builder \
    .appName(f"GenerateData_{singlestore_table_name}") \
    .getOrCreate()

# Step 3: Read the corresponding SQL file
sql_file = f"{singlestore_table_name}.sql"
if not os.path.exists(sql_file):
    print(f"Error: SQL file {sql_file} not found")
    spark.stop()
    sys.exit(1)

with open(sql_file, "r") as f:
    data_extract_query = f.read()

# Step 4: Extract data from Hive
supplier_df_pre = spark.sql(data_extract_query)

# Step 5: Validate Hive data against SingleStore schema
hive_columns = supplier_df_pre.columns

# Check for missing required columns
missing_required_columns = [col for col in required_columns if col not in hive_columns]
if missing_required_columns:
    print(f"Error: Missing required columns in Hive data: {missing_required_columns}")
    print(f"Required columns (non-nullable) in SingleStore: {required_columns}")
    print(f"Columns in Hive data: {hive_columns}")
    spark.stop()
    sys.exit(1)

# Check for missing optional columns (warning only)
missing_optional_columns = [col for col in table_optional_columns if col not in hive_columns]
if missing_optional_columns:
    print(f"Warning: Missing optional columns in Hive data: {missing_optional_columns}")
    print(f"Optional columns for {singlestore_table_name}: {table_optional_columns}")

# Reorder and select columns to match SingleStore schema
selected_columns = [col for col in singlestore_column_names if col in hive_columns or col in table_optional_columns]
supplier_df_clean = supplier_df_pre.select(*selected_columns)

# Cast columns to match SingleStore data types
for column in supplier_df_clean.columns:
    singlestore_type = singlestore_column_types.get(column, "string")
    mapped_type = column_type_mappings.get(singlestore_type, "string")
    if mapped_type == "timestamp":
        supplier_df_clean = supplier_df_clean.withColumn(column, F.to_timestamp(column, "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    elif mapped_type in ["double", "int", "string"]:
        supplier_df_clean = supplier_df_clean.withColumn(column, F.col(column).cast(mapped_type))

# Step 6: Transform data based on configuration
for column in supplier_df_clean.columns:
    if supplier_df_clean.schema[column].dataType.typeName() == "StringType":
        for replacement in default_string_replacements:
            supplier_df_clean = supplier_df_clean.withColumn(
                column, F.regexp_replace(F.col(column), replacement["pattern"], replacement["replacement"])
            )
        supplier_df_clean = supplier_df_clean.withColumn(
            column, F.regexp_replace(F.col(column), special_chars_to_remove, "")
        )
    supplier_df_clean = supplier_df_clean.withColumn(
        column, F.when(F.col(column).isNull(), "").otherwise(F.col(column))
    )

# Step 7: Determine the number of partitions
total_records = supplier_df_clean.count()
print(f"Total records: {total_records}")
num_partitions = max(10, min(total_records // 1000000, 100))  # Adjust based on data size
print(f"Number of partitions: {num_partitions}")

# Step 8: Repartition by a suitable column
repartition_column = selected_columns[0] if selected_columns else "value"
supplier_df_repartitioned = supplier_df_clean.repartition(num_partitions, repartition_column)

# Step 9: Transform DataFrame to RDD and format with delimiter
def format_row(row):
    return delimiter.join(
        str(row[col]).replace("\\" + delimiter, delimiter) if row[col] is not None else ""
        for col in selected_columns
    )

rdd_formatted = supplier_df_repartitioned.rdd.map(lambda row: format_row(row))

# Step 10: Define output directory for this table
output_dir = os.path.join(output_base_dir, singlestore_table_name)
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir, exist_ok=True)

# Step 11: Write to multiple files locally
start_write_time = time.time()
rdd_formatted.saveAsTextFile(output_dir)
write_time = time.time() - start_write_time
print(f"Writing files took {write_time} seconds")

# Step 12: Verify generated files
local_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith("part-")]
print(f"Generated {len(local_files)} data files in {output_dir}")

# Stop the SparkSession
spark.stop()
