import yaml
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DoubleType, DecimalType, DateType, TimestampType
from datetime import date

# Initialize Spark session for Hive operations
spark = SparkSession.builder \
    .appName("SingleStoreSync") \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

# SingleStore connection properties
s2_conn_props = {
    "host": "<your-singlestore-host>",
    "port": 3306,
    "user": "<your-username>",
    "password": "<your-password>"
}

# Create a single pymysql connection
connection = pymysql.connect(
    host=s2_conn_props["host"],
    port=s2_conn_props["port"],
    user=s2_conn_props["user"],
    password=s2_conn_props["password"],
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor
)

# Current date for as_of_dt
as_of_dt = date.today()  # e.g., 2025-07-17

# 1. Read configuration from config.yaml
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)
tables = config["tables"]  # List of table names to process

# 2. Identify unique key columns for all tables in a single query
tables_list = ",".join(f"'{table}'" for table in tables)
query = f"""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = 'prod_db'
    AND TABLE_NAME IN ({tables_list})
    AND CONSTRAINT_NAME = 'UNIQUE'
    ORDER BY TABLE_NAME, COLUMN_NAME
"""
with connection.cursor() as cursor:
    cursor.execute(query)
    unique_keys_result = cursor.fetchall()
unique_keys_dict = {}
for table in tables:
    unique_keys = [row["COLUMN_NAME"] for row in unique_keys_result if row["TABLE_NAME"] == table]
    unique_keys_dict[table] = unique_keys

# 3. Iterate through each table and process deletions
for table in tables:
    unique_keys = unique_keys_dict.get(table, [])
    if not unique_keys:
        print(f"No unique keys found for table {table}. Skipping...")
        continue

    # Get schema from prod_db table
    schema_query = f"DESCRIBE prod_db.{table}"
    with connection.cursor() as cursor:
        cursor.execute(schema_query)
        schema_result = cursor.fetchall()
    schema = [(row["Field"], row["Type"]) for row in schema_result]
    spark_schema = StructType([
        StructField(field, _sql_type_to_spark_type(type_name), True)
        for field, type_name in schema
    ])
    # Add as_of_dt column to schema
    spark_schema_with_dt = StructType(spark_schema.fields + [StructField("as_of_dt", DateType(), False)])

    # Create Hive table if it doesn't exist
    hive_table_name = f"deleted_{table}"
    if not spark.catalog.tableExists(hive_table_name):
        empty_df = spark.createDataFrame([], spark_schema_with_dt)
        empty_df.write \
            .format("hive") \
            .mode("overwrite") \
            .saveAsTable(hive_table_name)
        print(f"Created Hive table {hive_table_name}")

    # 4. Construct SELECT query to identify records to delete
    join_conditions = " AND ".join([f"prod.{col} = landing.{col}" for col in unique_keys])
    select_query = f"""
        SELECT prod.*
        FROM prod_db.{table} prod
        LEFT JOIN landing_db.{table} landing
        ON {join_conditions}
        WHERE landing.{unique_keys[0]} IS NULL
    """

    # Execute SELECT query using cursor
    with connection.cursor() as cursor:
        cursor.execute(select_query)
        records_to_delete_result = cursor.fetchall()
    records_count = len(records_to_delete_result)

    # 5. Write deleted records to Hive table with as_of_dt
    if records_count > 0:
        # Convert query results to Spark DataFrame
        records_data = [[*row.values(), as_of_dt] for row in records_to_delete_result]
        records_to_delete = spark.createDataFrame(records_data, spark_schema_with_dt)

        # Append to Hive table
        records_to_delete.write \
            .format("hive") \
            .mode("append") \
            .saveAsTable(hive_table_name)
        print(f"Appended {records_count} deleted records for {table} to Hive table {hive_table_name} with as_of_dt={as_of_dt}")
    else:
        print(f"No records to delete for {table}")

    # 6. Execute DELETE query with join conditions
    if records_count > 0:
        delete_query = f"""
            DELETE prod
            FROM prod_db.{table} prod
            LEFT JOIN landing_db.{table} landing
            ON {join_conditions}
            WHERE landing.{unique_keys[0]} IS NULL
        """
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
        connection.commit()  # Commit the DELETE transaction
        print(f"Deleted {records_count} records from prod_db.{table}")
    else:
        print(f"No DELETE executed for {table}")

# Close the connection
connection.close()

# Stop Spark session
spark.stop()

# Helper function to map SingleStore types to Spark types
def _sql_type_to_spark_type(sql_type):
    type_mapping = {
        "int": IntegerType(),
        "bigint": LongType(),
        "varchar": StringType(),
        "text": StringType(),
        "double": DoubleType(),
        "decimal": DecimalType(38, 18),
        "date": DateType(),
        "datetime": TimestampType(),
        # Add more mappings as needed
    }
    for key in type_mapping:
        if key in sql_type.lower():
            return type_mapping[key]
    return StringType()  # Default to StringType for unmapped types
