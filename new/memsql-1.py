import pandas as pd
import mysql.connector
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DataExport").getOrCreate()

# Read data from your PySpark DataFrame (replace with your DataFrame)
your_spark_dataframe = spark.read.csv("hdfs:///path/to/your/spark/data.csv", header=True, inferSchema=True)

# Define the path for the CSV file
csv_file_path = "/path/to/your/datafile.csv"

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = your_spark_dataframe.toPandas()

# Save Pandas DataFrame to the CSV file
pandas_df.to_csv(csv_file_path, index=False)

# Define the MemSQL connection parameters
memsql_host = "your_memsql_host"
memsql_port = 3306  # Default MemSQL port
memsql_user = "your_username"
memsql_password = "your_password"
memsql_database = "your_database"

# Create a MemSQL database connection
memsql_connection = mysql.connector.connect(
    host=memsql_host,
    port=memsql_port,
    user=memsql_user,
    password=memsql_password,
    database=memsql_database
)

# Create a cursor to execute SQL statements
memsql_cursor = memsql_connection.cursor()

# Execute the LOAD DATA INFILE statement in MemSQL with REPLACE
load_data_sql = f"""
    LOAD DATA INFILE '{csv_file_path}'
    REPLACE INTO TABLE your_table
    FIELDS TERMINATED BY ',' (column1, column2, column3);
"""

memsql_cursor.execute(load_data_sql)

# Commit the transaction
memsql_connection.commit()

# Close the cursor and the connection
memsql_cursor.close()
memsql_connection.close()

# Stop Spark session
spark.stop()
