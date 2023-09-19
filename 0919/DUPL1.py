import pymysql
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("MemSQLLoad").getOrCreate()

# Sample DataFrame (replace with your actual DataFrame)
data = [(1, 1, "Data1", "AdditionalData1"), (1, 2, "Data2", "AdditionalData2"), (2, 1, "Data3", "AdditionalData3")]
columns = ["fileId", "recordId", "data", "additionalData"]
df = spark.createDataFrame(data, columns)

# Define MemSQL database connection details
memsql_host = "your_memsql_host"
memsql_port = 3306  # Default MemSQL port
memsql_user = "your_memsql_user"
memsql_password = "your_memsql_password"
memsql_database = "your_memsql_database"
memsql_table = "your_memsql_table"

# Establish a connection to MemSQL
connection = pymysql.connect(
    host=memsql_host,
    port=memsql_port,
    user=memsql_user,
    password=memsql_password,
    database=memsql_database,
    cursorclass=pymysql.cursors.DictCursor
)

try:
    with connection.cursor() as cursor:
        # Step 2: Perform Bulk Insert with ON DUPLICATE KEY UPDATE for multiple columns
        insert_records = [(row.fileId, row.recordId, row.data, row.additionalData) for row in df.rdd.collect()]
        insert_query = f"""
            INSERT INTO {memsql_table} (fileId, recordId, data, additionalData)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                data = VALUES(data),
                additionalData = VALUES(additionalData)
        """
        cursor.executemany(insert_query, insert_records)
    
    # Commit the changes
    connection.commit()

finally:
    connection.close()

# Stop Spark session
spark.stop()
