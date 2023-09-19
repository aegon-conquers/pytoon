import pymysql
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("MemSQLLoad").getOrCreate()

# Sample DataFrame (replace with your actual DataFrame)
data = [(1, 1, "Data1"), (1, 2, "Data2"), (2, 1, "Data3")]
columns = ["fileId", "recordId", "data"]
df = spark.createDataFrame(data, columns)

# Define MemSQL database connection details
memsql_host = "your_memsql_host"
memsql_port = 3306  # Default MemSQL port
memsql_user = "your_memsql_user"
memsql_password = "your_memsql_password"
memsql_database = "your_memsql_database"
memsql_table = "your_memsql_table"

# Batch process the DataFrame by fileId
file_ids = df.select("fileId").distinct().rdd.flatMap(lambda x: x).collect()
for file_id in file_ids:
    # Filter the DataFrame for the current fileId
    batch_df = df.filter(df.fileId == file_id)
    
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
            for row in batch_df.rdd.collect():
                # Check if the record with fileId and recordId already exists in MemSQL
                cursor.execute(f"SELECT * FROM {memsql_table} WHERE fileId = %s AND recordId = %s", (row.fileId, row.recordId))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Update the existing record
                    cursor.execute(
                        f"UPDATE {memsql_table} SET data = %s WHERE fileId = %s AND recordId = %s",
                        (row.data, row.fileId, row.recordId)
                    )
                else:
                    # Insert a new record
                    cursor.execute(
                        f"INSERT INTO {memsql_table} (fileId, recordId, data) VALUES (%s, %s, %s)",
                        (row.fileId, row.recordId, row.data)
                    )
        
        # Commit the changes for the current batch
        connection.commit()
    
    finally:
        connection.close()

# Stop Spark session
spark.stop()
