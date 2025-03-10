from spark_config import get_spark_session

# Job-specific configuration
APP_NAME = "HiveToSingleStoreJDBC"

# SingleStore JDBC URL and properties
jdbc_url = "jdbc:mysql://localhost:3306/your_singlestore_db?user=your_username&password=your_password"  # Replace with your details
jdbc_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "your_username",
    "password": "your_password",
    "batchsize": "1000"
}

# SingleStore table name
singlestore_table = "your_singlestore_table"
staging_table = "staging_table"

# Initialize SparkSession
spark = get_spark_session(APP_NAME)

# Define Hive SQL query with current timestamp
hive_query = """
    SELECT 
        name,          -- Maps to TEXT
        amount,        -- Maps to DECIMAL(20,6)
        quantity,      -- Maps to INT(11)
        CURRENT_TIMESTAMP as order_time  -- Maps to TIMESTAMP
    FROM your_hive_table
    WHERE some_condition
"""

# Execute Hive query and get DataFrame
df = spark.sql(hive_query)

# Repartition the DataFrame for more parallelism
# Option 1: Fixed number of partitions (e.g., 100)
df = df.repartition(100)  # Adjust based on cluster size and data volume

# Option 2: Partition by a column (e.g., 'name')
# df = df.repartition("name")

# Write to staging table
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", staging_table) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("batchsize", "1000") \
    .mode("overwrite") \
    .save()

# Merge staging into main table
import singlestoredb
conn = singlestoredb.connect(**{
    'host': 'localhost',
    'port': 3306,
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_singlestore_db'
})
cursor = conn.cursor()
merge_query = f"""
    INSERT INTO {singlestore_table} (name, amount, quantity, order_time)
    SELECT name, amount, quantity, order_time
    FROM {staging_table}
    ON DUPLICATE KEY UPDATE
        order_time = VALUES(order_time)
"""
cursor.execute(merge_query)
conn.commit()
cursor.close()
conn.close()

print(f"Distributed insertion and merge into {singlestore_table} with {df.rdd.getNumPartitions()} partitions completed")

# Stop the SparkSession
spark.stop()
