from spark_config import get_spark_session
import singlestoredb
from datetime import datetime

# Job-specific configuration
APP_NAME = "HiveToSingleStoreDistributed"

# SingleStore connection configuration
singlestore_config = {
    'host': 'localhost',        # Replace with your SingleStore host
    'port': 3306,              # Default SingleStore port
    'user': 'your_username',   # Replace with your username
    'password': 'your_password', # Replace with your password
    'database': 'your_singlestore_db'  # Replace with your SingleStore database
}

# SingleStore table name
singlestore_table = "your_singlestore_table"

# Initialize SparkSession
spark = get_spark_session(APP_NAME)

# Define Hive SQL query
hive_query = """
    SELECT 
        name,          -- Maps to TEXT
        amount,        -- Maps to DECIMAL(20,6)
        quantity       -- Maps to INT(11)
    FROM your_hive_table
    WHERE some_condition
"""

# Execute Hive query and get DataFrame
df = spark.sql(hive_query)

# Repartition the DataFrame for more parallelism
# Option 1: Specify a fixed number of partitions (e.g., 100)
df = df.repartition(100)  # Adjust based on cluster size and data volume

# Option 2: Partition by a column for better distribution (e.g., 'name')
# df = df.repartition("name")

# Function to process each partition and insert into SingleStore
def process_partition(partition):
    conn = None
    cursor = None
    try:
        conn = singlestoredb.connect(**singlestore_config)
        cursor = conn.cursor()

        insert_query = f"""
            INSERT INTO {singlestore_table} (name, amount, quantity, order_time)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                order_time = VALUES(order_time)
        """

        batch_size = 1000
        batch = []
        current_ts = datetime.now().isoformat()

        for row in partition:
            data = (
                row['name'],
                float(row['amount']),
                int(row['quantity']),
                current_ts
            )
            batch.append(data)
            if len(batch) >= batch_size:
                cursor.executemany(insert_query, batch)
                conn.commit()
                batch = []

        if batch:
            cursor.executemany(insert_query, batch)
            conn.commit()

        yield len(batch)
    except singlestoredb.Error as e:
        print(f"Error in partition: {e}")
        yield 0
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Apply the function to each partition
df.foreachPartition(process_partition)

print(f"Distributed insertion into {singlestore_table} with {df.rdd.getNumPartitions()} partitions completed")

# Stop the SparkSession
spark.stop()
