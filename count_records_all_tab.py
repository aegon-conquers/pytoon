import mysql.connector
from mysql.connector import Error

# Database connection details
config = {
    'host': 'your_host',          # e.g., 'localhost' or SingleStore server IP
    'port': 3306,                 # Default SingleStore/MySQL port
    'user': 'your_username',      # Your SingleStore username
    'password': 'your_password',  # Your SingleStore password
    'database': 'your_schema'     # Your schema/database name
}

def get_table_record_counts():
    try:
        # Establish connection
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # Query to get all table names in the schema
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_TYPE = 'BASE TABLE'
        """, (config['database'],))
        
        tables = cursor.fetchall()

        if not tables:
            print(f"No tables found in schema '{config['database']}'")
            return

        # Dictionary to store table counts
        table_counts = {}

        # Loop through each table and get the record count
        for (table_name,) in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                table_counts[table_name] = count
            except Error as e:
                print(f"Error counting records in table '{table_name}': {e}")

        # Print the results
        print(f"\nRecord counts for tables in schema '{config['database']}':")
        print("-" * 50)
        for table, count in table_counts.items():
            print(f"{table:<30} | {count:>10}")
        print("-" * 50)

    except Error as e:
        print(f"Error connecting to SingleStore: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    get_table_record_counts()
