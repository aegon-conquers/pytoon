import os
import sys
import mysql.connector
from mysql.connector import Error

def load_csv_to_memsql(directory_path, table_name):
    # MemSQL connection details
    memsql_config = {
        'host': 'your_memsql_host',
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database_name'
    }
    
    try:
        connection = mysql.connector.connect(**memsql_config)
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Iterate through all files in the directory
            for filename in os.listdir(directory_path):
                if filename.endswith('.csv'):
                    full_path = os.path.join(directory_path, filename)
                    
                    # SQL command to load data
                    load_command = f"""
                    LOAD DATA LOCAL INFILE '{full_path}' 
                    INTO TABLE {table_name} 
                    FIELDS TERMINATED BY ',' 
                    ENCLOSED BY '"' 
                    LINES TERMINATED BY '\n'
                    IGNORE 1 ROWS;  # Assumes the CSV has a header row
                    """
                    cursor.execute(load_command)
                    print(f"Successfully loaded data from {filename} into {table_name}")
            
            cursor.close()
            connection.close()
            print("All CSV files processed.")
    
    except Error as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <directory_path> <table_name>")
        sys.exit(1)
    
    directory_path = sys.argv[1]
    table_name = sys.argv[2]
    load_csv_to_memsql(directory_path, table_name)
