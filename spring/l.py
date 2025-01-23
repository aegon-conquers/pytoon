import os
import sys
import mysql.connector
from mysql.connector import Error

def load_csv_to_singlestore(directory_path, table_name):
    singlestore_config = {
        'host': 'your_singlestore_host',
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database_name',
        'allow_local_infile': True  # This enables LOAD DATA LOCAL INFILE
    }
    
    try:
        connection = mysql.connector.connect(**singlestore_config)
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Verify if local_infile is supported by this connection
            cursor.execute("SHOW VARIABLES LIKE 'local_infile';")
            result = cursor.fetchone()
            if result and result[1].lower() == 'on':
                print("Local infile is enabled. Proceeding with data load.")
                
                for filename in os.listdir(directory_path):
                    if filename.endswith('.csv'):
                        full_path = os.path.join(directory_path, filename)
                        load_command = f"""
                        LOAD DATA LOCAL INFILE '{full_path}' 
                        INTO TABLE {table_name} 
                        FIELDS TERMINATED BY ',' 
                        ENCLOSED BY '"' 
                        LINES TERMINATED BY '\n'
                        IGNORE 1 ROWS;
                        """
                        cursor.execute(load_command)
                        print(f"Successfully loaded data from {filename} into {table_name}")
            else:
                print("Local infile is not enabled. Cannot proceed with LOAD DATA LOCAL INFILE.")
                return
            
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
    load_csv_to_singlestore(directory_path, table_name)
