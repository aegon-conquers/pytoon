import os
import sys
from singlestoredb import connect

def load_csv_to_singlestore(directory_path, table_name):
    # SingleStore DB connection details
    singlestore_config = {
        'host': 'your_singlestore_host',
        'port': 3306,  # default port for SingleStore DB
        'user': 'your_username',
        'password': 'your_password',
        'database': 'your_database_name'
    }
    
    try:
        with connect(**singlestore_config) as conn:
            with conn.cursor() as cursor:
                # Attempt to enable local_infile for this session
                cursor.execute("SET SESSION local_infile = 1;")
                
                # Verify if local_infile is now supported by this connection
                cursor.execute("SHOW VARIABLES LIKE 'local_infile';")
                result = cursor.fetchone()
                if result and result[1].lower() == 'on':
                    print("Local infile is now enabled for this session. Proceeding with data load.")
                    
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
                    print("Failed to enable local infile for this session. Cannot proceed with LOAD DATA LOCAL INFILE.")
                    return
                
            print("All CSV files processed.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <directory_path> <table_name>")
        sys.exit(1)
    
    directory_path = sys.argv[1]
    table_name = sys.argv[2]
    load_csv_to_singlestore(directory_path, table_name)
