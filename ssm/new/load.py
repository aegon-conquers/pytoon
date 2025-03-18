import subprocess
import os
import sys
import time
from multiprocessing import Pool
from functools import partial
import yaml
from datetime import datetime
import mysql.connector
from configparser import ConfigParser

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Configuration constants
delimiter = config["delimiter"]
lines_terminated_by = config["lines_terminated_by"]
escaped_by = config["escaped_by"]
bulk_insert_buffer_size = config["bulk_insert_buffer_size"]
max_retries = config["max_retries"]
retry_delay = config["retry_delay"]
log_file = config["log_file"]
mysql_config_file = config["mysql_config_file"]
optional_columns = config.get("optional_columns", {})

# Get parameters from command line
if len(sys.argv) != 3:
    print("Usage: python load_data.py <input_dir> <dest_table>")
    sys.exit(1)
input_dir = sys.argv[1]
dest_table = sys.argv[2]

# Parse MySQL configuration file to extract connection details
config_parser = ConfigParser()
config_parser.read(mysql_config_file)
if "client" not in config_parser:
    print(f"Error: [client] section not found in {mysql_config_file}")
    sys.exit(1)

mysql_config = {
    "host": config_parser["client"].get("host", "localhost"),
    "port": config_parser["client"].get("port", 3306),
    "user": config_parser["client"].get("user"),
    "password": config_parser["client"].get("password"),
    "database": config_parser["client"].get("database")
}

# Connect to SingleStore and retrieve table schema
try:
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()
    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query, (dest_table,))
    singlestore_columns = [row[0] for row in cursor.fetchall()]
    if not singlestore_columns:
        print(f"Error: Table {dest_table} not found in SingleStore")
        sys.exit(1)
    cursor.close()
    connection.close()
except mysql.connector.Error as e:
    print(f"Error connecting to SingleStore: {e}")
    sys.exit(1)

# Get optional columns for this table
table_optional_columns = optional_columns.get(dest_table, [])

# Define a function to run a LOAD DATA command
def run_load_command(file_path, attempt=1, mysql_config_file=mysql_config_file, 
                    dest_table=dest_table, delimiter=delimiter, 
                    lines_terminated_by=lines_terminated_by, escaped_by=escaped_by, 
                    bulk_insert_buffer_size=bulk_insert_buffer_size, columns=singlestore_columns):
    load_command = [
        "mysql",
        f"--defaults-file={mysql_config_file}",
        "--local-infile=1",
        "-e",
        f"LOAD DATA LOCAL INFILE '{file_path}' INTO TABLE {dest_table} "
        f"FIELDS TERMINATED BY '{delimiter}' ENCLOSED BY '' ESCAPED BY '{escaped_by}' "
        f"LINES TERMINATED BY '{lines_terminated_by}' "
        f"({', '.join(columns)}) "
        f"SET @@bulk_insert_buffer_size = {bulk_insert_buffer_size}"
    ]
    try:
        result = subprocess.run(load_command, check=True, capture_output=True, text=True)
        return (file_path, True, attempt, result.stdout, result.stderr if result.stderr else "")
    except subprocess.CalledProcessError as e:
        error_msg = f"File: {file_path}, Attempt: {attempt}, Error: {str(e)}, Stderr: {e.stderr}"
        log_message(error_msg)
        return (file_path, False, attempt, str(e), e.stderr if e.stderr else "")

# Define a function to log messages
def log_message(message):
    with open(log_file, "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")

# Verify input directory and get files
if not os.path.exists(input_dir):
    print(f"Error: Input directory {input_dir} does not exist")
    sys.exit(1)

local_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.startswith("part-")]
if not local_files:
    print(f"Error: No data files found in {input_dir}")
    sys.exit(1)

print(f"Found {len(local_files)} data files in {input_dir}")

# Initial load attempt (parallel)
start_load_time = time.time()
failed_files = []  # List of (file_path, attempt_count)
success_count = 0

with Pool(processes=16) as pool:
    load_func = partial(run_load_command, attempt=1)
    results = pool.map(load_func, local_files)

# Process initial load results
for file_path, success, attempt, stdout, stderr in results:
    if success:
        success_count += 1
        print(f"Successfully loaded {file_path}")
    else:
        failed_files.append((file_path, 1))
        print(f"Failed to load {file_path} (attempt 1), will retry later")

# Retry failed files (parallel with 4 workers)
retry_round = 1
while failed_files and retry_round <= max_retries:
    print(f"Retry round {retry_round} for {len(failed_files)} failed files")
    new_failed_files = []
    with Pool(processes=4) as pool:
        load_func = partial(run_load_command, attempt=retry_round + 1)
        results = pool.map(load_func, [file_path for file_path, _ in failed_files])
    
    for file_path, success, attempt, stdout, stderr in results:
        if success:
            success_count += 1
            print(f"Successfully loaded {file_path} on retry {retry_round}")
        else:
            new_attempt_count = attempt
            if new_attempt_count <= max_retries:
                new_failed_files.append((file_path, new_attempt_count))
                print(f"Failed to load {file_path} (attempt {new_attempt_count}), will retry again")
            else:
                log_message(f"File: {file_path}, Attempt: {new_attempt_count}, Status: Permanent Failure, Error: {stdout}")
                print(f"Permanently failed to load {file_path} after {max_retries} attempts")
    failed_files = new_failed_files
    retry_round += 1

# Final reporting
load_time = time.time() - start_load_time
print(f"Loaded {success_count * (len(local_files) / len(local_files))} rows into {dest_table} in {load_time} seconds")
print(f"Total files processed: {len(local_files)}")
print(f"Successfully loaded: {success_count}")
print(f"Permanently failed: {len(failed_files)}")
if failed_files:
    print("Permanently failed files:")
    for file_path, _ in failed_files:
        print(f"  {file_path}")
