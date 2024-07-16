import os
import shutil

# Define the path to the CSV directory and the target CSV file
csv_dir = "/path/to/output.csv"
target_file = "/path/to/final_output.csv"

# Find the actual CSV file within the directory
for file_name in os.listdir(csv_dir):
    if file_name.endswith(".csv"):
        source_file = os.path.join(csv_dir, file_name)
        # Rename the file
        shutil.move(source_file, target_file)
        break

# Optionally, you can remove the now-empty directory
os.rmdir(csv_dir)


import pandas as pd

# Define the path to the CSV file
csv_file_path = "/path/to/your_file.csv"

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv(csv_file_path)

# Display the DataFrame
print(df)

