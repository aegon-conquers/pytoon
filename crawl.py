import os
import fnmatch

base_directory = "/usr/temp/8456"

def process_files(directory):
    for root, dirs, files in os.walk(directory):
        # Filter directories that start with the pattern "2024011"
        dirs[:] = [d for d in dirs if d.startswith("2024011")]

        for dir_name in dirs:
            data_directory = os.path.join(root, dir_name, "data")

            # If "data" directory exists, proceed to the next step
            if os.path.exists(data_directory):
                load_directories = [d for d in os.listdir(data_directory) if d.startswith("load_")]

                for load_dir in load_directories:
                    load_path = os.path.join(data_directory, load_dir)

                    # Process files in every "load_*" directory
                    process_load_directory(load_path)

def process_load_directory(load_directory):
    load_files = [f for f in os.listdir(load_directory) if fnmatch.fnmatch(f, "ap_load_*.dat")]
    for file in load_files:
        file_path = os.path.join(load_directory, file)
        key = get_key_from_file(file_path)
        print(f"Key: {key}, Filename: {file}")

def get_key_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            # Read the first two lines
            lines = file.readlines()[:2]

            # Split the second line by "\t" and get the value at Index 3
            if len(lines) >= 2:
                second_line_values = lines[1].strip().split("\t")
                if len(second_line_values) >= 4:
                    key = second_line_values[3]
                    return key
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
    return None

if __name__ == "__main__":
    process_files(base_directory)
