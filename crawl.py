import os

base_directory = "/usr/temp/8456"

def process_files(directory):
    for root, dirs, files in os.walk(directory):
        # Filter directories that start with "202401"
        dirs[:] = [d for d in dirs if d.startswith("202401")]
        for file in files:
            file_path = os.path.join(root, file)
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
