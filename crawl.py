import os

base_directory = "/usr/temp/8456"

# Iterate over directories that start with "2024011"
for root, dirs, files in os.walk(base_directory):
    dirs[:] = [d for d in dirs if d.startswith("2024011")]

    # For every directory found, go into /data directory
    for directory in dirs:
        data_path = os.path.join(root, directory, "data")

        # Check if the /data directory exists
        if os.path.exists(data_path):
            # Iterate over directories inside /data
            for subdir in os.listdir(data_path):
                subdir_path = os.path.join(data_path, subdir)

                # Check if it's a directory
                if os.path.isdir(subdir_path):
                    # Look for files matching the pattern ap_load_*.dat
                    for filename in os.listdir(subdir_path):
                        if filename.startswith("ap_load_") and filename.endswith(".dat"):
                            # Full path to the file
                            file_path = os.path.join(subdir_path, filename)

                            # Read the first two lines of the file
                            with open(file_path, 'r') as file:
                                lines = file.readlines()
                                if len(lines) >= 2:
                                    # Count occurrences of empty strings between "\t"
                                    second_line_values = lines[1].split("\t")
                                    empty_string_count = second_line_values.count('')

                                    # Print key and filename
                                    key = second_line_values[3] if len(second_line_values) > 3 else None
                                    print(f"Key: {key}, Filename: {file_path}, Empty String Count: {empty_string_count}")
