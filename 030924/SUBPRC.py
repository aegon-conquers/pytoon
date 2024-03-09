import subprocess

def find_recent_files(directory):
    try:
        # Construct the find command
        command = ["find", directory, "-type", "f", "-mtime", "-2"]
        
        # Execute the command and capture the output
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        # Split the output into a list of file paths
        file_paths = result.stdout.strip().split("\n")
        
        # Print or return the list of file paths
        return file_paths
    except subprocess.CalledProcessError as e:
        # Handle any errors from the subprocess
        print("Error:", e)
        return []

# Example usage
directory_path = "/path/to/directory"
recent_files = find_recent_files(directory_path)
print("Recent files:")
for file_path in recent_files:
    print(file_path)
