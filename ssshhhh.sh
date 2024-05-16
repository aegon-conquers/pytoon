#!/bin/bash

# Store current date in MMDDYYYY format
run_date=$(date +%m%d%Y)

# Create output directory path
out_dir="/output/$run_date"

# Call Python job with out_dir as an argument
python your_python_script.py "$out_dir"

# Check if the Python job succeeded
if [ $? -eq 0 ]; then
    echo "Python job completed successfully."
    
    # Call another shell script with out_dir as an argument
    ./another_shell_script.sh "$out_dir"

    # Return 0 indicating success
    exit 0
else
    echo "Python job failed."
    exit 1
fi
