#!/bin/bash

# Input file containing TicketNumber|FilePath
input_file="$1"

# Check if input file exists
if [ ! -f "$input_file" ]; then
    echo "Error: Input file not found"
    exit 1
fi

# Output file
output_file="output.txt"

# Clear output file if it exists
> "$output_file"

# Read input file line by line
while IFS='|' read -r ticket filepath; do
    # Check if filepath exists and is readable
    if [ -r "$filepath" ]; then
        # Read 3rd line, get 6th field (tab-separated)
        token6=$(sed -n '3p' "$filepath" | awk -F'\t' '{print $6}')
        
        # Check if token6 is not empty
        if [ -n "$token6" ]; then
            # Write to output file in format: Token6|TicketNumber|FilePath
            echo "${token6}|${ticket}|${filepath}" >> "$output_file"
        else
            echo "Warning: No value found at index 6 for file $filepath (ticket $ticket)"
        fi
    else
        echo "Warning: Cannot read file $filepath for ticket $ticket"
    fi
done < "$input_file"

echo "Processing complete. Output written to $output_file"
