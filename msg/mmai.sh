#!/bin/bash

# This script runs a series of Hive queries, captures each query and its result in an output file,
# and emails the file to a specified address.
# Assumptions:
# - Hive is installed and configured on the system.
# - The 'mail' command is available for sending emails (common on Unix-like systems; may need configuration).
# - Queries are defined in the QUERIES array below. Add your queries here, each as a string.
# - If queries are multi-line, enclose them properly in quotes.
# - Replace 'your.email@example.com' with the actual email address.

# Array of Hive queries (add your queries here)
QUERIES=(
  "SELECT * FROM your_table LIMIT 10;"
  "SELECT COUNT(*) FROM another_table;"
  # Add more queries as needed
)

# Output file
OUTPUT_FILE="hive_query_results.txt"

# Email address (replace with your actual email)
EMAIL="your.email@example.com"

# Subject for the email
SUBJECT="Hive Query Results"

# Clear the output file
> "$OUTPUT_FILE"

# Loop through each query
for query in "${QUERIES[@]}"; do
  echo "1. Query:" >> "$OUTPUT_FILE"
  echo "$query" >> "$OUTPUT_FILE"
  echo "" >> "$OUTPUT_FILE"
  
  echo "2. Result:" >> "$OUTPUT_FILE"
  hive -e "$query" >> "$OUTPUT_FILE" 2>&1  # Capture stdout and stderr
  echo "" >> "$OUTPUT_FILE"
  echo "----------------------------------------" >> "$OUTPUT_FILE"
  echo "" >> "$OUTPUT_FILE"
done

# Send the email
mail -s "$SUBJECT" "$EMAIL" < "$OUTPUT_FILE"

# Optional: Echo completion
echo "Script completed. Results emailed to $EMAIL."
