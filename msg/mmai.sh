#!/bin/bash

# This script runs a series of Hive queries, captures each query and its result,
# removes the first 15 lines from a temporary copy before emailing, and sends via sendmail.

# ──────────────────────────────────────────────
# CONFIGURATION ─ edit these values
# ──────────────────────────────────────────────

QUERIES=(
  "SELECT * FROM your_table LIMIT 10;"
  "SELECT COUNT(*) FROM another_table WHERE dt = '2025-12-31';"
  # Add your real queries here
)

TO_EMAIL="your.email@example.com"
FROM_EMAIL="hive-script@$(hostname -f)"
SUBJECT="Hive Query Results - $(date +%Y-%m-%d)"

OUTPUT_FILE="hive_query_results_$(date +%Y%m%d_%H%M%S).txt"
TEMP_FILE="${OUTPUT_FILE}.email.tmp"

LINES_TO_REMOVE=15

# ──────────────────────────────────────────────

# Clear main output file
> "$OUTPUT_FILE"

# Optional nice header
{
  echo "Hive Query Results Report"
  echo "Generated: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "Hostname:  $(hostname)"
  echo "User:      $(whoami)"
  echo "----------------------------------------"
  echo ""
} >> "$OUTPUT_FILE"

# Run all queries
for query in "${QUERIES[@]}"; do
  echo "1. Query:" >> "$OUTPUT_FILE"
  echo "$query" >> "$OUTPUT_FILE"
  echo "" >> "$OUTPUT_FILE"
  
  echo "2. Result:" >> "$OUTPUT_FILE"
  echo "----------------------------------------" >> "$OUTPUT_FILE"
  
  hive -e "$query" >> "$OUTPUT_FILE" 2>&1
  
  echo "" >> "$OUTPUT_FILE"
  echo "----------------------------------------" >> "$OUTPUT_FILE"
  echo "" >> "$OUTPUT_FILE"
done

# ──────────────────────────────────────────────
# Prepare email version: skip first LINES_TO_REMOVE lines
# ──────────────────────────────────────────────

if [ -s "$OUTPUT_FILE" ] && [ "$(wc -l < "$OUTPUT_FILE")" -gt "$LINES_TO_REMOVE" ]; then
  tail -n +$((LINES_TO_REMOVE + 1)) "$OUTPUT_FILE" > "$TEMP_FILE"
else
  # File too small or empty → send full content (or handle differently)
  cp "$OUTPUT_FILE" "$TEMP_FILE"
  echo "Warning: File has ≤ $LINES_TO_REMOVE lines — sending full content" >&2
fi

# ──────────────────────────────────────────────
# Send email using sendmail
# ──────────────────────────────────────────────

{
  echo "To: $TO_EMAIL"
  echo "From: $FROM_EMAIL"
  echo "Subject: $SUBJECT"
  echo "Content-Type: text/plain; charset=UTF-8"
  echo ""
  cat "$TEMP_FILE"
} | sendmail -t

SENDMAIL_EXIT=$?

# Cleanup temp file
rm -f "$TEMP_FILE"

# ──────────────────────────────────────────────
# Feedback
# ──────────────────────────────────────────────

if [ $SENDMAIL_EXIT -eq 0 ]; then
  echo "Success:"
  echo "  • Full results saved to:   $OUTPUT_FILE"
  echo "  • Email sent (first $LINES_TO_REMOVE lines removed) to: $TO_EMAIL"
else
  echo "Error: sendmail exited with code $SENDMAIL_EXIT"
  echo "Full results are still saved in $OUTPUT_FILE"
  exit 1
fi

# Optional: remove full file after successful send (uncomment if desired)
# rm -f "$OUTPUT_FILE"
