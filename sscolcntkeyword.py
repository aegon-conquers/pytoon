import mysql.connector  # SingleStore uses MySQL-compatible protocol
from collections import Counter

# Database connection configuration
config = {
    'host': 'localhost',      # Replace with your SingleStore host
    'port': 3306,             # Default SingleStore port
    'user': 'your_username',  # Replace with your username
    'password': 'your_password',  # Replace with your password
    'database': 'your_database'   # Replace with your database name
}

try:
    # Connect to SingleStore
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Query to get all column names from all tables in the current database
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
    """
    cursor.execute(query)

    # Fetch all column names
    column_names = [row[0] for row in cursor.fetchall()]

    # Split each column name by underscore and flatten the list
    keywords = []
    for col in column_names:
        split_keywords = col.split('_')
        keywords.extend(split_keywords)

    # Count occurrences of each keyword
    keyword_counts = Counter(keywords)

    # Sort by count (descending) and then alphabetically
    sorted_keywords = sorted(keyword_counts.items(), key=lambda x: (-x[1], x[0]))

    # Print results
    print("Keyword Occurrences:")
    print("--------------------")
    for keyword, count in sorted_keywords:
        print(f"{keyword}: {count}")

except mysql.connector.Error as e:
    print(f"Error connecting to SingleStore: {e}")

finally:
    # Clean up
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
