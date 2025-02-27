import singlestoredb as s2
from config import db_config  # Import the config from config.py

def get_not_null_columns(cursor, schema):
    """Fetch columns with NOT NULL constraints for each table."""
    query = """
        SELECT 
            TABLE_NAME,
            COLUMN_NAME
        FROM 
            information_schema.COLUMNS
        WHERE 
            TABLE_SCHEMA = %s
            AND IS_NULLABLE = 'NO'
            AND TABLE_NAME IN (
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_TYPE = 'BASE TABLE'
            )
        ORDER BY 
            TABLE_NAME, COLUMN_NAME
    """
    cursor.execute(query, (schema, schema))
    results = cursor.fetchall()

    # Organize NOT NULL columns by table
    not_null_columns = {}
    for table_name, column_name in results:
        if table_name not in not_null_columns:
            not_null_columns[table_name] = []
        not_null_columns[table_name].append(column_name)
    return not_null_columns

def check_null_and_blank_values():
    try:
        # Establish connection
        connection = s2.connect(**db_config)
        cursor = connection.cursor()

        # Get NOT NULL columns for all tables in the schema
        not_null_columns = get_not_null_columns(cursor, db_config['database'])

        if not not_null_columns:
            print(f"No tables with NOT NULL columns found in schema '{db_config['database']}'")
            return

        # Check each table for NULL and blank values in NOT NULL columns
        for table_name, columns in not_null_columns.items():
            print(f"\nChecking table: {table_name}")
            print(f"NOT NULL columns: {', '.join(columns)}")
            print("-" * 50)

            for column in columns:
                # Query for NULL count
                null_query = f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE {column} IS NULL
                """
                # Query for blank count (empty string or whitespace)
                blank_query = f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE TRIM({column}) = '' OR {column} IS NULL
                """

                try:
                    # Check NULL values
                    cursor.execute(null_query)
                    null_count = cursor.fetchone()[0]
                    
                    # Check blank values
                    cursor.execute(blank_query)
                    blank_count = cursor.fetchone()[0]

                    print(f"Column: {column}")
                    print(f"NULL values: {null_count}")
                    print(f"Blank values ('' or whitespace): {blank_count}")
                    if null_count > 0:
                        print(f"WARNING: {null_count} NULL(s) found in NOT NULL column!")
                    if blank_count > 0:
                        print(f"NOTE: {blank_count} blank value(s) found.")
                    print("-" * 50)

                except s2.Error as e:
                    print(f"Error querying column '{column}' in table '{table_name}': {e}")

    except s2.Error as e:
        print(f"Error connecting to SingleStore: {e}")

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    check_null_and_blank_values()
