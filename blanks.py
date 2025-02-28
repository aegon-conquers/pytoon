import singlestoredb as s2
from config import db_config

def get_all_tables(cursor, schema):
    """Fetch all base tables in the schema."""
    query = """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """
    cursor.execute(query, (schema,))
    return [row[0] for row in cursor.fetchall()]

def check_blanks_in_table(cursor, table_name):
    """Check for blank values in all columns of a table."""
    # Get all columns for the table
    query = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        ORDER BY COLUMN_NAME
    """
    cursor.execute(query, (db_config['database'], table_name))
    columns = cursor.fetchall()

    if not columns:
        print(f"No columns found for table '{table_name}'")
        return

    print(f"\nChecking for blank values in table: {table_name}")
    found_blanks = False
    for column_name, data_type in columns:
        if data_type in ('char', 'varchar', 'text', 'mediumtext', 'longtext'):
            blank_query = f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE TRIM({column_name}) = ''
            """
        else:
            blank_query = f"""
                SELECT 0 
                FROM {table_name} 
                WHERE 1=0
            """

        try:
            cursor.execute(blank_query)
            result = cursor.fetchone()
            blank_count = result[0] if result is not None else 0  # Handle None case

            if blank_count > 0:
                found_blanks = True
                print(f"{column_name}: {blank_count}")

        except s2.Error as e:
            print(f"Error querying column '{column_name}' in table '{table_name}': {e}")

    if not found_blanks:
        print(f"No blank values found in any column of table '{table_name}'.")

def check_blanks_all_tables():
    try:
        connection = s2.connect(**db_config)
        cursor = connection.cursor()

        # Get all tables in the schema
        tables = get_all_tables(cursor, db_config['database'])

        if not tables:
            print(f"No tables found in schema '{db_config['database']}'")
            return

        # Check each table
        for table_name in tables:
            check_blanks_in_table(cursor, table_name)

    except s2.Error as e:
        print(f"Error connecting to SingleStore: {e}")

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("\nConnection closed.")

if __name__ == "__main__":
    check_blanks_all_tables()
