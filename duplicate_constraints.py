import singlestoredb as s2
from config import db_config

def get_unique_keys(cursor, schema):
    """Fetch unique keys grouped by constraint name for each table."""
    query = """
        SELECT 
            t.TABLE_NAME,
            c.COLUMN_NAME,
            t.CONSTRAINT_NAME
        FROM 
            information_schema.TABLE_CONSTRAINTS t
        JOIN 
            information_schema.KEY_COLUMN_USAGE c
        ON 
            t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
            AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND t.TABLE_NAME = c.TABLE_NAME
        WHERE 
            t.TABLE_SCHEMA = %s
            AND t.CONSTRAINT_TYPE = 'UNIQUE'
        ORDER BY 
            t.TABLE_NAME, t.CONSTRAINT_NAME, c.COLUMN_NAME
    """
    cursor.execute(query, (schema,))
    results = cursor.fetchall()

    # Organize unique keys by table and constraint
    unique_keys = {}
    for table_name, column_name, constraint_name in results:
        if table_name not in unique_keys:
            unique_keys[table_name] = {}
        if constraint_name not in unique_keys[table_name]:
            unique_keys[table_name][constraint_name] = []
        unique_keys[table_name][constraint_name].append(column_name)
    return unique_keys

def check_duplicates():
    try:
        connection = s2.connect(**db_config)
        cursor = connection.cursor()

        unique_keys = get_unique_keys(cursor, db_config['database'])

        if not unique_keys:
            print(f"No unique key constraints found in schema '{db_config['database']}'")
            return

        for table_name, constraints in unique_keys.items():
            print(f"\nChecking table: {table_name}")
            print("-" * 50)
            for constraint_name, columns in constraints.items():
                print(f"Unique key constraint '{constraint_name}': {', '.join(columns)}")

                column_list = ', '.join(columns)
                query = f"""
                    SELECT {column_list}, COUNT(*) as count
                    FROM {table_name}
                    GROUP BY {column_list}
                    HAVING COUNT(*) > 1
                """
                try:
                    cursor.execute(query)
                    duplicates = cursor.fetchall()

                    if duplicates:
                        print(f"Found duplicates for constraint '{constraint_name}':")
                        for row in duplicates:
                            values = row[:-1]
                            count = row[-1]
                            print(f"Values: {values} | Occurrences: {count}")
                    else:
                        print(f"No duplicates found for constraint '{constraint_name}'.")
                except s2.Error as e:
                    print(f"Error querying table '{table_name}' for '{constraint_name}': {e}")
                print("-" * 50)

    except s2.Error as e:
        print(f"Error connecting to SingleStore: {e}")

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    check_duplicates()
