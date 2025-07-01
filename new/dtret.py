import yaml
import singlestoredb as s2

# Database connection parameters
connection_params = {
    'host': 'your_host',
    'port': 3306,
    'user': 'your_user',
    'password': 'your_password',
    'database': 'prod_db'
}

# Read tables from config.yaml
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
tables = config['tables']

# Connect to SingleStore DB
conn = s2.connect(**connection_params)

def get_unique_key_columns(db_connection, table_name):
    query = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
        AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = 'prod_db'
        AND tc.table_name = %s
    ORDER BY kcu.ordinal_position;
    """
    with db_connection.cursor() as cursor:
        cursor.execute(query, (table_name,))
        return [row[0] for row in cursor.fetchall()]

# Process each table
for table in tables:
    try:
        # Get unique key columns
        unique_keys = get_unique_key_columns(conn, table)
        if not unique_keys:
            print(f"No unique key constraints found for table {table}. Skipping.")
            continue

        # Construct the join condition for unique keys
        join_conditions = [f"p.{col} = l.{col}" for col in unique_keys]
        join_clause = " AND ".join(join_conditions)
        
        # Construct the DELETE query
        delete_query = f"""
        DELETE p FROM prod_db.{table} p
        LEFT JOIN landing_db.{table} l
            ON {join_clause}
        WHERE l.{unique_keys[0]} IS NULL;
        """
        
        # Execute the DELETE query
        with conn.cursor() as cursor:
            cursor.execute(delete_query)
            deleted_rows = cursor.rowcount
            print(f"Deleted {deleted_rows} rows from prod_db.{table} where unique keys not in landing_db.")
        
        # Commit the transaction
        conn.commit()
        
    except Exception as e:
        print(f"Error processing table {table}: {str(e)}")
        conn.rollback()

# Close the connection
conn.close()
