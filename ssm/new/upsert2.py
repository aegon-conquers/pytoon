import pymysql
import argparse

def get_columns_and_keys(host, port, database, user, password, schema, table):
    """Retrieve column names and unique key columns for a table."""
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    try:
        with connection.cursor() as cursor:
            # Get all column names
            cursor.execute("""
                SELECT column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            columns = [row[0] for row in cursor.fetchall()]

            # Get unique key columns (primary key or unique index)
            cursor.execute("""
                SELECT kcu.column_name
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                ORDER BY kcu.ordinal_position
            """, (schema, table))
            unique_key_columns = [row[0] for row in cursor.fetchall()]

            return columns, unique_key_columns
    finally:
        connection.close()

def generate_query(landing_schema, prod_schema, table_name, columns, unique_key_columns):
    """Generate the INSERT ... ON DUPLICATE KEY UPDATE query, excluding unique key columns from UPDATE."""
    # All columns for INSERT and SELECT
    columns_str = ', '.join(columns)

    # Non-unique key columns for ON DUPLICATE KEY UPDATE
    non_key_columns = [col for col in columns if col not in unique_key_columns]
    if not non_key_columns:
        raise ValueError(f"No non-unique key columns to update for table {prod_schema}.{table_name}")

    update_clause = ', '.join([f'{col} = VALUES({col})' for col in non_key_columns])

    query = f"""
        INSERT INTO {prod_schema}.{table_name} ({columns_str})
        SELECT {columns_str} FROM {landing_schema}.{table_name}
        ON DUPLICATE KEY UPDATE {update_clause}
    """
    return query

def execute_query(query, host, port, database, user, password):
    """Execute the query in SingleStoreDB."""
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=True
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            print(f"Query executed successfully: {query}")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise
    finally:
        connection.close()

def main():
    parser = argparse.ArgumentParser(description="Transfer data from landing to prod tables")
    parser.add_argument('--landing_schema', required=True, help='Landing schema name')
    parser.add_argument('--prod_schema', required=True, help='Prod schema name')
    parser.add_argument('--table_name', required=True, help='Table name')
    parser.add_argument('--host', required=True, help='SingleStoreDB host')
    parser.add_argument('--port', type=int, default=3306, help='SingleStoreDB port')
    parser.add_argument('--database', required=True, help='SingleStoreDB database')
    parser.add_argument('--user', required=True, help='SingleStoreDB user')
    parser.add_argument('--password', required=True, help='SingleStoreDB password')
    args = parser.parse_args()

    # Get columns and unique key columns
    columns, unique_key_columns = get_columns_and_keys(
        args.host, args.port, args.database, args.user, args.password,
        args.prod_schema, args.table_name
    )

    if not columns:
        raise ValueError(f"No columns found for table {args.prod_schema}.{args.table_name}")
    if not unique_key_columns:
        raise ValueError(f"No unique key found for table {args.prod_schema}.{args.table_name}")

    # Generate the query
    query = generate_query(args.landing_schema, args.prod_schema, args.table_name, columns, unique_key_columns)

    # Execute the query
    execute_query(
        query,
        args.host, args.port, args.database, args.user, args.password
    )

if __name__ == "__main__":
    main()
