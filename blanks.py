import singlestoredb as s2
from config import db_config

def check_blanks_in_table(table_name):
    try:
        connection = s2.connect(**db_config)
        cursor = connection.cursor()

        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND TABLE_NAME IN (
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_TYPE = 'BASE TABLE'
            )
            ORDER BY COLUMN_NAME
        """
        cursor.execute(query, (db_config['database'], table_name, db_config['database']))
        columns = cursor.fetchall()

        if not columns:
            print(f"No columns found for table '{table_name}' in schema '{db_config['database']}'")
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
                blank_count = cursor.fetchone()[0]

                if blank_count > 0:
                    found_blanks = True
                    print(f"{column_name}: {blank_count}")

            except s2.Error as e:
                print(f"Error querying column '{column_name}' in table '{table_name}': {e}")

        if not found_blanks:
            print(f"No blank values found in any column of table '{table_name}'.")

    except s2.Error as e:
        print(f"Error connecting to SingleStore: {e}")

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    table_to_check = 'your_table_name'  # Replace with your table name
    check_blanks_in_table(table_to_check)
