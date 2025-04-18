from pyspark.sql import SparkSession
import pymysql

# Reuse the get_columns and generate_query functions from above

def main():
    parser = argparse.ArgumentParser(description="Transfer data from landing to prod tables with PySpark")
    parser.add_argument('--landing_schema', required=True, help='Landing schema name')
    parser.add_argument('--prod_schema', required=True, help='Prod schema name')
    parser.add_argument('--table_name', required=True, help='Table name')
    parser.add_argument('--host', required=True, help='SingleStoreDB host')
    parser.add_argument('--port', type=int, default=3306, help='SingleStoreDB port')
    parser.add_argument('--database', required=True, help='SingleStoreDB database')
    parser.add_argument('--user', required=True, help='SingleStoreDB user')
    parser.add_argument('--password', required=True, help='SingleStoreDB password')
    parser.add_argument('--output_path', required=True, help='Output path for verification CSV')
    args = parser.parse_args()

    # Get columns
    columns = get_columns(
        args.host, args.port, args.database, args.user, args.password,
        args.prod_schema, args.table_name
    )

    if not columns:
        raise ValueError(f"No columns found for table {args.prod_schema}.{args.table_name}")

    # Generate the query
    query = generate_query(args.landing_schema, args.prod_schema, args.table_name, columns)

    # Initialize Spark session
    spark = SparkSession.builder.appName("TransferToProd").getOrCreate()

    # Execute the query via JDBC
    try:
        spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{args.host}:{args.port}/{args.database}?user={args.user}&password={args.password}") \
            .option("query", query) \
            .load()
        print(f"Successfully executed query: {query}")

        # Optionally, verify the prod table and write to CSV
        result_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{args.host}:{args.port}/{args.database}?user={args.user}&password={args.password}") \
            .option("query", f"SELECT * FROM {args.prod_schema}.{args.table_name}") \
            .load()
        result_df.write.csv(args.output_path, mode='overwrite', header=True)

    except Exception as e:
        print(f"Error executing query: {e}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
