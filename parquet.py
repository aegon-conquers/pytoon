from pyspark.sql import SparkSession
import os

def convert_to_parquet(input_file, output_folder):
    # Initialize Spark session
    spark = SparkSession.builder.appName("TabToParquet").getOrCreate()

    try:
        # Check if the output folder already exists
        if os.path.exists(output_folder):
            raise ValueError(f"Error: Output folder '{output_folder}' already exists. Choose a different folder.")

        # Read tab-separated text file into DataFrame
        df = spark.read.option("delimiter", "\t").csv(input_file, header=True, inferSchema=True)

        # Write DataFrame to Parquet format
        df.write.mode("overwrite").parquet(output_folder)

        print(f"Conversion successful. Parquet files written to {output_folder}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python script.py <input_tab_file> <output_parquet_folder>")
        sys.exit(1)

    input_tab_file = sys.argv[1]
    output_parquet_folder = sys.argv[2]

    convert_to_parquet(input_tab_file, output_parquet_folder)
