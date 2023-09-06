from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("JSONArraySplit").getOrCreate()

# Sample DataFrame with a JSON array column
data = [("John", '[{"name": "John", "age": 30, "dob": "1992-01-15"}, {"name": "Darth", "age": 40, "dob": "1982-05-25"}]'),
        ("Darth", '[{"name": "Darth", "age": 45, "dob": "1977-11-03"}]'),
        ("Alice", '[]')]

columns = ["name", "json_data"]

df = spark.createDataFrame(data, columns)

# Split the JSON array into separate columns for each name (John and Darth)
df = df.withColumn("john_data", when(col("json_data").contains('John'), col("json_data")).otherwise("[]"))
df = df.withColumn("darth_data", when(col("json_data").contains('Darth'), col("json_data")).otherwise("[]"))

# Define a UDF (User-Defined Function) to extract attributes from the JSON objects
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

def extract_attributes(json_data):
    if json_data:
        # Parse the JSON array and extract attributes
        try:
            json_objects = eval(json_data)
            names_data = {}
            for obj in json_objects:
                name = obj["name"]
                age = obj["age"]
                dob = obj["dob"]
                names_data[name + "_age"] = age
                names_data[name + "_dob"] = dob
            return names_data
        except Exception as e:
            pass
    return {}

# Define the schema for the extracted attributes
schema = StructType([
    StructField("John_age", IntegerType(), True),
    StructField("John_dob", DateType(), True),
    StructField("Darth_age", IntegerType(), True),
    StructField("Darth_dob", DateType(), True)
])

# Create a UDF based on the extract_attributes function
extract_attributes_udf = udf(extract_attributes, schema)

# Apply the UDF to create new columns with extracted attributes
df = df.withColumn("attributes", extract_attributes_udf(col("json_data")))

# Select the desired columns
result_df = df.select("name", "attributes.*")

# Show the result
result_df.show()
