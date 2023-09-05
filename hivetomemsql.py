from pyspark.sql import SparkSession
import requests
import json

# Initialize Spark session
spark = SparkSession.builder.appName("HiveToMemSQL").enableHiveSupport().getOrCreate()

# Read data from Hive table
hive_table = "iq.recomm"
columns = ["id", "notes", "recomm"]
hive_data = spark.sql(f"SELECT {','.join(columns)} FROM {hive_table}")

# Convert Hive DataFrame to JSON array
json_data = hive_data.toJSON().collect()

# Loop through JSON records and process based on configuration
for record in json_data:
    data = json.loads(record)

    # Configure which action to perform
    update_memsql = True  # Set to True to update MemSQL
    call_api = True       # Set to True to call the API

    if update_memsql:
        # Update MemSQL table
        # Modify this section to update MemSQL using the data
        # You can use the previous example for updating MemSQL table

    if call_api:
        # Call API for each record
        api_url = "your_api_endpoint"
        headers = {"Content-Type": "application/json"}

        # Make the API request
        try:
            response = requests.post(api_url, data=record, headers=headers)
            response_json = response.json()
            print("API response:", response_json)
        except requests.exceptions.RequestException as e:
            print("API request failed:", e)

# Stop Spark session
spark.stop()
