#!/bin/bash

# Set the path to your PySpark script
PYSPARK_SCRIPT="your_script.py"

# Set the Spark master URL
SPARK_MASTER="spark://your_spark_master:7077"  # Replace with your Spark master URL

# Additional configuration options (if needed)
SPARK_OPTIONS="--num-executors 4 --executor-memory 4G"

# Set the Python version (e.g., python3 or python)
PYTHON="python3"

# Submit the PySpark script to the cluster
$PYTHON $PYSPARK_SCRIPT \
  --master $SPARK_MASTER \
  $SPARK_OPTIONS
