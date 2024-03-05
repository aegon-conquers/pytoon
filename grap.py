from pyspark import SparkContext
from pyspark.sql import SparkSession
from graphframes import GraphFrame

# Initialize SparkContext and SparkSession
sc = SparkContext()
spark = SparkSession.builder.appName("ConnectedComponentsExample").getOrCreate()

# Sample data
vertices = spark.createDataFrame([
    ("1", "Alice"),
    ("2", "Bob"),
    ("3", "Charlie"),
    ("4", "David"),
    ("5", "Emma"),
    ("6", "Frank")
], ["id", "name"])

edges = spark.createDataFrame([
    ("1", "2", "friend"),
    ("2", "3", "friend"),
    ("3", "4", "friend"),
    ("4", "5", "friend"),
    ("5", "6", "friend")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(vertices, edges)

# Find the connected components
connectedComponents = g.connectedComponents()

# Show the connected components
connectedComponents.show()

# Stop SparkContext
sc.stop()
