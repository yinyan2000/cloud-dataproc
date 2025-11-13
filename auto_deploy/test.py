from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Dataproc GCS Write Example").getOrCreate()

# Create a sample DataFrame
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["name", "id"]
df = spark.createDataFrame(data, columns)

# GCS path to write the data
gcs_path = "gs://yinyan-dataproc-bucket-1/deploy-auto"

# Write the DataFrame to GCS in Parquet format
df.write.mode("overwrite").parquet(gcs_path)

print(f"Successfully wrote data to {gcs_path}")

# Stop the Spark session
spark.stop()
