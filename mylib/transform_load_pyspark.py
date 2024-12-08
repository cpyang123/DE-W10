from pyspark.sql import SparkSession

def transform_and_load(dataset_path="data/housing_data.csv"):
    """Transforms and loads data using PySpark"""
    spark = SparkSession.builder.appName("ETL Transform and Load").getOrCreate()

    # Load the dataset into a Spark DataFrame
    df = spark.read.csv(dataset_path, header=True, inferSchema=True)

    # Perform transformations (example: add a computed column)
    df_transformed = df.withColumn("RoomToBedRatio", df["AveRooms"] / df["AveBedrms"])

    # Register the DataFrame as a temporary SQL view for further queries
    df_transformed.createOrReplaceGlobalTempView("tbl_housing_data")

    # Save transformed data to a persistent storage (e.g., Parquet format)
    output_path = dataset_path.replace(".csv", "_transformed.parquet")
    df_transformed.write.parquet(output_path, mode="overwrite")

    print(f"Transformed data saved to {output_path}")

# Example usage:
if __name__ == "__main__":
    transform_and_load()
