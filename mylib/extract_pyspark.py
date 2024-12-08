from pyspark.sql import SparkSession

def extract(
    url="https://raw.githubusercontent.com/cpyang123/DE-W5/refs/heads/main/train.csv",
    file_path="data/housing_data.csv",
    directory="data",
):
    """Extract data from a URL to a Spark DataFrame"""
    spark = SparkSession.builder.appName("ETL Extract").getOrCreate()

    # Download the file (optional if you want to keep this step)
    import os
    import requests

    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    # Load CSV into a Spark DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Save the subset (if necessary) back to disk (optional in PySpark context)
    df.write.csv(file_path.replace(".csv", "_processed"), header=True, mode="overwrite")

    return df

# Example usage:
if __name__ == "__main__":
    extract()
