import sys
import argparse
from pyspark.sql import SparkSession
from mylib.extract_pyspark import extract
from mylib.transform_load_pyspark import transform_and_load
from mylib.query_pyspark import execute_query
from mylib.config import default_query


def handle_arguments(args):
    """Parse CLI arguments and return the parsed arguments"""
    parser = argparse.ArgumentParser(description="ETL-Query script")
    parser.add_argument(
        "action",
        choices=[
            "extract",
            "transform_load",
            "general_query",
        ],
    )
    parser.add_argument("query", nargs="?", default="default")
    return parser.parse_args(args)


def check_and_create_table(spark):
    """Check if the table exists, and create it from the Parquet file if not."""
    table_name = "global_temp.tbl_housing_data"
    parquet_file = "data/housing_data_transformed.parquet"

    if not spark.catalog.tableExists("global_temp", "tbl_housing_data"):
        print(f"Table {table_name} does not exist. Creating from Parquet file...")
        df = spark.read.parquet(parquet_file)
        df.createOrReplaceGlobalTempView("tbl_housing_data")
        print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} exists.")


def main():
    """Handle CLI commands and execute appropriate actions"""
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        print("Extracting data...")
        extract()
    elif args.action == "transform_load":
        print("Transforming and loading data...")
        transform_and_load()
    elif args.action == "general_query":
        spark = SparkSession.builder.appName("ETL Main Query").getOrCreate()
        check_and_create_table(spark)
        query = default_query if args.query == "default" else args.query
        execute_query(query, spark)
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
