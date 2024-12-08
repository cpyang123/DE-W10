from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def execute_query(query, spark_session=None):
    """Executes a SQL query using PySpark"""
    if spark_session is None:
        spark = SparkSession.builder.appName("ETL Query").getOrCreate()
    else:
        spark = spark_session

    try:
        result = spark.sql(query)
        result.show()  # Display the result
        return result
    except AnalysisException as e:
        print(f"Query execution failed: {e}")
        return None

def log_query(query, result):
    """Logs a query and its result"""
    log_file = "query_log.md"
    with open(log_file, "a") as file:
        file.write(f"```sql\n{query}\n```")
        file.write(f"```response\n{result}\n```")

# Example usage:
def main():
    spark = SparkSession.builder.appName("Query Example").getOrCreate()
    query = """
        SELECT * FROM global_temp.tbl_housing_data
    """
    result = execute_query(query, spark)
    log_query(query, "Result displayed in console")

if __name__ == "__main__":
    main()
