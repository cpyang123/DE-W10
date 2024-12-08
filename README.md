[![Python Application Test with Github Actions for DE-W10](https://github.com/cpyang123/DE-W10/actions/workflows/test.yml/badge.svg)](https://github.com/cpyang123/DE-W10/actions/workflows/test.yml)

# DE-W6 ETL-Query Script

This is a command-line interface (CLI) script that handles various ETL (Extract, Transform, Load) processes and querying operations using PySpark. The script provides several actions to work with data extraction, loading, and querying, using a set of predefined commands.

## Requirements

- Python 3.x
- PySpark 3.x
- Required dependencies (install with `pip install -r requirements.txt`)

### Dependencies:
- `pyspark`
- `databricks-sql-connector`
- `python-dotenv`
- Other packages listed in `requirements.txt`

## Usage

Run the script with one of the supported actions. The available actions are:

- `extract`: Extracts data from a source.
- `transform_load`: Transforms and loads data into a destination using PySpark.
- `general_query`: Run a general query. You can specify a custom query or use the default one.

### Example Commands:

1. **Extract Data:**
   ```bash
   python main.py extract
   ```
2. **Transform and Load Data:**
   ```bash
   python main.py transform_load
   ```
3. **Run a General Query:**
   ```bash
   python main.py general_query "SELECT * FROM global_temp.tbl_housing_data"
   ```

If no custom query is provided, or if "default" is specified, the script will run the default query.

### Query Behavior:

In the "general_query" action, the script checks if the table `tbl_housing_data` exists. If not, it automatically creates the table from the Parquet file generated during the `transform_load` step.

## Notes

- Ensure that the `mylib` package is in your Python path and includes modules for `extract`, `transform_load`, and `query` functionalities.
- The `transform_load` action generates a Parquet file at `data/housing_data_transformed.parquet` for persistent storage.
- The `general_query` action uses PySpark's SQL capabilities and handles table creation dynamically if needed.

## Testing

Tests are included for the main script functionalities:

- Extracting data
- Transforming and loading data
- Running general queries

Run the tests using:
```bash
pytest
```

## Contributions

Contributions are welcome! Please fork the repository and submit a pull request with any improvements or additional features.

## License

This project is licensed under the MIT License.

