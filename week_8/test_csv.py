import duckdb

def check_parquet():
    # Connect to DuckDB
    connection = duckdb.connect()

    # View a sample of rows from the Parquet file
    data_query = """
        SELECT *
        FROM './india_weather.parquet'
        LIMIT 5
    """
    rows = connection.execute(data_query).fetchall()
    print("\nSample Rows:")
    for i,row in enumerate(rows):
        print(i, row)

check_parquet()