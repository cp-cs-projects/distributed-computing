import duckdb

def check_parquet():
    # Connect to DuckDB
    connection = duckdb.connect()

    # View a sample of rows from the Parquet file
    data_query = """
        SELECT *
        FROM '../week_3/2022_pyarrow.parquet'
        LIMIT 90
    """
    rows = connection.execute(data_query).fetchall()
    print("\nSample Rows:")
    for i,row in enumerate(rows):
        print(i, row)


def total_users():
    # Connect to DuckDB
    connection = duckdb.connect()

    data_query = """
        SELECT user_id, COUNT(*) AS occurrence_count
        FROM '../2022xy_pyarrow.parquet'
        GROUP BY user_id
    """
    rows = connection.execute(data_query).fetchall()
    
    for i in range(len(rows)):
        print(i+1, rows[i])



def canada_painters():
    connection = duckdb.connect()
    data_query = """
    SELECT COUNT(DISTINCT user_id) AS unique_user_count
    FROM '../2022xy_pyarrow.parquet'
    WHERE x BETWEEN 176 AND 243
      AND y BETWEEN 492 AND 527
    """
    rows = connection.execute(data_query).fetchall()
    print(rows)

import duckdb

def top_canada():
    connection = duckdb.connect()
    data_query = """
    SELECT pixel_color, COUNT(*) AS pixel_count
    FROM '../2022xy_pyarrow.parquet'
    WHERE x BETWEEN 176 AND 243
      AND y BETWEEN 492 AND 527
    GROUP BY pixel_color
    ORDER BY pixel_count DESC
    LIMIT 10
    """
    rows = connection.execute(data_query).fetchall()
    print(rows)

# Call the function
top_canada()

