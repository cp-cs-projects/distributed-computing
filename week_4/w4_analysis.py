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


def top_20_pixels():
    # Connect to DuckDB
    connection = duckdb.connect()

    data_query = """
        SELECT coordinate, COUNT(*) AS occurrence_count
        FROM '2022_pyarrow.parquet'
        GROUP BY coordinate
        ORDER BY occurrence_count DESC
        LIMIT 20
    """
    rows = connection.execute(data_query).fetchall()
    
    for i in range(len(rows)):
        print(i+1, rows[i])

def highest_time(coord):

    connection = duckdb.connect()

    data_query = f"""
    SELECT CAST(timestamp AS DATE) AS day, COUNT(*) AS occurrence_count
    FROM '2022_pyarrow.parquet'
    WHERE coordinate = {coord}
    GROUP BY day
    ORDER BY occurrence_count DESC
    LIMIT 10;
    """
    rows = connection.execute(data_query).fetchall()
    return rows

def canvas_size():

    connection = duckdb.connect()

    # View a sample of rows from the Parquet file
    data_query = """
    SELECT MIN(coordinate), MAX(coordinate)
    FROM '2022_pyarrow.parquet'
    """
    rows = connection.execute(data_query).fetchall()
    return rows

def coordinate_color(coord):

    connection = duckdb.connect()

    # View a sample of rows from the Parquet file
    data_query = f"""
    SELECT pixel_color, COUNT(*) as occurrence_count
    FROM '2022_pyarrow.parquet'
    WHERE coordinate = '{coord}'
    GROUP BY pixel_color
    ORDER BY occurrence_count DESC
    LIMIT 10;
    """
    rows = connection.execute(data_query).fetchall()
    print(rows)

def top_colors():

    connection = duckdb.connect()

    # View a sample of rows from the Parquet file
    data_query = f"""
    SELECT pixel_color, COUNT(*) as occurrence_count
    FROM '2022_pyarrow.parquet'
    GROUP BY pixel_color
    ORDER BY occurrence_count DESC
    LIMIT 50;
    """
    rows = connection.execute(data_query).fetchall()
    print(rows)

# Call the function
top_colors()

