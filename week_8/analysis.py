import duckdb
import folium
import pandas as pd
import visualize

def check_parquet():
    # Connect to DuckDB
    connection = duckdb.connect()

    # View a sample of rows from the Parquet file
    query = """
        SELECT *
        FROM './india_weather.parquet'
        LIMIT 5
    """
    rows = connection.execute(query).fetchall()
    print("\nSample Rows:")
    for i,row in enumerate(rows):
        print(i, row)

def avg_temp():
    # Connect to DuckDB
    connection = duckdb.connect()

    # Calculate average temperature for each city (DESC and ASC, 2024 v. 2010)
    avg_temp_query = """
        SELECT city, AVG(temperature_2m) AS avg_temp, lat, lng
        FROM './india_weather.parquet'
        WHERE YEAR(date) = 2024 
        GROUP BY city, lat, lng
        ORDER BY avg_temp DESC
        LIMIT 12;
    """
    rows = connection.execute(avg_temp_query).fetchall()
    print("\nAverage Temperature:")
    for r in rows:
        print(r)
    return rows

def highest_avg_felt_temp():
    connection = duckdb.connect()

    # Calculate highest differences in apparent temperature and actual temperature
    query = """
        SELECT city, AVG(apparent_temperature)-AVG(temperature_2m) AS felt_temp, lat, lng
        FROM './india_weather.parquet'
        GROUP BY city, lng, lat
        ORDER BY felt_temp DESC
        LIMIT 5;
    """
    rows = connection.execute(query).fetchall()
    print("\nHighest Difference:")
    for r in rows:
        print(r)
    return rows

def highest_avg_diff():
    connection = duckdb.connect()

    query = """
        WITH avg_temp_2010 AS (
            SELECT city, AVG(temperature_2m) AS avg_temp_2010, lat, lng
            FROM './india_weather.parquet'
            WHERE YEAR(date) = 2010
            GROUP BY city, lat, lng
        ),
        avg_temp_2024 AS (
            SELECT city, AVG(temperature_2m) AS avg_temp_2024, lat, lng
            FROM './india_weather.parquet'
            WHERE YEAR(date) = 2024
            GROUP BY city, lat, lng
        )
        SELECT t2010.city, t2024.avg_temp_2024 - t2010.avg_temp_2010 AS avg_diff, t2010.lat, t2010.lng
        FROM avg_temp_2010 t2010
        JOIN avg_temp_2024 t2024 ON t2010.city = t2024.city
        ORDER BY avg_diff DESC
        LIMIT 10;
    """
    rows = connection.execute(query).fetchall()
    print("\nHighest Difference in Avg between 2010 and 2024:")
    for r in rows:
        print(r)
    return rows

def diurnal_temp():
    connection = duckdb.connect()

    query = """
        WITH daily_temps_2024 AS (
            SELECT 
                city, lat, lng, 
                DAY(date) AS day, 
                MAX(temperature_2m) AS daily_max,
                MIN(temperature_2m) AS daily_min
            FROM './india_weather.parquet'
            WHERE YEAR(date) = 2024
            GROUP BY city, lat, lng, day
        )
        SELECT 
            city, ABS(AVG(daily_max) - AVG(daily_min)) AS dtr_2024, lat, lng, 
        FROM daily_temps_2024
        GROUP BY city, lat, lng
        ORDER BY dtr_2024 ASC
        LIMIT 10;
    """
    rows = connection.execute(query).fetchall()
    print("\nDiurnal Temp:")
    for r in rows:
        print(r)
    return rows

def lowest_humidity():
    connection = duckdb.connect()

    query = """
        SELECT city, AVG(relative_humidity_2m) AS avg_humidity
        FROM './india_weather.parquet'
        GROUP BY city
        ORDER BY avg_humidity DESC
        LIMIT 5;
    """
    rows = connection.execute(query).fetchall()
    print("Lowest Humidity:")
    for row in rows:
        print(row)

def night_temp_diffs():
    connection = duckdb.connect()

    query = """
        WITH night_temps_2010 AS (
            SELECT 
                city, lat, lng, 
                AVG(temperature_2m) AS night_temp_2010
            FROM './india_weather.parquet'
            WHERE YEAR(date) = 2010 
              AND (HOUR(date) >= 18 OR HOUR(date) <= 6)
            GROUP BY city, lat, lng
        ),
        night_temps_2024 AS (
            SELECT 
                city, lat, lng, 
                AVG(temperature_2m) AS night_temp_2024
            FROM './india_weather.parquet'
            WHERE YEAR(date) = 2024 
              AND (HOUR(date) >= 18 OR HOUR(date) <= 6)
            GROUP BY city, lat, lng
        )
        SELECT 
            t2010.city, 
            t2024.night_temp_2024 - t2010.night_temp_2010 AS night_temp_diff,
            t2010.lat, 
            t2010.lng
        FROM night_temps_2010 t2010
        JOIN night_temps_2024 t2024 ON t2010.city = t2024.city 
        ORDER BY night_temp_diff DESC
        LIMIT 10;
    """
    
    rows = connection.execute(query).fetchall()
    print("\nTop 10 Cities with Highest Night Temperature Increase (2010 - 2024):")
    for r in rows:
        print(r)
    
    return rows

def city_vs_surroundings():
    connection = duckdb.connect()

    query = """
        WITH urban_city AS (
            SELECT lat, lng, AVG(temperature_2m) AS avg_temp_city
            FROM './india_weather.parquet'
            WHERE city = 'Ahmedabad'
            GROUP BY city, lat, lng
            LIMIT 1
        )
        SELECT city, AVG(temperature_2m) AS avg_temp, india_weather.lat, india_weather.lng, urban_city.avg_temp_city, urban_city.lat, urban_city.lng
        FROM './india_weather.parquet', urban_city
        WHERE 
            (india_weather.lat BETWEEN urban_city.lat - 1 AND urban_city.lat + 1) 
            AND (india_weather.lng BETWEEN urban_city.lng - 1 AND urban_city.lng + 1)
            AND YEAR(date) = 2024
        GROUP BY city, india_weather.lat, india_weather.lng, urban_city.avg_temp_city, urban_city.lat, urban_city.lng
        ORDER BY avg_temp ASC
        LIMIT 6;
    """
    rows = connection.execute(query).fetchall()
    for r in rows:
        print(r)
    return rows

# wrote diff function calls 
visualize.map_data(city_vs_surroundings())