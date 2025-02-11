import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from time import perf_counter_ns

def percent_users():
    connection = duckdb.connect()

    data_query = """
        WITH total_users AS (
            SELECT COUNT(DISTINCT user_id) AS total_user_count
            FROM '../2022xy_pyarrow.parquet'
        ),
        canada_users AS (
            SELECT COUNT(DISTINCT user_id) AS canada_user_count
            FROM '../2022xy_pyarrow.parquet'
            WHERE x BETWEEN 176 AND 243
              AND y BETWEEN 492 AND 527 
        )
        SELECT 
            canada_user_count, (canada_users.canada_user_count * 100.0 / total_users.total_user_count) AS percentage
        FROM canada_users, total_users
    """

    rows = connection.execute(data_query).fetchall()
    return rows 


def top_canada_colors():
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
    return rows

def top_times():
    connection = duckdb.connect()
    data_query = """
    SELECT 
    CAST(timestamp AS DATE) AS date,
    EXTRACT(HOUR FROM timestamp) AS hour,
    COUNT(DISTINCT user_id) AS users
    FROM '../2022xy_pyarrow.parquet'
    WHERE x BETWEEN 176 AND 243
        AND y BETWEEN 492 AND 527
    GROUP BY date, hour
    ORDER BY users DESC, date, hour
    """
    rows = connection.execute(data_query).fetchall()
    return rows
    df = connection.execute(data_query).fetchdf()
    df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['hour'].astype(str) + ':00:00')
    df = df.sort_values(by='datetime')

    
    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(df['datetime'], df['users'], marker='o', linestyle='-', label="Users per hour")
    
    plt.xlabel("Time")
    plt.ylabel("Number of Users")
    plt.title("Activity Spikes in Canadian Flag Region (r/place)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    start_counter = perf_counter_ns()
    print("num users | percent of r/place users: ", percent_users())
    print("\ntop colors for Canada: ", top_canada_colors())
    print("top times:", top_times())
    end_counter = perf_counter_ns()
    print("\n time elapsed: ", end_counter - start_counter, "ns")
