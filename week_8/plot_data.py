import duckdb
import matplotlib as plt

# note - attempted to create this plot. Did not work.

def top_times():
    connection = duckdb.connect()
    data_query = """
    SELECT city, date, AVG(temperature_2m) AS avg_temp, lat, lng
        FROM './india_weather.parquet'
        GROUP BY city, lat, lng, date
        ORDER BY avg_temp DESC
        LIMIT 10;
    """

    df = connection.execute(data_query).fetchdf()
    df = df.sort_values(by='date')

    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['avg_temp'], marker='o', linestyle='-', label="Avg Temp Per Hour")
    
    plt.xlabel("Time")
    plt.ylabel("Temperature")
    plt.title("Average Temperature Increase from 2010 to 2024")
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    
    # Show the plot
    plt.show()

top_times()