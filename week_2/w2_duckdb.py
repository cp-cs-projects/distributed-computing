import sys
import duckdb
from time import perf_counter_ns
from datetime import datetime

def duckdb_analyze_data(start, end):

    start_dt = datetime.strptime(start, '%Y-%m-%d %H')
    end_dt = datetime.strptime(end, '%Y-%m-%d %H')
    print(start_dt, end_dt)

    start_counter = perf_counter_ns()

    connection = duckdb.connect()

    get_max_color = f"""
        SELECT pixel_color
        FROM './2022_pyarrow.parquet'
        WHERE timestamp >= '{start_dt}' 
          AND timestamp < '{end_dt}'
        GROUP BY pixel_color
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """

    get_max_pixel = f"""
        SELECT coordinate
        FROM './2022_pyarrow.parquet'
        WHERE timestamp >= '{start_dt}'
          AND timestamp < '{end_dt}'
        GROUP BY coordinate
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """

    max_color = connection.execute(get_max_color).fetchone()
    max_pixel = connection.execute(get_max_pixel).fetchone()
    
    end_counter = perf_counter_ns()
    return max_color, max_pixel, end_counter - start_counter
    

if __name__ == '__main__':

    if len(sys.argv) != 5:
        print("Usage: python3 w2_duckdb.py YYYY-MM-DD HH YYYY-MM-DD HH")
        sys.exit(1)

    start = sys.argv[1] + " " + sys.argv[2] # 2022-04-04 01
    end = sys.argv[3] + " " + sys.argv[4]

    if start >= end:
        print("End hour must be after start hour")
        sys.exit(1)
    else:
        max_color, max_pixel, time_elapsed = duckdb_analyze_data(start, end)
        print(f"Time elapsed: {time_elapsed} ns")
        print(f"Most placed color: {max_color}")
        print(f"Most placed pixel: {max_pixel}")
        
    #direct comps work - print('2022-04-04 01:35:54.071 UTC' < '2022-04-04 01:35:55.263 UTC')