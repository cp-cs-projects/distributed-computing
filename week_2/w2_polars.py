import sys
import polars as pl
from time import perf_counter_ns
from datetime import datetime

'''
- analyze (start, end) : most placed color , most placed pixel location
- start and end hrs as CLI arguments, YYYY-MM-DD HH, validate end hour is after start

data example:
['timestamp:str', 'user_id:str', 'pixel_color:str', 'coordinate:str']
['2022-04-04 01:31:15.044 UTC', 'D23Fyt66c0/bremR1gMZYfxiSSH10/jlFKilZjZJVtCPL+okOejfsERw3ccyVgjQhlPJzKoxc05QyJ2jFm1btA==', '#000000', '810,198']
'''

def pandas_analyze_data(start, end):

    start_dt = datetime.strptime(start, '%Y-%m-%d %H')
    end_dt = datetime.strptime(end, '%Y-%m-%d %H')
    print(start_dt, end_dt)

    start_counter = perf_counter_ns()

    df = pl.read_parquet('2022_pyarrow.parquet')
    
    filtered_data = df.filter(
        (pl.col("timestamp") >= start_dt) &
        (pl.col("timestamp") < end_dt)
    )

    max_color = filtered_data["pixel_color"].mode()
    max_pixel = filtered_data["coordinate"].mode()
    
    end_counter = perf_counter_ns()
    return max_color, max_pixel, end_counter - start_counter
    

if __name__ == '__main__':

    if len(sys.argv) != 5:
        print("Usage: python3 w2_pandas.py YYYY-MM-DD HH YYYY-MM-DD HH")
        sys.exit(1)

    start = sys.argv[1] + " " + sys.argv[2] # 2022-04-04 01
    end = sys.argv[3] + " " + sys.argv[4]

    if start >= end:
        print("End hour must be after start hour")
        sys.exit(1)
    else:
        max_color, max_pixel, time_elapsed = pandas_analyze_data(start, end)
        print(f"Time elapsed: {time_elapsed} ns")
        print(f"Most placed color: {max_color}")
        print(f"Most placed pixel: {max_pixel}")
        
    #direct comps work - print('2022-04-04 01:35:54.071 UTC' < '2022-04-04 01:35:55.263 UTC')