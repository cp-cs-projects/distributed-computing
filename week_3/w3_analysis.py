from time import perf_counter_ns
import sys
from datetime import datetime
import duckdb


def rank_colors(start, end):
    """
    rank colors by # of distinct users that placed those colors
    """
    start_counter = perf_counter_ns()
    connection = duckdb.connect()
    results = f"""
        SELECT pixel_color
        FROM './2022_pyarrow.parquet'
        WHERE timestamp >= '{start}' 
          AND timestamp < '{end}'
        GROUP BY user_id
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """

    end_counter = perf_counter_ns()

    return ranking


def avg_session_length(start, end):
    """
    session = user activity within 15-min window of inactivity
    return average session length in seconds in timeframe,
    only incl. cases where a user had more than 1 pixel placement in timeframe
    """


def pixel_placements(start, end):
    """
    50th, 75th, 90th, and 99th percentiles of num pixels
    """


def count_first_time_users(start, end):
    """
    how many users placed their first pixel ever within the specified timeframe
    """


if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: python3 analyze.py YYYY-MM-DD HH YYYY-MM-DD HH")
        sys.exit(1)

    start = sys.argv[1] + " " + sys.argv[2]  # 2022-04-04 01
    end = sys.argv[3] + " " + sys.argv[4]

    if start >= end:
        print("End hour must be after start hour")
        sys.exit(1)
    else:
        start_dt = datetime.strptime(start, "%Y-%m-%d %H")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H")

        ranked_colors = rank_colors(start_dt, end_dt)
