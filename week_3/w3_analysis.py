from time import perf_counter_ns
import sys
from datetime import datetime
import duckdb
import webcolors


def rank_colors(conn, start, end):
    """
    rank colors by # of distinct users that placed those colors
    """

    get_ranking = f"""
        SELECT pixel_color, COUNT(DISTINCT user_id) as distinct_users
        FROM './2022_pyarrow.parquet'
        WHERE timestamp >= '{start}' 
          AND timestamp < '{end}'
        GROUP BY pixel_color
        ORDER BY distinct_users DESC
    """
    rankings = conn.execute(get_ranking).fetchall()
    for i in range(len(rankings)):
        try:
            color = webcolors.hex_to_name(rankings[i][0])
        except ValueError:
            color = rankings[i][0]                         

        print (i+1,"-", color,":",rankings[i][1], "users")

    


def avg_session_length(conn, start, end):
    """
    session = user activity within 15-min window of inactivity
    return average session length in seconds in timeframe,
    only incl. cases where a user had more than 1 pixel placement in timeframe

    within timeframe:
    - find users with 1+ event in frame
    - order user events by timestamp to identify when new session occurs
    - get session start and end times, number events in session, session length
    - exclude users with only one event
    - compute avg session length
    """

    session_length = f"""
        WITH sessions AS (
            SELECT user_id, timestamp, 
                LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS prev_timestamp
            FROM './2022_pyarrow.parquet'
            WHERE timestamp >= '{start}' AND timestamp < '{end}'
        ), 
        lengths AS (
            SELECT user_id,
                EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as session_length
            FROM sessions
            WHERE EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) <= 900
        ),
        valid_users AS (
            SELECT user_id
            FROM lengths
            GROUP BY user_id
            HAVING COUNT(*) > 1
        )
        SELECT 
            AVG(session_length) AS avg_session_length
        FROM lengths l
        JOIN valid_users v ON l.user_id = v.user_id
    """

    return conn.execute(session_length).fetchone()


def pixel_placements(conn, start, end):
    """
    50th, 75th, 90th, and 99th percentiles of num pixels
    1. get counts per user_id within timestamp
    2. calculate percentiles ordered by count
    """

    user_counts = f"""
        with user_counts AS
        (
            SELECT user_id, COUNT(*) AS pixels_counted
            FROM './2022_pyarrow.parquet'
            WHERE timestamp >= '{start}'
                AND timestamp < '{end}'
            GROUP BY user_id
        )
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP(ORDER BY pixels_counted) AS median,
            PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY pixels_counted) AS p75,
            PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY pixels_counted) AS p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP(ORDER BY pixels_counted) AS p99
        FROM
            user_counts
        """
    
    return conn.execute(user_counts).fetchall()

  

def count_first_time_users(conn, start, end):
    """
    how many users placed their first pixel ever within the specified timeframe
    1. get first placement time for each user
    2. filter users where first time falls between start and end.
    """

    first_pixel = f"""
        SELECT COUNT(*)
        FROM (
            SELECT user_id, MIN(timestamp) AS first_placement
            FROM './2022_pyarrow.parquet'
            GROUP BY user_id
        )
        WHERE first_placement >= '{start}'
            AND first_placement < '{end}'
    """

    return conn.execute(first_pixel).fetchone()[0]

    

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
        start_counter = perf_counter_ns()
        start_dt = datetime.strptime(start, "%Y-%m-%d %H")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H")
        conn = duckdb.connect()
        print("ranking:")
        ranked_colors = rank_colors(conn, start_dt, end_dt)
        print("\n # average session length: ", avg_session_length(conn, start_dt, end_dt))
        print("\n percentiles of pixels placed: ", pixel_placements(conn, start_dt, end_dt))
        print("\n # first time users: ", count_first_time_users(conn, start_dt, end_dt))
        end_counter = perf_counter_ns()
        print("\n time elapsed: ", end_counter - start_counter, "ns")

