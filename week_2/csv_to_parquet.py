import pandas as pd

csv_path = './2022_place_canvas_history.csv'
parquet_path = './2022_place_canvas_history.parquet'
parquet_file = None

for chunk in pd.read_csv(csv_path, chunksize=100000):
    # Convert the timestamp column to datetime format
    chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], format='mixed')

    if not parquet_file:
        parquet_file = chunk.to_parquet(parquet_path, engine="pyarrow", compression='snappy', index=False)
    else:
        chunk.to_parquet(parquet_path, append=True, engine="pyarrow", compression='snappy', index=False)

print(f"CSV converted to Parquet at {parquet_path}")
