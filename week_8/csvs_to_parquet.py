import pyarrow.csv as pv
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path

input_dir = Path("/Users/srirocks2020/Desktop/w_d_1")
parquet_file = "india_weather.parquet"
coords_dir = "./weather.csv"
BLOCK_SIZE = 100_000_000

read_options = pv.ReadOptions(block_size=BLOCK_SIZE)
parquet_writer = None

city_coords_df = pl.read_csv(coords_dir)

try:
    for csv_path in input_dir.glob("*.csv"):
        city = csv_path.stem  # Get city name from filename
        
        csv_reader = pv.open_csv(str(csv_path), read_options=read_options)
        
        for record_batch in csv_reader:
            df = pl.from_arrow(record_batch)
            
            # Add city column and process datetime
            df = df.with_columns(
                pl.lit(city).alias("city")
            )
            
            # Select only required columns and ensure proper types
            df = df.select([
                pl.col("city"),
                pl.col("date"),
                pl.col("temperature_2m").cast(pl.Float32),
                pl.col("relative_humidity_2m").cast(pl.Float32),
                pl.col("apparent_temperature").cast(pl.Float32),
                pl.col("precipitation").cast(pl.Float32),
                pl.col("snowfall").cast(pl.Float32),
                pl.col("pressure_msl").cast(pl.Float32),
                pl.col("cloud_cover").cast(pl.Float32),
                pl.col("wind_speed_10m").cast(pl.Float32),
                pl.col("wind_direction_10m").cast(pl.Float32)
            ])

            df = df.join(city_coords_df, on="city", how="left")
            
            table = df.to_arrow()
            
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    parquet_file, 
                    schema=table.schema, 
                    compression="zstd"
                )
                
            parquet_writer.write_table(table)

finally:
    if parquet_writer:
        parquet_writer.close()

print(f"Successfully consolidated all data into {parquet_file}")
