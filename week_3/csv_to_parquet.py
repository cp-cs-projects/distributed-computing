import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl

csv_file = "/Users/srirocks2020/Cal_Poly/csc-369/2022_place_canvas_history.csv"
parquet_file = "./2022_pyarrow.parquet"

DATESTRING_FORMAT = "%Y-%m-%d %H:%M:%S"
BLOCK_SIZE = 100_000_000

read_options = pv.ReadOptions(block_size=BLOCK_SIZE)
csv_reader = pv.open_csv(csv_file, read_options=read_options)
parquet_writer = None
users = {}
curr_id = 1
try:
    for record_batch in csv_reader:

        def id_to_num(uid):
            global curr_id
            if uid not in users:
                users[uid] = curr_id
                curr_id += 1
            return users[uid]

        df = pl.from_arrow(record_batch)

        """
        timestamp: datetime
        user_id: int64
        pixel_color: string
        coordinate: [int64, int64]
        """

        df = df.with_columns(
            pl.col("timestamp")
            .str.replace(r" UTC$", "")
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f", strict=False)
            .alias("timestamp")
        )

        df = df.with_columns(
            pl.col("user_id")
            .map_elements(lambda x : id_to_num(x), return_dtype=pl.Int64)
            .alias("user_id"))

                    
        df = df.filter(pl.col("coordinate").str.count_matches(",") == 1).with_columns(
            pl.concat_list(
                [
                    pl.col("coordinate")
                    .str.split_exact(",", 1)
                    .struct.field("field_0")
                    .cast(pl.Int64),
                    pl.col("coordinate")
                    .str.split_exact(",", 1)
                    .struct.field("field_1")
                    .cast(pl.Int64),
                ]
            ).alias("coordinate")
        )

        table = df.to_arrow()

        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(
                parquet_file, schema=table.schema, compression="zstd"
            )
        parquet_writer.write_table(table)

finally:
    if parquet_writer:
        parquet_writer.close()

print(f"Successfully converted {csv_file} to {parquet_file}")


'''
  df = df.with_columns(
            pl.col("pixel_color")
            .str.replace(r"#", "")
            .str.decode("hex")
            .alias("pixel_color"))

        df = (
            df.filter(
                pl.col("coordinate").str.count_matches(",") == 1
            )
            .with_columns(
                pl.col("coordinate")
                .str.split_exact(",", 1)
                .struct.field("field_0")
                .cast(pl.Int64)
                .alias("x"),
                pl.col("coordinate")
                .str.split_exact(",", 1)
                .struct.field("field_1")
                .cast(pl.Int64)
                .alias("y"),
            )
            .drop("coordinate")
          )
        
'''
