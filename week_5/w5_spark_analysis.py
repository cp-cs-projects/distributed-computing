from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, countDistinct, count, expr, to_date, hour
from time import perf_counter_ns

# Initialize Spark session with a security manager enabled
spark = SparkSession.builder.appName("CanadaAnalysis").getOrCreate()

def percent_users():
    df = spark.read.parquet("../2022xy_pyarrow.parquet")
    
    total_users = df.select(countDistinct("user_id").alias("total_user_count"))
    canada_users = df.filter((col("x").between(176, 243)) & (col("y").between(492, 527))) \
                     .select(countDistinct("user_id").alias("canada_user_count"))
    result = canada_users.crossJoin(total_users).withColumn("percentage", expr("canada_user_count * 100.0 / total_user_count"))
    
    return result.collect()

def top_canada_colors():
    df = spark.read.parquet("../2022xy_pyarrow.parquet")
    result = df.filter((col("x").between(176, 243)) & (col("y").between(492, 527))) \
               .groupBy("pixel_color") \
               .agg(count("*").alias("pixel_count")) \
               .orderBy(col("pixel_count").desc()) \
               .limit(10)
    
    return result.collect()

def top_times():
    df = spark.read.parquet("../2022xy_pyarrow.parquet")
    result = df.filter((col("x").between(176, 243)) & (col("y").between(492, 527))) \
               .groupBy(to_date(col("timestamp")).alias("date"), hour(col("timestamp")).alias("hour")) \
               .agg(countDistinct("user_id").alias("users")) \
               .orderBy(col("users").desc(), col("date"), col("hour"))
    
    return result.collect()

if __name__ == "__main__":
    start_counter = perf_counter_ns()
    print("num users | percent of r/place users:", percent_users())
    print("\ntop colors for Canada:", top_canada_colors())
    print("top times:", top_times())
    end_counter = perf_counter_ns()
    print("\n time elapsed:", end_counter - start_counter, "ns")

    spark.stop()
