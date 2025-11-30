from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, hour
)
import os


def main():
    spark = SparkSession.builder \
        .appName("NYC-Taxi-ETL") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    input_path = "data/raw/yellow_2023_01.parquet"
    output_path = "data/clean/yellow_2023_01_clean.parquet"

    print("Loading dataset...")
    df = spark.read.parquet(input_path)

    print("Cleaning dataset...")

    df_clean = df \
        .withColumn("pickup_datetime", to_timestamp("tpep_pickup_datetime")) \
        .withColumn("dropoff_datetime", to_timestamp("tpep_dropoff_datetime")) \
        .filter(col("passenger_count") > 0) \
        .filter(col("fare_amount") > 0) \
        .filter(col("trip_distance") > 0)

    # Add derived columns (for dashboard)
    df_clean = df_clean \
        .withColumn("pickup_year", year("pickup_datetime")) \
        .withColumn("pickup_month", month("pickup_datetime")) \
        .withColumn("pickup_day", dayofmonth("pickup_datetime")) \
        .withColumn("pickup_hour", hour("pickup_datetime"))

    # Ensure clean folder exists
    os.makedirs("data/clean", exist_ok=True)

    print("Saving cleaned dataset...")
    df_clean.write.mode("overwrite").parquet(output_path)

    print("ETL complete! Cleaned data saved to:")
    print(output_path)


if __name__ == "__main__":
    main()
