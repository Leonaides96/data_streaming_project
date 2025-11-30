from pyspark.sql import SparkSession
import os

INPUT = "data/clean/yellow_2023_01_clean.parquet"

def main():
    spark = SparkSession.builder.appName("Hybrid-DW").getOrCreate()

    print("=== Step 1: Load cleaned data with Spark ===")
    df_spark = spark.read.parquet(INPUT)
    print("Spark rows:", df_spark.count())

    print("=== Step 2: Convert Spark → Pandas ===")
    df = df_spark.toPandas()
    print("Pandas rows:", len(df))

    print("=== Step 3: Build DW tables using Pandas ===")

    # ---- Daily metrics ----
    df["date"] = df["pickup_datetime"].dt.date

    df_daily = df.groupby("date").agg(
        trip_count=("pickup_datetime", "count"),
        avg_fare=("fare_amount", "mean"),
        total_revenue=("fare_amount", "sum"),
        avg_trip_distance=("trip_distance", "mean"),
    ).reset_index()

    # ---- Hourly metrics ----
    df_hourly = df.groupby("pickup_hour").agg(
        trip_count=("pickup_datetime", "count"),
        avg_fare=("fare_amount", "mean"),
        avg_distance=("trip_distance", "mean")
    ).reset_index()

    # ---- Passenger count distribution ----
    df_passenger = df.groupby("passenger_count").agg(
        trip_count=("pickup_datetime", "count")
    ).reset_index()

    print("=== Step 4: Save DW tables (Parquet) ===")

    os.makedirs("data/warehouse", exist_ok=True)

    df_daily.to_parquet("data/warehouse/dw_daily.parquet", index=False)
    df_hourly.to_parquet("data/warehouse/dw_hourly.parquet", index=False)
    df_passenger.to_parquet("data/warehouse/dw_passenger.parquet", index=False)

    print("DW tables saved:")
    print(" - data/warehouse/dw_daily.parquet")
    print(" - data/warehouse/dw_hourly.parquet")
    print(" - data/warehouse/dw_passenger.parquet")

    print("\nHybrid Data Warehouse complete!")


if __name__ == "__main__":
    main()
