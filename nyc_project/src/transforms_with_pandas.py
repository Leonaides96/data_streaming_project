from pyspark.sql import SparkSession
import pandas as pd
import os

RAW_PATH = "data/raw/yellow_2023_01.parquet"
CLEAN_PATH = "data/clean/yellow_2023_01_clean.parquet"

def main():
    # initialized the spark session
    spark = SparkSession.builder.appName("Hybrid-ETL").getOrCreate()

    print("=== Step 1: Load raw data with Spark ===")
    df_spark = spark.read.parquet(RAW_PATH)

    print(f"Spark loaded rows: {df_spark.count()}")

    print("=== Step 2: Convert Spark → Pandas ===")
    df_pandas = df_spark.toPandas()
    print(f"Pandas loaded rows: {len(df_pandas)}")

    print("=== Step 3: Transform using Pandas ===")

    df_pandas["pickup_datetime"] = pd.to_datetime(df_pandas["tpep_pickup_datetime"], errors="coerce")
    df_pandas["dropoff_datetime"] = pd.to_datetime(df_pandas["tpep_dropoff_datetime"], errors="coerce")

    df_pandas = df_pandas[
        (df_pandas["passenger_count"] > 0) &
        (df_pandas["fare_amount"] > 0) &
        (df_pandas["trip_distance"] > 0)
    ].dropna(subset=["pickup_datetime", "dropoff_datetime"])

    # Derived columns
    df_pandas["pickup_year"] = df_pandas["pickup_datetime"].dt.year
    df_pandas["pickup_month"] = df_pandas["pickup_datetime"].dt.month
    df_pandas["pickup_day"] = df_pandas["pickup_datetime"].dt.day
    df_pandas["pickup_hour"] = df_pandas["pickup_datetime"].dt.hour

    print("=== Step 4: Convert Pandas → Spark ===")
    df_spark_clean = spark.createDataFrame(df_pandas)

    print("Spark clean dataframe rows:", df_spark_clean.count())

    print("=== Step 5: Save cleaned dataset with Spark ===")
    os.makedirs("data/clean", exist_ok=True)
    df_spark_clean.write.mode("overwrite").parquet(CLEAN_PATH)

    print("Cleaned dataset saved:", CLEAN_PATH)


if __name__ == "__main__":
    main()
