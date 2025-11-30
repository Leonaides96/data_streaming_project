from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, dayofmonth, col, monotonically_increasing_id
)

# ------------------------------------------------------------
# 1. Initialize Spark
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("NYC Taxi DW Load") \
    .master("local[*]") \
    .getOrCreate()

# ------------------------------------------------------------
# 2. Load NYC Taxi Raw Data
# ------------------------------------------------------------
INPUT = "data/clean/yellow_2023_01_clean.parquet"
df_raw = spark.read.parquet(INPUT)

# ------------------------------------------------------------
# 3. Basic Cleaning / Filtering
# ------------------------------------------------------------
df_clean = df_raw.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") >= 0)
)

# ------------------------------------------------------------
# 4. Create Dimension Tables
# ------------------------------------------------------------

# ---- Dim Vendor ----
dim_vendor = df_clean.select("VendorID").distinct() \
    .withColumnRenamed("VendorID", "vendor_id") \
    .withColumn("vendor_key", monotonically_increasing_id())

# ---- Dim RateCode ----
dim_ratecode = df_clean.select("RatecodeID").distinct() \
    .withColumnRenamed("RatecodeID", "ratecode_id") \
    .withColumn("ratecode_key", monotonically_increasing_id())

# ---- Dim Payment ----
dim_payment = df_clean.select("payment_type").distinct() \
    .withColumn("payment_key", monotonically_increasing_id())

# ---- Dim Date ----
dim_date = (
    df_clean
    .select(
        col("tpep_pickup_datetime").alias("date")
    )
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .distinct()
    .withColumn("date_key", monotonically_increasing_id())
)

# ------------------------------------------------------------
# 5. Create Fact Table
# ------------------------------------------------------------

df_fact = (
    df_clean
    # Join with dim tables to get surrogate keys
    .join(dim_vendor, df_clean.VendorID == dim_vendor.vendor_id)
    .join(dim_ratecode, df_clean.RatecodeID == dim_ratecode.ratecode_id)
    .join(dim_payment, "payment_type")
    .join(dim_date, df_clean.tpep_pickup_datetime == dim_date.date)
    .select(
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amou_
