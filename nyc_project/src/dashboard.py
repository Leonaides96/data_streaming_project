import streamlit as st
import pandas as pd

st.title("NYC Taxi Hybrid Dashboard")

df_daily = pd.read_parquet("data/warehouse/dw_daily.parquet")
df_hourly = pd.read_parquet("data/warehouse/dw_hourly.parquet")
df_passenger = pd.read_parquet("data/warehouse/dw_passenger.parquet")

st.header("📅 Daily Trips")
st.line_chart(df_daily.set_index("date")["trip_count"])

st.header("💰 Daily Revenue")
st.line_chart(df_daily.set_index("date")["total_revenue"])

st.header("⏰ Trips by Hour")
st.bar_chart(df_hourly.set_index("pickup_hour")["trip_count"])

st.header("👥 Passenger Count Distribution")
st.bar_chart(df_passenger.set_index("passenger_count")["trip_count"])

st.info("Hybrid Engine: Ingest = Spark | Transform = Pandas | DW = Pandas → Parquet")
