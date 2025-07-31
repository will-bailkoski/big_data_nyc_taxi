import dask.dataframe as dd
import pandas as pd
from glob import glob
from pathlib import Path
import os

# --- CONFIG ---
parquet_glob_path = "/d/hpc/projects/FRI/wb33355/T1/*_partitioned/**/*.parquet"
pickup_col = "pickup_datetime"
weather_csv_path = "/d/hpc/projects/FRI/wb33355/T5/NYC_Central_Park_weather_1869-2022.csv"
output_dir = "/d/hpc/projects/FRI/wb33355/T5/output_with_weather"

# --- STEP 1: Load Trips ---
parquet_globs = sorted(glob(parquet_glob_path, recursive=True))
trips = dd.read_parquet(
    parquet_globs,
    engine="pyarrow",
    gather_statistics=False,
    assume_missing=True
)

# Ensure pickup_col is datetime
trips[pickup_col] = dd.to_datetime(trips[pickup_col], errors="coerce")

# --- STEP 2: Load Weather Data (Pandas) ---
df_weather = pd.read_csv(weather_csv_path, parse_dates=["DATE"])
df_weather = df_weather.dropna(subset=["DATE"])
df_weather["pickup_date"] = df_weather["DATE"].dt.normalize()
df_weather = df_weather.drop(columns=["DATE"])

pd.set_option("display.max_columns", None)
weather_lookup = df_weather.set_index("pickup_date").to_dict(orient="index")


# --- STEP 3: Partition Function to Merge Weather ---
def append_weather_to_partition(df, weather_dict):
    df = df.copy()
    df["pickup_date"] = df["pickup_datetime"].dt.normalize()

    weather_df = df["pickup_date"].map(weather_dict.get).apply(pd.Series)
    weather_df.index = df.index

    df = pd.concat([df, weather_df], axis=1).drop(columns=["pickup_date"])
    return df


# --- STEP 4: Apply Weather Join ---
trips = trips.map_partitions(append_weather_to_partition, weather_lookup)

# --- STEP 5: Save Output as Parquet ---
Path(output_dir).mkdir(parents=True, exist_ok=True)
print(f"Writing augmented dataset to: {Path(output_dir).resolve()}")

trips.to_parquet(
    output_dir,
    engine="pyarrow",
    write_index=False,
    overwrite=True
)

# Small sanity check (triggers minimal compute)
print(trips.head())
