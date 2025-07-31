import dask.dataframe as dd
import pandas as pd
from glob import glob
import os

# ---- CONFIG ----
parquet_glob_path = "/d/hpc/projects/FRI/wb33355/T1/**/*.parquet"
pickup_col = "pickup_datetime"  # Change to match your dataset
dropoff_col = "dropoff_datetime"
puloc_col = "pulocationid"
doloc_col = "dolocationid"

# Output: {hour: {location_id: count}}
pickup_counts = {}
dropoff_counts = {}

# ---- PROCESS FILES ONE AT A TIME ----

parquet_files = sorted(glob(parquet_glob_path, recursive=True))
print(len(parquet_files))

for path in parquet_files:
    print(f"Processing: {path}")
    df = dd.read_parquet(path, engine="pyarrow", columns=[pickup_col, dropoff_col, puloc_col, doloc_col])

    # Round datetime to the hour
    df["pickup_hour"] = dd.to_datetime(df[pickup_col]).dt.floor("h")
    df["dropoff_hour"] = dd.to_datetime(df[dropoff_col]).dt.floor("h")

    # Group and count pickups
    pickups = df.groupby(["pickup_hour", puloc_col]).size().compute()
    for (hour, loc_id), count in pickups.items():
        pickup_counts.setdefault(hour, {}).setdefault(loc_id, 0)
        pickup_counts[hour][loc_id] += count

    # Group and count dropoffs
    dropoffs = df.groupby(["dropoff_hour", doloc_col]).size().compute()
    for (hour, loc_id), count in dropoffs.items():
        dropoff_counts.setdefault(hour, {}).setdefault(loc_id, 0)
        dropoff_counts[hour][loc_id] += count

# ---- SAVE RESULTS (Optional) ----
# Convert to DataFrame for export
pickup_df = pd.DataFrame([
    {"hour": hour, "location_id": loc_id, "count": count}
    for hour, counts in pickup_counts.items()
    for loc_id, count in counts.items()
])

dropoff_df = pd.DataFrame([
    {"hour": hour, "location_id": loc_id, "count": count}
    for hour, counts in dropoff_counts.items()
    for loc_id, count in counts.items()
])

pickup_df.to_csv("pickup_counts_by_hour.csv", index=False)
dropoff_df.to_csv("dropoff_counts_by_hour.csv", index=False)

print("Done!")