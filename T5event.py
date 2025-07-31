import os
from pathlib import Path
import geopandas as gpd
import pandas as pd
import dask.dataframe as dd
import dask
from shapely.geometry import Point

input_dir = "/d/hpc/projects/FRI/wb33355/T1"
output_dir = "./event_points/data"

event_info_path = "/d/hpc/projects/FRI/wb33355/T5/NYC_Parks_Events_Listing___Event_Listing_20250415.csv"
event_location_path = "/d/hpc/projects/FRI/wb33355/T5/NYC_Parks_Events_Listing___Event_Locations (2).csv"
taxi_zone_shapefile_path = "/d/hpc/projects/FRI/wb33355/T5/taxi_zones/taxi_zones.shp"

glob_patterns = ["*.parquet"]

# Read taxi zones
gdf_zones = gpd.read_file(taxi_zone_shapefile_path)

# -- Step 1: Load & combine event data --
df_info = pd.read_csv(event_info_path, parse_dates=["start_time", "end_time"])
df_loc = pd.read_csv(event_location_path)

df_event = pd.merge(df_info, df_loc, on="event_id", how="inner")
gdf_event = gpd.GeoDataFrame(
    df_event,
    geometry=gpd.points_from_xy(df_event["long"], df_event["lat"]),
    crs="EPSG:4326"
)
gdf_event = gdf_event.to_crs(gdf_zones.crs)

# -- Step 2: Spatially join with zones --
gdf_event = gpd.sjoin(
    gdf_event,
    gdf_zones[["LocationID", "geometry"]],
    how="left",
    predicate="within"
)

# Optional debug
print("Sample joined events:")
print(gdf_event[["title", "start_time", "end_time", "LocationID"]].head())

# Save joined events for inspection
try:
    gdf_event.to_file("augmented_events.shp")
except Exception as e:
    print(f"(Skipping write of augmented_events.shp) {e}")

# -- Step 3: Read parquet with Dask --
parquet_globs = [str(Path(input_dir) / "*_partitioned/**" / pat) for pat in glob_patterns]
print("Reading Parquet from patterns:")
for g in parquet_globs:
    print(f"  - {g}")

trips = dd.read_parquet(
    parquet_globs,
    engine="pyarrow",
    gather_statistics=False
)

# -- Step 4: Parse datetime columns if needed --
datetime_cols = ["pickup_datetime", "dropoff_datetime"]
for col in datetime_cols:
    if col in trips.columns:
        trips[col] = dd.to_datetime(trips[col], errors="coerce")

# -- Step 5: Map LocationID + time proximity to event names --

# Convert event data to pandas dictionary
event_records = gdf_event.dropna(subset=["LocationID"]).to_dict("records")

def _build_event_matcher(events):
    from datetime import timedelta

    def match_events(zone_id, dt):
        if pd.isna(zone_id) or pd.isna(dt):
            return ""
        zone_id = int(zone_id)
        matched = [
            e["title"]
            for e in events
            if int(e["LocationID"]) == zone_id
            and e["start_time"] - timedelta(hours=1) <= dt <= e["end_time"] + timedelta(hours=1)
        ]
        return ", ".join(matched)

    return match_events

# Use Dask map_partitions to apply the matcher function
pickup_matcher = _build_event_matcher(event_records)
dropoff_matcher = _build_event_matcher(event_records)

trips["pickup_events"] = trips.map_partitions(
    lambda df: df.apply(lambda row: pickup_matcher(row["pulocationid"], row["pickup_datetime"]), axis=1),
    meta=("pickup_events", "string")
)
trips["dropoff_events"] = trips.map_partitions(
    lambda df: df.apply(lambda row: dropoff_matcher(row["dolocationid"], row["dropoff_datetime"]), axis=1),
    meta=("dropoff_events", "string")
)

# Optional: cast output columns to string type
trips["pickup_events"] = trips["pickup_events"].astype("string")
trips["dropoff_events"] = trips["dropoff_events"].astype("string")

# Repartition for efficiency
trips = trips.repartition(partition_size="100MB")

# -- Step 6: Write output parquet --
Path(output_dir).mkdir(parents=True, exist_ok=True)
print(f"Writing augmented dataset to: {Path(output_dir).resolve()}")

trips.to_parquet(
    output_dir,
    engine="pyarrow",
    write_index=False,
    overwrite=True
)

# Final sanity check
print(trips[["pickup_datetime", "pickup_events", "dropoff_datetime", "dropoff_events"]].head())
