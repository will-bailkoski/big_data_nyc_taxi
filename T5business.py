import os
from pathlib import Path
import geopandas as gpd
import pandas as pd
import dask.dataframe as dd
from shapely.geometry import Point

input_dir = "/d/hpc/projects/FRI/wb33355/T1"
output_dir = "./business_points/data"

business_csv_path = "/d/hpc/projects/FRI/wb33355/T5/Businesses_dataset_20250731.csv"
taxi_zone_shapefile_path = "/d/hpc/projects/FRI/wb33355/T5/taxi_zones/taxi_zones.shp"

glob_patterns = ["*.parquet"]

pd.set_option("display.max_columns", None)

# -- Step 1: Build zone -> [business names] mapping --

gdf_zones = gpd.read_file(taxi_zone_shapefile_path)

# Read businesses CSV and build geometry
df_business = pd.read_csv(business_csv_path)
if not {"Longitude", "Latitude", "Business Name"}.issubset(df_business.columns):
    raise ValueError("Business CSV must contain 'Longitude', 'Latitude', and 'Business Name' columns.")

gdf_business = gpd.GeoDataFrame(
    df_business,
    geometry=gpd.points_from_xy(df_business["Longitude"], df_business["Latitude"]),
    crs="EPSG:4326"
)

# Project to match zones CRS and spatial join
gdf_business = gdf_business.to_crs(gdf_zones.crs)
gdf_business = gpd.sjoin(
    gdf_business,
    gdf_zones[["LocationID", "geometry"]],
    how="left",
    predicate="within"
)

print("Joined businesses sample:")
print(gdf_business[["Business Name", "LocationID"]].head())

try:
    gdf_business.to_file("augmented_businesses.shp")
except Exception as e:
    print(f"(Skipping write of augmented_businesses.shp) {e}")

# Build mapping {LocationID: [business names]}
businesses_by_zone = (
    gdf_business.groupby("LocationID", dropna=True)["Business Name"]
    .apply(list)
    .to_dict()
)
businesses_by_zone = {int(k): v for k, v in businesses_by_zone.items() if pd.notna(k)}

# -- Step 2: Read parquet files --

parquet_globs = [str(Path(input_dir) / "*_partitioned/**" / pat) for pat in glob_patterns]
print("Reading Parquet from patterns:")
for g in parquet_globs:
    print(f"  - {g}")

trips = dd.read_parquet(
    parquet_globs,
    engine="pyarrow",
    gather_statistics=False
)

# -- Step 3: Augment with pickup_businesses / dropoff_businesses --

required_cols = {"pulocationid", "dolocationid"}
missing = [c for c in required_cols if c not in trips.columns]
if missing:
    raise ValueError(f"Missing required columns in trips: {missing}")

def _zone_to_business_csv(s):
    def f(x):
        if pd.isna(x):
            return ""
        try:
            return ", ".join(businesses_by_zone.get(int(x), []))
        except Exception:
            return ""
    return s.map(f)

trips["pickup_businesses"] = trips["pulocationid"].map_partitions(
    _zone_to_business_csv, meta=("pickup_businesses", "string")
)
trips["dropoff_businesses"] = trips["dolocationid"].map_partitions(
    _zone_to_business_csv, meta=("dropoff_businesses", "string")
)

# Optional: cast to string
for col in ["pickup_businesses", "dropoff_businesses"]:
    trips[col] = trips[col].astype("string")

trips = trips.repartition(partition_size="100MB")

# -- Step 4: Write output parquet --

Path(output_dir).mkdir(parents=True, exist_ok=True)
print(f"Writing augmented dataset to: {Path(output_dir).resolve()}")

trips.to_parquet(
    output_dir,
    engine="pyarrow",
    write_index=False,
    overwrite=True
)

print(trips[["pulocationid", "pickup_businesses", "dolocationid", "dropoff_businesses"]].head())
