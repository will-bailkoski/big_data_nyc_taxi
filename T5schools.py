import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import dask.dataframe as dd
from shapely.geometry import Point

input_dir = "/d/hpc/projects/FRI/wb33355/T1"      # directory containing parquet files (searched recursively)
output_dir = "./school_points/data"   # directory to write the augmented parquet dataset

school_shapefile_path = "/d/hpc/projects/FRI/wb33355/T5/school_points/SchoolPoints_APS_2024_08_28.shp"
taxi_zone_shapefile_path = "/d/hpc/projects/FRI/wb33355/T5/taxi_zones/taxi_zones.shp"

glob_patterns = ["**/*.parquet"]
# ----------------------------

pd.set_option("display.max_columns", None)

# -- Step 1: Build zone -> [school names] mapping (local GeoPandas) --

# Read zones
gdf_zones = gpd.read_file(taxi_zone_shapefile_path)

# Read schools and build geometry from lon/lat
gdf_schools = gpd.read_file(school_shapefile_path).copy()
if not {"Longitude", "Latitude", "Name"}.issubset(gdf_schools.columns):
    raise ValueError("School shapefile must contain 'Longitude', 'Latitude', and 'Name' columns.")

gdf_schools["geometry"] = gpd.points_from_xy(
    gdf_schools["Longitude"],
    gdf_schools["Latitude"],
    crs="EPSG:4326"
)

# Match CRS with zones and spatial join (which zone contains each school)
gdf_schools = gdf_schools.to_crs(gdf_zones.crs)
gdf_schools = gpd.sjoin(
    gdf_schools,
    gdf_zones[["LocationID", "geometry"]],
    how="left",
    predicate="within"
)

print("Joined schools sample:")
print(gdf_schools[["Name", "LocationID"]].head())

# Optional: write augmented schools for inspection
try:
    gdf_schools.to_file("augmented_schools.shp")
except Exception as e:
    print(f"(Skipping write of augmented_schools.shp) {e}")

# Build mapping {LocationID: [school names]}
schools_by_zone = (
    gdf_schools.groupby("LocationID", dropna=True)["Name"]
    .apply(list)
    .to_dict()
)
# Ensure integer keys
schools_by_zone = {int(k): v for k, v in schools_by_zone.items() if pd.notna(k)}


# -- Step 2: Read ALL matching Parquet files with Dask (distributed by your environment) --

# Build recursive glob(s)
parquet_globs = [str(Path(input_dir) / "*_partitioned" / pat) for pat in glob_patterns]
print("Reading Parquet from patterns:")
for g in parquet_globs:
    print(f"  - {g}")


trips = dd.read_parquet(
    parquet_globs,
    engine="pyarrow",
    gather_statistics=False  # faster startup when many files
)

# (No datetime harmonization/renames; assumed already consistent, as requested.)

# -- Step 3: Augment with pickup_schools / dropoff_schools --

# sanity: ensure the required columns exist
required_cols = {"pulocationid", "dolocationid"}
missing = [c for c in required_cols if c not in trips.columns]
if missing:
    raise ValueError(f"Missing required columns in trips: {missing}")

def _zone_to_school_csv(s):
    # s is a pandas Series within each partition
    def f(x):
        if pd.isna(x):
            return ""
        # cast to int if possible; leave empty if not found
        try:
            return ", ".join(schools_by_zone.get(int(x), []))
        except Exception:
            return ""
    return s.map(f)

trips["pickup_schools"] = trips["pulocationid"].map_partitions(
    _zone_to_school_csv, meta=("pickup_schools", "string")
)
trips["dropoff_schools"] = trips["dolocationid"].map_partitions(
    _zone_to_school_csv, meta=("dropoff_schools", "string")
)

# Cast a few known string-like columns consistently (optional, helps Arrow unification)
for col in ["congestion_surcharge", "airport_fee", "pickup_schools", "dropoff_schools"]:
    if col not in trips.columns:
        trips[col] = ""
    trips[col] = trips[col].astype("string")

# Optional: tune partition size for write/memory behavior
trips = trips.repartition(partition_size="100MB")

# -- Step 4: Write out Parquet dataset (distributed write) --

Path(output_dir).mkdir(parents=True, exist_ok=True)
print(f"Writing augmented dataset to: {Path(output_dir).resolve()}")

trips.to_parquet(
    output_dir,
    engine="pyarrow",
    write_index=False,
    overwrite=True
)

# Small sanity check (triggers minimal compute)
print(trips[["pulocationid", "pickup_schools", "dolocationid", "dropoff_schools"]].head())
