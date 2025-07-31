from pathlib import Path
import geopandas as gpd
import pandas as pd
import dask.dataframe as dd
from shapely import wkt

input_dir = "/d/hpc/projects/FRI/wb33355/T1"         # recursively searched for parquet files
output_dir = "/d/hpc/projects/FRI/wb33355/T5/augmented_pois" # parquet dataset directory to write

poi_csv_path = "Points_of_Interest_20250415.csv"
taxi_zone_shapefile_path = "taxi_zones/taxi_zones.shp"

glob_patterns = [
    "*.parquet",
]
# --------------------------------------------------

pd.set_option("display.max_columns", None)

# --- STEP 1: Load & process POIs locally (GeoPandas) ---

# Read taxi zones
gdf_zones = gpd.read_file(taxi_zone_shapefile_path)

# Read POIs CSV (expects WKT in 'the_geom')
df_poi = pd.read_csv(poi_csv_path)

if "the_geom" not in df_poi.columns:
    raise ValueError("POI CSV must contain a 'the_geom' column with WKT geometry.")

# Parse WKT to geometry (EPSG:4326 by spec)
df_poi = df_poi.copy()
df_poi["geometry"] = df_poi["the_geom"].apply(wkt.loads)
gdf_poi = gpd.GeoDataFrame(df_poi, geometry="geometry", crs="EPSG:4326")

# Reproject to match zones
gdf_poi = gdf_poi.to_crs(gdf_zones.crs)

# Filter to relevant facility types if present
relevant_types = {3, 4, 7, 9, 12}
if "FACILITY_T" in gdf_poi.columns:
    gdf_poi = gdf_poi[gdf_poi["FACILITY_T"].isin(relevant_types)]

# Spatial join to get LocationID per POI
gdf_poi = gpd.sjoin(
    gdf_poi,
    gdf_zones[["LocationID", "geometry"]],
    how="left",
    predicate="within"
)

# Normalize name column
if "NAME" in gdf_poi.columns and "Name" not in gdf_poi.columns:
    gdf_poi = gdf_poi.rename(columns={"NAME": "Name"})
elif "Name" not in gdf_poi.columns:
    raise ValueError("POI data must contain a 'NAME' or 'Name' column.")

print("Joined POIs sample:")
print(gdf_poi[["Name", "LocationID"]].head(10))

# Optional: write augmented POIs for inspection
try:
    gdf_poi.to_file("augmented_poi.shp")
except Exception as e:
    print(f"(Skipping write of augmented_poi.shp) {e}")

# Build mapping {LocationID: [POI names]}
pois_by_zone = (
    gdf_poi.groupby("LocationID", dropna=True)["Name"]
    .apply(list)
    .to_dict()
)
# Ensure integer keys
pois_by_zone = {int(k): v for k, v in pois_by_zone.items() if pd.notna(k)}

# --- STEP 2: Read ALL matching Parquet files with Dask (distributed by your environment) ---

parquet_globs = [str(Path(input_dir) / "*_partitioned/**" / pat) for pat in glob_patterns]
print("Reading Parquet from patterns:")
for g in parquet_globs:
    print(f"  - {g}")

trips = dd.read_parquet(
    parquet_globs,
    engine="pyarrow",
    gather_statistics=False  # faster startup when many small files
)

# --- STEP 3: Augment with pickup_pois / dropoff_pois ---

# Ensure required columns exist
required_cols = {"pulocationid", "dolocationid"}
missing = [c for c in required_cols if c not in trips.columns]
if missing:
    raise ValueError(f"Missing required columns in trips: {missing}")

def _zone_to_poi_csv(s):
    # s is a pandas Series (per partition)
    def f(x):
        if pd.isna(x):
            return ""
        try:
            return ", ".join(pois_by_zone.get(int(x), []))
        except Exception:
            return ""
    return s.map(f)

trips["pickup_pois"] = trips["pulocationid"].map_partitions(
    _zone_to_poi_csv, meta=("pickup_pois", "string")
)
trips["dropoff_pois"] = trips["dolocationid"].map_partitions(
    _zone_to_poi_csv, meta=("dropoff_pois", "string")
)

# Optional: cast to consistent string dtypes (helps Arrow unification across datasets)
for col in ["congestion_surcharge", "airport_fee", "pickup_pois", "dropoff_pois"]:
    if col not in trips.columns:
        trips[col] = ""
    trips[col] = trips[col].astype("string")

# Optional: tune partition size for memory/perf when writing
trips = trips.repartition(partition_size="100MB")

# --- STEP 4: Write Parquet dataset (distributed write) ---

Path(output_dir).mkdir(parents=True, exist_ok=True)
print(f"Writing augmented dataset to: {Path(output_dir).resolve()}")

trips.to_parquet(
    output_dir,
    engine="pyarrow",
    write_index=False,
    overwrite=True
)

# Small sanity check (triggers minimal compute)
print(trips[["pulocationid", "pickup_pois", "dolocationid", "dropoff_pois"]].head())
