import os
import time
import duckdb
import pandas as pd
import dask.dataframe as dd

# === Setup ===
GREEN_PARQUET_PATH = "/d/hpc/projects/FRI/wb33355/T1/green_partitioned/year=2024"
EXPORT_DIR = "./format_exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

year = 2024
file_prefix = os.path.join(EXPORT_DIR, f"green_{year}")

# === Load 2024 partition ===
print("Reading Green Taxi 2024 partition with Dask...")
ddf = dd.read_parquet(GREEN_PARQUET_PATH)
df = ddf.compute()
print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")

# === Export to different formats ===
print("Exporting to CSV...")
csv_path = f"{file_prefix}.csv"
df.to_csv(csv_path, index=False)

print("Exporting to Gzipped CSV...")
csv_gz_path = f"{file_prefix}.csv.gz"
df.to_csv(csv_gz_path, index=False, compression='gzip')

print("Exporting to HDF5...")
hdf_path = f"{file_prefix}.h5"
df_for_hdf = df.copy()
string_cols = df_for_hdf.select_dtypes(include=['string']).columns
df_for_hdf[string_cols] = df_for_hdf[string_cols].astype('object')
df_for_hdf.to_hdf(hdf_path, key='green2024', mode='w', format='table')

print("Exporting to DuckDB...")
duckdb_path = f"{file_prefix}.duckdb"
con = duckdb.connect(duckdb_path)
con.execute("CREATE OR REPLACE TABLE green2024 AS SELECT * FROM df")
con.close()

# === Compare read speeds and sizes ===
formats = {
    "CSV": csv_path,
    "CSV (gz)": csv_gz_path,
    "HDF5": hdf_path,
    "DuckDB": duckdb_path
}

print("\n=== Size & Load Time Comparison ===")
results = []
for fmt, path in formats.items():
    size_mb = os.path.getsize(path) / (1024 * 1024)

    if fmt == "HDF5":
        start = time.time()
        _ = pd.read_hdf(path, key='green2024')
        elapsed = time.time() - start

    elif fmt == "DuckDB":
        start = time.time()
        con = duckdb.connect(path)
        _ = con.execute("SELECT * FROM green2024").fetchdf()
        con.close()
        elapsed = time.time() - start

    else:
        start = time.time()
        _ = pd.read_csv(path)
        elapsed = time.time() - start

    results.append((fmt, f"{size_mb:.2f} MB", f"{elapsed:.3f} sec"))

# Print summary
print(f"{'Format':<12} {'Size':>12} {'Read Time':>12}")
for row in results:
    print(f"{row[0]:<12} {row[1]:>12} {row[2]:>12}")
