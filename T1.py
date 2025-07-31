import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Base directories
base_input = Path("/d/hpc/projects/FRI/bigdata/data/Taxi")
base_output = Path("/d/hpc/projects/FRI/wb33355/T1")
base_output.mkdir(parents=True, exist_ok=True)

# Settings per dataset
settings = [
    ("yellow", range(2012, 2026), "tpep_pickup_datetime", "tpep_dropoff_datetime"),
    ("green", range(2016, 2026), "lpep_pickup_datetime", "lpep_dropoff_datetime"),
    ("fhv", range(2016, 2026), "pickup_datetime", "dropoff_datetime"),
    ("fhvhv", range(2019, 2026), "pickup_datetime", "dropoff_datetime"),
]

# Process each dataset type
for name, years, pickup_col_raw, dropoff_col_raw in settings:
    print(f"\n=== Handling {name.upper()} dataset ===")
    subfolder = base_output / f"{name}_partitioned"
    subfolder.mkdir(exist_ok=True)

    for item in sorted(base_input.iterdir()):
        fname = item.name
        if not (fname.startswith(f"{name}_tripdata_") and fname.endswith(".parquet")):
            continue
        if not any(str(y) in fname for y in years):
            continue

        print(f"[{name.upper()}] Reading {fname}")
        try:
            data = pq.read_table(item)
            original_fields = data.schema.names
            columns_lower = {c.lower(): c for c in original_fields}

            # Abort if required fields are not present
            if pickup_col_raw.lower() not in columns_lower or dropoff_col_raw.lower() not in columns_lower:
                print(f"⚠️ Skipping {fname} due to missing required columns.")
                continue

            # Map and rename
            updated_columns = []
            for col in original_fields:
                key = col.lower()
                if key == pickup_col_raw.lower():
                    updated_columns.append("pickup_datetime")
                elif key == dropoff_col_raw.lower():
                    updated_columns.append("dropoff_datetime")
                else:
                    updated_columns.append(key)
            data = data.rename_columns(updated_columns)

            # Extract and append year column
            pickup_dt = data["pickup_datetime"]
            year_info = pa.compute.year(pickup_dt)
            data = data.append_column("year", year_info)

            # Save partitioned by year
            pq.write_to_dataset(
                data,
                root_path=subfolder,
                partition_cols=["year"],
                row_group_size=2_000_000,
                existing_data_behavior="overwrite_or_ignore"
            )
            print(f"[{name.upper()}] Finished and saved")
        except Exception as err:
            print(f"[{name.upper()}] ⚠️ Failed on {fname}: {err}")
