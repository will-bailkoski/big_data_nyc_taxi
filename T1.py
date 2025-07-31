from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

def get_dataset_meta():
    return [
        # name, valid years, pickup, dropoff
        ("yellow", range(2012, 2026), "tpep_pickup_datetime", "tpep_dropoff_datetime"),
        ("green", range(2016, 2026), "lpep_pickup_datetime", "lpep_dropoff_datetime"),
        ("fhv", range(2016, 2026), "pickup_datetime", "dropoff_datetime"),
        ("fhvhv", range(2019, 2026), "pickup_datetime", "dropoff_datetime")
    ]

def collect_files(source_dir, prefix, valid_years):
    all_files = list(source_dir.glob(f"{prefix}_tripdata_*.parquet"))
    return list(filter(lambda f: any(str(y) in f.name for y in valid_years), all_files))

def standardize_columns(tbl, pickup_key, dropoff_key):
    original = tbl.schema.names
    renamed = []
    for col in original:
        low = col.lower()
        if low == pickup_key:
            renamed.append("pickup_datetime")
        elif low == dropoff_key:
            renamed.append("dropoff_datetime")
        else:
            renamed.append(low)
    return tbl.rename_columns(renamed)

def annotate_by_year(tbl):
    extracted = pa.compute.year(tbl["pickup_datetime"])
    return tbl.append_column("year", extracted)

def writer(records, target_dir, pickup_col, dropoff_col):
    for path in records:
        try:
            table = pq.read_table(path)
            field_map = {name.lower(): name for name in table.schema.names}
            if pickup_col.lower() not in field_map or dropoff_col.lower() not in field_map:
                print(f"⚠️ Skipping {path.name} due to column absence")
                continue

            normalized = standardize_columns(table, pickup_col.lower(), dropoff_col.lower())
            enriched = annotate_by_year(normalized)

            pq.write_to_dataset(
                enriched,
                root_path=target_dir,
                partition_cols=["year"],
                row_group_size=2_000_000,
                existing_data_behavior="overwrite_or_ignore"
            )
            print(f"✅ Wrote: {path.name}")
        except Exception as ex:
            print(f"💥 Error on {path.name}: {ex}")

def run_pipeline():
    base_input = Path("/d/hpc/projects/FRI/bigdata/data/Taxi")
    base_output = Path("/d/hpc/projects/FRI/wb33355/T1/bonus")
    base_output.mkdir(exist_ok=True)

    for name, year_range, pickup_col, dropoff_col in get_dataset_meta():
        print(f"\n🚕 Dataset: {name.upper()}")
        target_folder = base_output / f"{name}_partitioned"
        target_folder.mkdir(exist_ok=True)

        files = collect_files(base_input, name, year_range)
        if not files:
            print(f"⚠️ No files found for {name}")
            continue

        files = files[:5]
        # Pass pickup_col and dropoff_col here explicitly
        writer(files, target_folder, pickup_col, dropoff_col)
if __name__ == "__main__":
    run_pipeline()
