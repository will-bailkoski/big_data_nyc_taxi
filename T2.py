import dask.dataframe as dd
import numpy as np
import os
from glob import glob
import matplotlib.pyplot as plt
from tqdm import tqdm

# === Configuration ===
DATASETS = ["green", "fhv", "fhvhv", "yellow"]
PARQUET_ROOT = "/d/hpc/projects/FRI/wb33355/T1"
OUTPUT_ROOT = "/d/hpc/projects/FRI/wb33355/T2"
RESULTS_ROOT = "/d/hpc/projects/FRI/wb33355/T2/cleaning_results"
os.makedirs(RESULTS_ROOT, exist_ok=True)


def plot_stats(df, dataset_name):
    df.plot(
        x='year',
        y=['invalid_timestamps', 'invalid_distances', 'invalid_prices'],
        kind='bar',
        stacked=True,
        figsize=(10, 6)
    )
    plt.title(f"{dataset_name.capitalize()} Trip Data Quality Issues by Year")
    plt.ylabel("Trip Count")
    plt.xlabel("Year")
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_ROOT, f"{dataset_name}_quality_issues_chart.png"))
    plt.close()


for dataset in DATASETS:
    print(f"\n=== Processing dataset: {dataset} ===")
    parquet_dir = os.path.join(PARQUET_ROOT, f"{dataset}_partitioned")
    parquet_files = sorted(glob(os.path.join(parquet_dir, "**", "*.parquet")))

    ddfs = [dd.read_parquet(pf) for pf in parquet_files]
    print(len(ddfs), "dataframes read")

    dfs = []
    stats_ddfs = []

    for i in tqdm(range(len(ddfs))):
        ddf = ddfs[i]

        print(ddf.columns)

        numeric_cols = [
        'fare_amount', 'mta_tax', 'improvement_surcharge',
        'tolls_amount', 'congestion_surcharge', 'airport_fee',
        'extra', 'tip_amount', 'total_amount'
    	]
        for col in numeric_cols:
            if col in ddf.columns:
                ddf[col] = dd.to_numeric(ddf[col], errors='coerce')

        # === Validation Rules ===
        pickup = ddf['pickup_datetime']
        dropoff = ddf['dropoff_datetime']
        pickup_year = pickup.dt.year
        dropoff_year = dropoff.dt.year
        year_gap = dropoff_year - pickup_year

        valid_time_order = pickup < dropoff
        same_year = year_gap == 0
        crosses_new_year = (
            (year_gap == 1) &
            (pickup.dt.month == 12) & (pickup.dt.day == 31) &
            (dropoff.dt.month == 1) & (dropoff.dt.day == 1)
        )
        # valid_year = ddf['year'].between(2011, 2025)
        valid_year = ddf['pickup_datetime'].dt.year.between(2011, 2025)
        time_valid = valid_time_order & (same_year | crosses_new_year) & valid_year

        duration_hours = (dropoff - pickup).dt.total_seconds() / 3600
        max_distance = duration_hours * 75
        distance_valid = ddf['trip_distance'].between(0, max_distance, inclusive='neither')

        total_charges = (
            ddf['fare_amount'] + ddf['mta_tax'] + ddf['improvement_surcharge'] +
            ddf['tolls_amount'] + ddf['congestion_surcharge'] + ddf['airport_fee']
        )
        positive_components = (
            (ddf['extra'] >= 0) & (ddf['mta_tax'] >= 0) & (ddf['tip_amount'] >= 0) &
            (ddf['tolls_amount'] >= 0) & (ddf['improvement_surcharge'] >= 0) &
            (ddf['congestion_surcharge'] >= 0) & (ddf['airport_fee'] >= 0) &
            (ddf['total_amount'] > 0)
        )
        price_valid = positive_components & total_charges.between(0, ddf['total_amount'], inclusive='right')

        # === Flag invalids ===
        invalid_time = ~time_valid
        invalid_distance = ~distance_valid
        invalid_price = ~price_valid
        is_clean = ~(invalid_time | invalid_distance | invalid_price)

        summary = ddf[['year']].copy()
        summary['invalid_time'] = invalid_time
        summary['invalid_distance'] = invalid_distance
        summary['invalid_price'] = invalid_price
        summary['clean'] = is_clean

        stats_ddfs.append(
            summary.groupby('year').agg(
                total_trips=("year", "count"),
                invalid_timestamps=("invalid_time", "sum"),
                invalid_distances=("invalid_distance", "sum"),
                invalid_prices=("invalid_price", "sum"),
                valid_trip_count=("clean", "sum"),
            )
        )

        clean_mask = time_valid & distance_valid & price_valid
        dfs.append(ddf.loc[clean_mask].reset_index(drop=True))

    # === Combine and Save Statistics ===
    print("Computing yearly stats...")
    final_stats_ddf = dd.concat(stats_ddfs).groupby("year").sum()
    final_stats = final_stats_ddf.compute().reset_index().sort_values("year")

    final_stats.to_csv(os.path.join(RESULTS_ROOT, f"{dataset}_tripdata_quality_stats.csv"), index=False)
    plot_stats(final_stats, dataset)

    # === Save Cleaned Parquet Data ===
    print("Saving cleaned data...")
    clean_ddf = dd.concat(dfs)
    dataset_clean_dir = os.path.join(OUTPUT_ROOT, f"{dataset}_cleaned")
    os.makedirs(dataset_clean_dir, exist_ok=True)
    clean_ddf.to_parquet(
        dataset_clean_dir,
        engine='pyarrow',
        partition_on=['year'],
        row_group_size=2_000_000,
        overwrite="overwrite_or_ignore"
    )
