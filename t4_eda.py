#!/usr/bin/env python3
"""
Task 4 EDA: temporal & spatial aggregates, similarity, and visualizations
for all four T1 datasets. Streams with Dask + PyArrow, uses DuckDB for OD,
Pandas+Matplotlib for final charts. Outputs CSVs, PNGs, and a JSON under t4_outputs/.
"""

import os, time, json, glob
import dask.dataframe as dd
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BASE     = "/d/hpc/projects/FRI/wb33355/T1"
OUT_ROOT = "t4_outputs"
os.makedirs(OUT_ROOT, exist_ok=True)

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def timed(fn, *args, **kwargs):
    t0 = time.perf_counter()
    res = fn(*args, **kwargs)
    return res, time.perf_counter() - t0

def save_fig(fig, fname):
    out = os.path.join(OUT_ROOT, fname)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    services = sorted([d for d in os.listdir(BASE) if d.endswith("_partitioned")])
    all_summary = {}
    hourly_norm = {}

    for svc in services:
        svc_path = os.path.join(BASE, svc)
        print(f"\n--- Processing {svc} ---")

        # 1) Load parquet via Dask+PyArrow
        df = dd.read_parquet(
            svc_path,
            engine="pyarrow",
            gather_statistics=False
        )
        # unify to lowercase column names
        df = df.rename(columns={c: c.lower() for c in df.columns})

        # extract time features
        df["pickup_hour"] = df.pickup_datetime.dt.floor("H")
        df["year"]        = df.pickup_hour.dt.year
        df["month"]       = df.pickup_hour.dt.month
        df["hour"]        = df.pickup_hour.dt.hour

        total = df.shape[0].compute()
        print(f"Total rows: {total:,}")
        summary = {"total_trips": int(total)}

        # 2) Trips per year (Dask groupby)
        yr = (
            df.groupby("year")
              .size()
              .compute()
              .reset_index(name="num_trips")
              .sort_values("year")
        )
        yr.to_csv(f"{OUT_ROOT}/{svc}_trips_per_year.csv", index=False)
        summary["trips_per_year"] = yr.to_dict("records")
        fig, ax = plt.subplots()
        yr.plot.bar(x="year", y="num_trips", ax=ax, legend=False)
        ax.set_title(f"{svc}: Trips per Year")
        save_fig(fig, f"{svc}_trips_per_year.png")

        # 3) Trips per hour
        hr = (
            df.groupby("hour")
              .size()
              .compute()
              .reset_index(name="num_trips")
              .sort_values("hour")
        )
        hr.to_csv(f"{OUT_ROOT}/{svc}_trips_per_hour.csv", index=False)
        summary["trips_per_hour"] = hr.to_dict("records")
        fig, ax = plt.subplots()
        hr.plot.line(x="hour", y="num_trips", ax=ax, legend=False)
        ax.set_title(f"{svc}: Trips by Hour")
        save_fig(fig, f"{svc}_trips_per_hour.png")
        hourly_norm[svc] = (hr.set_index("hour")["num_trips"] / hr["num_trips"].sum()).rename(svc)

        # 4) Trips per month
        mm = (
            df.groupby(["year","month"])
              .size()
              .compute()
              .reset_index(name="num_trips")
              .sort_values(["year","month"])
        )
        mm.to_csv(f"{OUT_ROOT}/{svc}_trips_per_month.csv", index=False)
        summary["trips_per_month"] = mm.to_dict("records")

        # 5) Trip‑distance summary (if exists)
        if "trip_distance" in df.columns:
            td = df["trip_distance"].describe().compute().to_dict()
            # ensure JSON‐serializable
            summary["trip_distance_summary"] = {k: float(v) for k,v in td.items()}
        else:
            print(f"  → skipping trip_distance summary (column not present)")
            summary["trip_distance_summary"] = None

        # 6) Top‑10 OD via DuckDB
        pattern = os.path.join(svc_path, "year=*/*.parquet")
        con = duckdb.connect()
        od10 = con.execute(f"""
            SELECT pulocationid, dolocationid, COUNT(*) AS num_trips
              FROM read_parquet('{pattern}')
          GROUP BY pulocationid, dolocationid
          ORDER BY num_trips DESC
          LIMIT 10
        """).df()
        od10.to_csv(f"{OUT_ROOT}/{svc}_top10_od.csv", index=False)
        summary["top10_od_pairs"] = od10.to_dict("records")
        con.close()

        all_summary[svc] = summary

    # 7) Hourly similarity across services
    hr_df = pd.concat(hourly_norm, axis=1).fillna(0)
    corr = hr_df.corr()
    corr.to_csv(f"{OUT_ROOT}/hourly_similarity.csv")
    fig, ax = plt.subplots(figsize=(6,5))
    cax = ax.matshow(corr, vmin=0, vmax=1)
    ax.set_xticks(range(len(corr))); ax.set_yticks(range(len(corr)))
    ax.set_xticklabels(corr.columns, rotation=90)
    ax.set_yticklabels(corr.index)
    fig.colorbar(cax); ax.set_title("Hourly Distribution Correlation")
    save_fig(fig, "hourly_similarity.png")

    # 8) Write JSON summary
    with open(f"{OUT_ROOT}/t4_aggregates.json","w") as jf:
        json.dump(all_summary, jf, indent=2)

    print(f"\nDone: all artifacts in {OUT_ROOT}/")

if __name__ == "__main__":
    main()
