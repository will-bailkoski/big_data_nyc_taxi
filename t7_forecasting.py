import os
import time
import joblib
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from dask_ml.linear_model import LinearRegression as DaskLR
from xgboost.dask import DaskDMatrix, train as xgb_train
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

# ─────────────────────────────────────────────────────────────────────────────
# 1) Connect to Dask scheduler launched by SLURM script
sched = os.environ.get("SCHEDULER")
if not sched:
    raise RuntimeError("Please set SCHEDULER=host:port in your SLURM script")
client = Client(sched)
print("Dask dashboard:", client.dashboard_link)

# ─────────────────────────────────────────────────────────────────────────────
# 2) Paths to the four year‑partitioned Parquet trees
BASE = "/d/hpc/projects/FRI/wb33355/T1"
services = {
    "Yellow":  f"{BASE}/yellow_partitioned",
    "Green":   f"{BASE}/green_partitioned",
    "FHV":     f"{BASE}/fhv_partitioned",
    "FHVHV":   f"{BASE}/fhvhv_partitioned",
}

# 3) Load & standardize into one Dask DataFrame
ddfs = []
for svc, path in services.items():
    df = dd.read_parquet(path, engine="pyarrow")
    # rename pickup timestamp to common name
    if "tpep_pickup_datetime" in df.columns:
        df = df.rename(columns={"tpep_pickup_datetime": "pickup_datetime"})
    if "pickup_datetime" not in df.columns:
        raise KeyError(f"{svc} missing pickup_datetime")
    df = df[["pickup_datetime", "PULocationID"]].rename(
        columns={"PULocationID": "pickup_zone"}
    )
    df["service_type"] = svc
    ddfs.append(df)

all_trips = dd.concat(ddfs, interleave_partitions=True)
del ddfs

# ─────────────────────────────────────────────────────────────────────────────
# 4) Feature engineering: hourly counts + temporal + spatial lookup
all_trips["pickup_hour"] = all_trips.pickup_datetime.dt.floor("H")
grp = all_trips.groupby(["pickup_hour", "pickup_zone"])
dfh = grp.size().rename("trip_count").reset_index()

# load zone centroids (assumes you have a small CSV locally)
zones = pd.read_csv("taxi_zone_centroids.csv", index_col=0)
dfh = dfh.merge(zones, left_on="pickup_zone", right_index=True, how="left")

# extract simple temporal features
dfh["hour"] = dfh.pickup_hour.dt.hour
dfh["dow"] = dfh.pickup_hour.dt.weekday
dfh["month"] = dfh.pickup_hour.dt.month

# persist before ML
dfh = dfh.persist()

# train/val/test split
dfh["year"] = dfh.pickup_hour.dt.year
train = dfh[dfh.year.isin([2019,2020,2021,2022])]
val   = dfh[dfh.year == 2023]
test  = dfh[dfh.year == 2024]

features = ["hour", "dow", "month", "lat", "lon"]
X_train = train[features]
y_train = train["trip_count"]
X_val   = val[features]
y_val   = val["trip_count"]
X_test  = test[features]
y_test  = test["trip_count"]

# helper to record metrics
results = []

def eval_and_record(name, y_true, y_pred, t_start):
    y_true, y_pred = y_true.compute(), y_pred.compute()
    mse = mean_squared_error(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    t = time.time() - t_start
    results.append({"model": name, "MSE": mse, "MAE": mae, "time_s": t})
    print(f"{name}: MSE={mse:.2f}, MAE={mae:.2f}, time={t:.1f}s")

# ─────────────────────────────────────────────────────────────────────────────
# A) Dask‑ML LinearRegression
t0 = time.time()
dask_lr = DaskLR()
dask_lr.fit(X_train, y_train)
y_pred = dask_lr.predict(X_val)
eval_and_record("DaskLR", y_val, y_pred, t0)

# ─────────────────────────────────────────────────────────────────────────────
# B) XGBoost.dask
t0 = time.time()
dtrain = DaskDMatrix(client, X_train, y_train)
params = {"objective": "reg:squarederror", "tree_method": "hist"}
bst = xgb_train(client, params, dtrain, num_boost_round=100)
y_pred = bst.predict(DaskDMatrix(client, X_val, y_val))
eval_and_record("XGBoost", y_val, y_pred, t0)

# ─────────────────────────────────────────────────────────────────────────────
# C) SGDRegressor.partial_fit
t0 = time.time()
from dask_ml.model_selection import to_iterable
sgd = SGDRegressor(max_iter=1, warm_start=True)
for Xb, yb in to_iterable(X_train, y_train, chunksize=100_000):
    sgd.partial_fit(Xb, yb)
y_pred = sgd.predict(X_val.compute())
eval_and_record("SGD.partial_fit", y_val, y_pred, t0)

# ─────────────────────────────────────────────────────────────────────────────
# 7) Save metrics & models
pd.DataFrame(results).to_csv("t7_results.csv", index=False)
joblib.dump(dask_lr, "model_dasklr.joblib")
bst.save_model("model_xgb.json")
joblib.dump(sgd, "model_sgd.joblib")

print("All done.")
