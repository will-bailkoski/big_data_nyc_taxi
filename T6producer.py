import argparse
import glob
import json
import os
import sys
import time
import heapq
from pathlib import Path
from typing import List, Tuple, Iterator, Optional, Dict, Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from confluent_kafka import Producer

# ---------- Fast JSON (optional) ----------
try:
    import orjson
    def dumps(obj) -> bytes:
        return orjson.dumps(
            obj,
            option=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_UTC_Z
        )
except Exception:
    def dumps(obj) -> bytes:
        return json.dumps(obj, default=str).encode("utf-8")


# ---------- Validation (catches "missing footer") ----------
def validate_parquet_glob(pattern: str, tiny_threshold: int = 10_000) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Returns (tiny_files, bad_files). bad_files includes (path, error_message).
    """
    try:
        from pyarrow import parquet as pq
    except Exception:
        print("⚠️ pyarrow.parquet not available; skipping detailed Parquet validation.", file=sys.stderr)
        files = glob.glob(pattern)
        tiny = [f for f in files if os.path.getsize(f) < tiny_threshold]
        return tiny, []

    tiny_files: List[str] = []
    bad_files: List[Tuple[str, str]] = []
    for f in glob.glob(pattern):
        try:
            if os.path.getsize(f) < tiny_threshold:
                tiny_files.append(f)
            _ = pq.ParquetFile(f).metadata  # forces footer read
        except Exception as e:
            bad_files.append((f, str(e)))
    return tiny_files, bad_files


# ---------- Kafka Producer ----------
def make_producer(bootstrap_servers: str, acks: str = "1") -> Producer:
    """
    Builds a Kafka producer tuned for throughput. Adjust as needed.
    """
    config = {
        "bootstrap.servers": bootstrap_servers,
        "compression.type": "lz4",              # or 'zstd'
        "linger.ms": 20,                        # small delay to batch messages
        "batch.num.messages": 10000,
        "queue.buffering.max.kbytes": 104857,   # ~100MB
        "acks": acks,                           # 'all' for stronger durability (slower)
    }
    return Producer(config)


# ---------- Iterators over Parquet (per-source) ----------
def iter_records_from_parquet(
    path_glob: str,
    datetime_col: str,
    source_label: str,
    batch_rows: int,
    keep_columns: Optional[List[str]],
    sort_within_batches: bool,
) -> Iterator[Tuple[pd.Timestamp, Dict[str, Any]]]:
    """
    Stream records from Parquet files as (pickup_datetime, record_dict).
    Uses PyArrow Dataset to read in bounded-size batches.

    If sort_within_batches is True, sorts each batch by pickup_datetime
    before yielding (useful if source files aren't strictly sorted).
    """
    # Ensure we always include the datetime source column
    files = glob.glob(path_glob)
    if not files:
        raise FileNotFoundError(f"No files matched: {path_glob}")

    cols = None
    if keep_columns:
        cols = list(dict.fromkeys([datetime_col] + keep_columns))

    dataset = ds.dataset(files, format="parquet")
    scanner = dataset.scanner(columns=cols, batch_size=batch_rows)

    for batch in scanner.to_batches():
        # Convert to pandas for easy per-row yield & datetime handling
        # (Arrow-native merge is possible, but pandas keeps this simple.)
        tbl = pa.Table.from_batches([batch])
        df = tbl.to_pandas(types_mapper=pd.ArrowDtype)

        # Normalize datetime column to pandas Timestamp (tz-naive OK)
        df["pickup_datetime"] = pd.to_datetime(df[datetime_col], errors="coerce")

        # Keep only requested columns + normalized pickup + source
        if keep_columns:
            # Preserve user-requested order (without duplicates)
            out_cols = ["pickup_datetime"] + [c for c in keep_columns if c != "pickup_datetime"]
            # Filter existing columns only
            out_cols = [c for c in out_cols if c in df.columns or c == "pickup_datetime"]
            df = df[out_cols]
        else:
            # Move pickup_datetime to front
            cols_all = ["pickup_datetime"] + [c for c in df.columns if c not in ("pickup_datetime", datetime_col)]
            df = df[cols_all]

        df["source"] = source_label

        if sort_within_batches:
            df = df.sort_values("pickup_datetime", kind="mergesort")

        # Yield records
        for row in df.itertuples(index=False):
            rec = row._asdict()
            ts = rec.get("pickup_datetime")
            if pd.isna(ts):
                # Skip rows with bad/missing datetime
                continue
            yield ts, rec


# ---------- K-way merge streaming ----------
def stream_kway_merge_to_kafka(
    yellow_glob: str,
    fhvhv_glob: str,
    yellow_topic: str,
    fhvhv_topic: str,
    producer: Producer,
    batch_rows: int = 10_000,
    keep_columns: Optional[List[str]] = None,
    sort_within_batches: bool = False,
    max_msgs_per_sec: float = 0.0,
    progress_every: int = 5_000,
) -> None:
    """
    Merge two source iterators (yellow, fhvhv) by pickup_datetime using heapq.merge,
    and stream to Kafka in near-perfect chronological order without a global sort.
    """
    # Map each source to its own iterator (note different datetime column names)
    gen_y = iter_records_from_parquet(
        path_glob=yellow_glob,
        datetime_col="pickup_datetime",   # yellow schema
        source_label="yellow",
        batch_rows=batch_rows,
        keep_columns=["total_amount","trip_distance","pulocationid","dolocationid","tip_amount"],
        sort_within_batches=sort_within_batches,
    )
    gen_f = iter_records_from_parquet(
        path_glob=fhvhv_glob,
        datetime_col="pickup_datetime",        # fhvhv schema
        source_label="fhvhv",
        batch_rows=batch_rows,
        keep_columns=["driver_pay","trip_miles","pulocationid","dolocationid","tips"],
        sort_within_batches=sort_within_batches,
    )

    sent_total = 0
    t0 = time.time()
    last_report = t0
    throttle = max_msgs_per_sec > 0
    throttle_step = max(100, int(max_msgs_per_sec // 2)) if throttle else 10_000

    for _, record in heapq.merge(gen_y, gen_f, key=lambda x: x[0]):
        topic = yellow_topic if record.get("source") == "yellow" else fhvhv_topic
        #print(topic)
        # Ensure ISO 8601 string for JSON
        ts = record.get("pickup_datetime")
        if isinstance(ts, pd.Timestamp):
            record["pickup_datetime"] = ts.isoformat()
        else:
            # Best-effort stringify
            record["pickup_datetime"] = str(ts)

        producer.produce(topic, value=dumps(record))
        sent_total += 1

        # Service callbacks and keep client queue flowing
        producer.poll(0)

        # Throttle to keep laptop + broker happy
        if throttle and (sent_total % throttle_step == 0):
            elapsed = time.time() - t0
            expected = sent_total / max_msgs_per_sec
            sleep_for = expected - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

        # Progress log & periodic flush
        now = time.time()
        if (sent_total % progress_every == 0) or (now - last_report >= 5.0):
            rate = sent_total / (now - t0 + 1e-9)
            print(f"📤 Sent {sent_total:,} messages  |  ~{rate:,.0f} msg/s")
            producer.flush()
            last_report = now

    # Final flush
    producer.flush()
    elapsed = time.time() - t0
    rate = sent_total / (elapsed + 1e-9)
    print(f"✅ Finished. Sent {sent_total:,} messages in {elapsed:,.1f}s  (~{rate:,.0f} msg/s)")


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(
        description="Stream NYC Taxi Parquet (yellow + fhvhv) to Kafka via k-way merge by pickup_datetime."
    )
    ap.add_argument("--broker", default="localhost:10000", help="Kafka bootstrap servers, e.g. localhost:10000")
    ap.add_argument("--yellow_glob", required=True, help=r"Glob for yellow Parquet files, e.g. C:\...\yellow\year=2022\*.parquet")
    ap.add_argument("--fhvhv_glob", required=True, help=r"Glob for fhvhv Parquet files, e.g. C:\...\fhvhv\year=2022\*.parquet")
    ap.add_argument("--yellow_topic", default="yellow_taxi_stream", help="Kafka topic for yellow messages")
    ap.add_argument("--fhvhv_topic", default="fhvhv_taxi_stream", help="Kafka topic for fhvhv messages")

    # These replace the old DuckDB-specific knobs
    ap.add_argument("--batch_rows", type=int, default=10_000, help="Rows per Arrow batch per source (memory knob)")
    ap.add_argument("--max_msgs_per_sec", type=float, default=0.0, help="Throttle messages/sec (0 = unlimited)")
    ap.add_argument("--sort_within_batches", action="store_true",
                    help="Sort each batch by pickup_datetime (use if source files aren't time-sorted)")
    ap.add_argument("--columns", default="",
                    help="Comma-separated list of columns to include (pickup_datetime is always included). Leave empty for all columns.")
    ap.add_argument("--validate_only", action="store_true", help="Run validation and exit")
    ap.add_argument("--acks", default="1", choices=["0", "1", "all"], help="Kafka producer acks")

    args = ap.parse_args()

    # Expand globs and sanity-check we have files
    yellow_files = glob.glob(args.yellow_glob)
    fhvhv_files = glob.glob(args.fhvhv_glob)

    if not yellow_files:
        print(f"❌ No yellow files matched: {args.yellow_glob}", file=sys.stderr)
        sys.exit(2)
    if not fhvhv_files:
        print(f"❌ No fhvhv files matched: {args.fhvhv_glob}", file=sys.stderr)
        sys.exit(2)

    # Validate Parquet files: catches "Missing footer"
    print("🔎 Validating Parquet files (this checks for truncated/bad files)...")
    tiny_y, bad_y = validate_parquet_glob(args.yellow_glob)
    tiny_f, bad_f = validate_parquet_glob(args.fhvhv_glob)

    def report_issues(name: str, tiny: List[str], bad: List[Tuple[str, str]]):
        if tiny:
            print(f"⚠️  {name}: {len(tiny)} unusually small files (possible partial downloads). Example:")
            for p in tiny[:5]:
                print("    -", p)
        if bad:
            print(f"⛔  {name}: {len(bad)} files failed footer/metadata read. Example:")
            for p, e in bad[:5]:
                print(f"    - {p}  ->  {e}")

    report_issues("YELLOW", tiny_y, bad_y)
    report_issues("FHVHV", tiny_f, bad_f)

    if tiny_y or bad_y or tiny_f or bad_f:
        print("\n❗ Some files look invalid. Re-download or exclude them before streaming.")
        if args.validate_only:
            sys.exit(1)
        sys.exit(1)

    if args.validate_only:
        print("✅ Validation passed. No obvious Parquet footer issues.")
        sys.exit(0)

    # Parse column selection
    keep_columns: Optional[List[str]] = None
    if args.columns.strip():
        # normalize, strip whitespace, drop duplicates while preserving order
        cols = [c.strip() for c in args.columns.split(",") if c.strip()]
        keep_columns = list(dict.fromkeys(cols))
        # We'll always include the normalized 'pickup_datetime' (added in iterator)

    # Create Kafka producer
    producer = make_producer(args.broker, acks=args.acks)

    print("🚀 Starting streaming via k-way merge (near-perfect chronological order)...")
    stream_kway_merge_to_kafka(
        yellow_glob=args.yellow_glob,
        fhvhv_glob=args.fhvhv_glob,
        yellow_topic=args.yellow_topic,
        fhvhv_topic=args.fhvhv_topic,
        producer=producer,
        batch_rows=args.batch_rows,
        keep_columns=keep_columns,
        sort_within_batches=args.sort_within_batches,
        max_msgs_per_sec=args.max_msgs_per_sec,
        progress_every=5_000,
    )


if __name__ == "__main__":
    # On Windows, stdout buffering can chunk prints; force flush for progress logs.
    sys.stdout.reconfigure(line_buffering=True)
    main()
