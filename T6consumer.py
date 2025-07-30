zone_ids_to_borough = {
    1: "EWR", 2: "Queens", 3: "Bronx", 4: "Manhattan", 5: "Staten Island",
    6: "Staten Island", 7: "Queens", 8: "Queens", 9: "Queens", 10: "Queens",
    11: "Brooklyn", 12: "Manhattan", 13: "Manhattan", 14: "Brooklyn", 15: "Queens",
    16: "Queens", 17: "Brooklyn", 18: "Bronx", 19: "Queens", 20: "Bronx",
    21: "Brooklyn", 22: "Brooklyn", 23: "Staten Island", 24: "Manhattan", 25: "Brooklyn",
    26: "Brooklyn", 27: "Queens", 28: "Queens", 29: "Brooklyn", 30: "Queens",
    31: "Bronx", 32: "Bronx", 33: "Brooklyn", 34: "Brooklyn", 35: "Brooklyn",
    36: "Brooklyn", 37: "Brooklyn", 38: "Queens", 39: "Brooklyn", 40: "Brooklyn",
    41: "Manhattan", 42: "Manhattan", 43: "Manhattan", 44: "Staten Island", 45: "Manhattan",
    46: "Bronx", 47: "Bronx", 48: "Manhattan", 49: "Brooklyn", 50: "Manhattan",
    51: "Bronx", 52: "Brooklyn", 53: "Queens", 54: "Brooklyn", 55: "Brooklyn",
    56: "Queens", 57: "Queens", 58: "Bronx", 59: "Bronx", 60: "Bronx",
    61: "Brooklyn", 62: "Brooklyn", 63: "Brooklyn", 64: "Queens", 65: "Brooklyn",
    66: "Brooklyn", 67: "Brooklyn", 68: "Manhattan", 69: "Bronx", 70: "Queens",
    71: "Brooklyn", 72: "Brooklyn", 73: "Queens", 74: "Manhattan", 75: "Manhattan",
    76: "Brooklyn", 77: "Brooklyn", 78: "Bronx", 79: "Manhattan", 80: "Brooklyn",
    81: "Bronx", 82: "Queens", 83: "Queens", 84: "Staten Island", 85: "Brooklyn",
    86: "Queens", 87: "Manhattan", 88: "Manhattan", 89: "Brooklyn", 90: "Manhattan",
    91: "Brooklyn", 92: "Queens", 93: "Queens", 94: "Bronx", 95: "Queens",
    96: "Queens", 97: "Brooklyn", 98: "Queens", 99: "Staten Island", 100: "Manhattan",
    101: "Queens", 102: "Queens", 103: "Manhattan", 104: "Manhattan", 105: "Manhattan",
    106: "Brooklyn", 107: "Manhattan", 108: "Brooklyn", 109: "Staten Island", 110: "Staten Island",
    111: "Brooklyn", 112: "Brooklyn", 113: "Manhattan", 114: "Manhattan", 115: "Staten Island",
    116: "Manhattan", 117: "Queens", 118: "Staten Island", 119: "Bronx", 120: "Manhattan",
    121: "Queens", 122: "Queens", 123: "Brooklyn", 124: "Queens", 125: "Manhattan",
    126: "Bronx", 127: "Manhattan", 128: "Manhattan", 129: "Queens", 130: "Queens",
    131: "Queens", 132: "Queens", 133: "Brooklyn", 134: "Queens", 135: "Queens",
    136: "Bronx", 137: "Manhattan", 138: "Queens", 139: "Queens", 140: "Manhattan",
    141: "Manhattan", 142: "Manhattan", 143: "Manhattan", 144: "Manhattan", 145: "Queens",
    146: "Queens", 147: "Bronx", 148: "Manhattan", 149: "Brooklyn", 150: "Brooklyn",
    151: "Manhattan", 152: "Manhattan", 153: "Manhattan", 154: "Brooklyn", 155: "Brooklyn",
    156: "Staten Island", 157: "Queens", 158: "Manhattan", 159: "Bronx", 160: "Queens",
    161: "Manhattan", 162: "Manhattan", 163: "Manhattan", 164: "Manhattan", 165: "Brooklyn",
    166: "Manhattan", 167: "Bronx", 168: "Bronx", 169: "Bronx", 170: "Manhattan",
    171: "Queens", 172: "Staten Island", 173: "Queens", 174: "Bronx", 175: "Queens",
    176: "Staten Island", 177: "Brooklyn", 178: "Brooklyn", 179: "Queens", 180: "Queens",
    181: "Brooklyn", 182: "Bronx", 183: "Bronx", 184: "Bronx", 185: "Bronx",
    186: "Manhattan", 187: "Staten Island", 188: "Brooklyn", 189: "Brooklyn", 190: "Brooklyn",
    191: "Queens", 192: "Queens", 193: "Queens", 194: "Manhattan", 195: "Brooklyn",
    196: "Queens", 197: "Queens", 198: "Queens", 199: "Bronx", 200: "Bronx",
    201: "Queens", 202: "Manhattan", 203: "Queens", 204: "Staten Island", 205: "Queens",
    206: "Staten Island", 207: "Queens", 208: "Bronx", 209: "Manhattan", 210: "Brooklyn",
    211: "Manhattan", 212: "Bronx", 213: "Bronx", 214: "Staten Island", 215: "Queens",
    216: "Queens", 217: "Brooklyn", 218: "Queens", 219: "Queens", 220: "Bronx",
    221: "Staten Island", 222: "Brooklyn", 223: "Queens", 224: "Manhattan", 225: "Brooklyn",
    226: "Queens", 227: "Brooklyn", 228: "Brooklyn", 229: "Manhattan", 230: "Manhattan",
    231: "Manhattan", 232: "Manhattan", 233: "Manhattan", 234: "Manhattan", 235: "Bronx",
    236: "Manhattan", 237: "Manhattan", 238: "Manhattan", 239: "Manhattan", 240: "Bronx",
    241: "Bronx", 242: "Bronx", 243: "Manhattan", 244: "Manhattan", 245: "Staten Island",
    246: "Manhattan", 247: "Bronx", 248: "Bronx", 249: "Manhattan", 250: "Bronx",
    251: "Staten Island", 252: "Queens", 253: "Queens", 254: "Bronx", 255: "Brooklyn",
    256: "Brooklyn", 257: "Brooklyn", 258: "Queens", 259: "Bronx", 260: "Queens",
    261: "Manhattan", 262: "Manhattan", 263: "Manhattan", 264: "Unknown", 265: "N/A"
}

import json
import math
import signal
import sys
from collections import defaultdict, Counter
from datetime import datetime

from confluent_kafka import Consumer

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION (adjust as needed)
# ─────────────────────────────────────────────────────────────────────────────

BROKER = "localhost:10000"
TOPIC = "yellow_taxi_stream"
GROUP_ID = "rolling_stats_group"

PRINT_EVERY = 500       # print summary every N messages
TOP_K = 10              # top/bottom K zones
VISITS = "pickup"       # "pickup" | "both"  (zone visit definition)
TOTAL_FIELD = "total_amount"  # field name for "total pay" in your stream


# ─────────────────────────────────────────────────────────────────────────────
# STATS UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

class RollingStats:
    """Online mean/std using Welford; tracks count, min, max too."""
    __slots__ = ("count", "mean", "M2", "min", "max")

    def __init__(self):
        self.count = 0
        self.mean = 0.0
        self.M2 = 0.0
        self.min = float("inf")
        self.max = float("-inf")

    def update(self, x: float):
        if x is None:
            return
        # update min/max
        if x < self.min: self.min = x
        if x > self.max: self.max = x
        # Welford
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.M2 += delta * delta2

    @property
    def std(self) -> float:
        return math.sqrt(self.M2 / self.count) if self.count > 1 else 0.0

    def summary(self):
        if self.count == 0:
            return {"count": 0, "mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}
        return {
            "count": self.count,
            "mean": self.mean,
            "std": self.std,
            "min": self.min if self.min != float("inf") else 0.0,
            "max": self.max if self.max != float("-inf") else 0.0,
        }

def to_int(x, default=None):
    try:
        if x is None or x == "":
            return default
        return int(x)
    except Exception:
        return default

def to_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def fmt_stat(s):
    return f"count={s['count']:>7d}  mean={s['mean']:>9.3f}  std={s['std']:>9.3f}  min={s['min']:>8.3f}  max={s['max']:>8.3f}"

# ─────────────────────────────────────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────────────────────────────────────

consumer_conf = {
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    # Optional tuning:
    # "enable.auto.commit": True,
    # "max.poll.interval.ms": 300000,
    # "session.timeout.ms": 45000,
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC, "fhvhv_taxi_stream"])

# Aggregations
borough_stats = defaultdict(lambda: {
    "trip_distance": RollingStats(),
    "tip_amount": RollingStats(),
    "total_amount": RollingStats(),
})
zone_stats = defaultdict(lambda: {
    "trip_distance": RollingStats(),
    "tip_amount": RollingStats(),
    "total_amount": RollingStats(),
})
pickup_counter = Counter()
dropoff_counter = Counter()

def shutdown(signum=None, frame=None):
    print("\n⏹️  Interrupted, closing consumer…", file=sys.stderr)
    try:
        consumer.close()
    finally:
        print("✅ Consumer closed.")
        sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)

# ─────────────────────────────────────────────────────────────────────────────
# STREAM LOOP
# ─────────────────────────────────────────────────────────────────────────────

print("📊 Starting streaming rolling stats …")
n_messages = 0

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        # benign if it's a partition EOF or similar; skip others
        continue

    try:
        record = json.loads(msg.value().decode("utf-8"))
    except Exception:
        continue

    # Extract fields (robust to missing/None/empty)
    pu = to_int(record.get("pulocationid"), default=None)
    do = to_int(record.get("dolocationid"), default=None)


    def to_float(x, default=None):
        """Convert to float; return default on None/''/NaN/non‑finite/parse errors."""
        if x is None:
            return default
        try:
            v = float(x)
            if math.isfinite(v):
                return v
        except Exception:
            pass
        return default


    def pick_float(*candidates, default=0.0, min_value=None, max_value=None):
        """
        Return the first candidate that converts to a finite float and passes optional bounds.
        Does NOT treat 0.0 as missing.
        """
        for c in candidates:
            v = to_float(c, default=None)
            if v is None:
                continue
            if (min_value is not None and v < min_value):
                continue
            if (max_value is not None and v > max_value):
                continue
            return v
        return default


    # Prefer the yellow fields; fall back to fhvhv fields
    trip_distance = pick_float(record.get("trip_distance"),
                               record.get("trip_miles"),
                               default=0.0, min_value=0.0)

    tip_amount = pick_float(record.get("tip_amount"),
                            record.get("tips"),
                            default=0.0, min_value=0.0)

    total_amount = pick_float(record.get("total_amount"),
                              record.get("driver_pay"),
                              default=0.0)
    #print(pu, do, trip_distance, tip_amount, total_amount)

    # Update visit counters
    if pu is not None:
        pickup_counter[pu] += 1
    if do is not None:
        dropoff_counter[do] += 1

    # Determine borough from pickup zone
    if pu is not None:
        borough = zone_ids_to_borough.get(pu)
        if borough:
            bstats = borough_stats[borough]
            bstats["trip_distance"].update(trip_distance)
            bstats["tip_amount"].update(tip_amount)
            bstats["total_amount"].update(total_amount)

        # Update zone stats for pickup zone
        zstats = zone_stats[pu]
        zstats["trip_distance"].update(trip_distance)
        zstats["tip_amount"].update(tip_amount)
        zstats["total_amount"].update(total_amount)

    n_messages += 1

    # Periodic reporting
    if n_messages % PRINT_EVERY == 0:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"Report @ {ts}  |  processed messages: {n_messages}")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # Top/bottom K pickup zones by visit counts
        # If you prefer both pickups+dropoffs, set VISITS="both"
        if VISITS == "both":
            visits = Counter(pickup_counter)
            for z, c in dropoff_counter.items():
                visits[z] += c
        else:
            visits = pickup_counter

        # Remove zones never seen (count==0)
        seen = [(z, c) for z, c in visits.items() if c > 0]
        seen.sort(key=lambda x: x[1], reverse=True)
        top_k = seen[:TOP_K]
        bottom_k = sorted(seen, key=lambda x: x[1])[:TOP_K]

        def zone_desc(zid):
            b = zone_ids_to_borough.get(zid, "Unknown")
            return f"Zone {zid} ({b})"

        # Top K
        print("\n📍 Top visited pickup zones:")
        for zid, cnt in top_k:
            zs = zone_stats[zid]
            print(f"  {zone_desc(zid)} — visits={cnt}")
            for feat in ("trip_distance", "tip_amount", "total_amount"):
                print(f"     {feat:>13}: {fmt_stat(zs[feat].summary())}")

        # Bottom K
        print("\n📍 Least visited pickup zones (among seen):")
        for zid, cnt in bottom_k:
            zs = zone_stats[zid]
            print(f"  {zone_desc(zid)} — visits={cnt}")
            for feat in ("trip_distance", "tip_amount", "total_amount"):
                print(f"     {feat:>13}: {fmt_stat(zs[feat].summary())}")

        # Borough stats
        print("\n🏙 Borough rolling stats (by pickup borough):")
        for borough in sorted(borough_stats.keys()):
            feats = borough_stats[borough]
            print(f"  → {borough}")
            for feat in ("trip_distance", "tip_amount", "total_amount"):
                print(f"     {feat:>13}: {fmt_stat(feats[feat].summary())}")

        print("\n(Every", PRINT_EVERY, "messages)\n")
