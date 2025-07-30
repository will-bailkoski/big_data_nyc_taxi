import json
import time
from confluent_kafka import Consumer, Producer, KafkaException
from river import cluster
import math

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

BROKER = "localhost:10000"
SOURCE_TOPIC = "yellow_taxi_stream"
OUTPUT_TOPIC = "taxi_clustered"
GROUP_ID = "stream_cluster_group"

NUM_CLUSTERS = 4

# ─────────────────────────────────────────────────────────────────────────────
# 1) Kafka Consumer / Producer setup
# ─────────────────────────────────────────────────────────────────────────────

consumer_conf = {
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
}
producer_conf = {
    "bootstrap.servers": BROKER,
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe(["yellow_taxi_stream", "fhvhv_taxi_stream"])

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message {msg.key()}: {err}")
    # else:
    #     print("Successful message send")

# ─────────────────────────────────────────────────────────────────────────────
# 2) Initialize River’s streaming KMeans
# ─────────────────────────────────────────────────────────────────────────────

kmeans_model = cluster.KMeans(n_clusters=NUM_CLUSTERS, halflife=0.1)

# ─────────────────────────────────────────────────────────────────────────────
# 3) Main loop: consume, cluster, and produce
# ─────────────────────────────────────────────────────────────────────────────

print(f"Starting streaming-clustering (KMeans with k={NUM_CLUSTERS}) …\n")

msg_count = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            # Skip any error messages
            continue

        # Parse JSON
        try:
            record = json.loads(msg.value().decode("utf-8"))
        except:
            continue

        # Extract features
        try:
            trip_dist = float(record.get("trip_distance", 0.0) or 0.0)
        except:
            trip_dist = 0.0
        try:
            passenger_cnt = float(record.get("passenger_count", 0.0))
            passenger_cnt = 0.0 if math.isnan(passenger_cnt) else passenger_cnt
        except:
            passenger_cnt = 0.0

        x = {"trip_distance": trip_dist, "passenger_count": passenger_cnt}

        # Update (learn) the KMeans model
        kmeans_model.learn_one(x)

        # Predict cluster for this point
        cluster_id = kmeans_model.predict_one(x)
        print(cluster_id)
        record["cluster_id"] = int(cluster_id)

        # Produce annotated JSON
        out_json = json.dumps(record, default=str).encode("utf-8")
        key = msg.key() or f"{msg.partition()}_{msg.offset()}".encode("utf-8")
        producer.produce(
            OUTPUT_TOPIC,
            key=key,
            value=out_json,
            callback=delivery_report,
        )
        producer.poll(0)

        msg_count += 1
        if msg_count % 100 == 0:
            print(f"  • {msg_count} records clustered so far…")

except KeyboardInterrupt:
    print("\n⏹️  Interrupted by user")
finally:
    producer.flush(timeout=10)
    consumer.close()
    print("✅  Clusterer shut down cleanly.")