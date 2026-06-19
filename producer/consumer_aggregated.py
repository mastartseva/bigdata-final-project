from kafka import KafkaConsumer
import json

TOPIC = "iot_aggregated"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
    key_deserializer=lambda k: json.loads(k.decode("utf-8")) if k is not None else None,
)

print("Listening on iot_aggregated...")

for msg in consumer:
    if msg.value is None:
        print("KEY:", msg.key, "VALUE: <null/tombstone>")
        continue

    print("KEY:", msg.key, "VALUE:", msg.value)