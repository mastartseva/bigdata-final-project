import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "iot_raw_events"
BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_event():
    now = datetime.now(timezone.utc)
    return {
        "device_id": str(random.randint(1000, 9999)),
        "type_id": random.randint(1, 5),
        "event_time_ms": int(now.timestamp() * 1000),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2)
    }

def main():
    print("Starting IoT generator...")
    while True:
        event = generate_event()
        producer.send(TOPIC, value=event)
        producer.flush()
        print(event)
        time.sleep(1)

if __name__ == "__main__":
    main()