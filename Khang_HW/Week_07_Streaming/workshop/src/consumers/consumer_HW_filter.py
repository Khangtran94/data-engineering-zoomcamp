import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models_HW import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id=None,
    value_deserializer=ride_deserializer,
    consumer_timeout_ms=5000 # stop after 5 seconds of no new messages
)

print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    if ride.trip_distance <= 5:
        continue  # skip short tr
    pickup_dt = datetime.fromtimestamp(ride.lpep_pickup_datetime / 1000)
    dropoff_dt = datetime.fromtimestamp(ride.lpep_dropoff_datetime / 1000)
    # print(f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
        #   f"passengers={ride.passenger_count}, "
        #   f"distance={ride.trip_distance}, "
        #   f"tip=${ride.tip_amount:.2f}, "
        #   f"amount=${ride.total_amount:.2f}, "
        #   f"pickup={pickup_dt}, "
        #   f"dropoff={dropoff_dt}")
    count += 1

print(f"\nTotal received: {count} messages")
consumer.close()