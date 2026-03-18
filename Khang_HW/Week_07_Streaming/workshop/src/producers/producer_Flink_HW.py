import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models_HW_Flink import Ride, ride_from_row

# Download NYC yellow taxi trip data (first 1000 rows)
# url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
# Get the directory where this script lives
script_dir = Path(__file__).parent

columns = ['PULocationID', 'DOLocationID', 'passenger_count','trip_distance', 
            'tip_amount','total_amount', 'lpep_pickup_datetime','lpep_dropoff_datetime']
df = pd.read_parquet(script_dir / 'green_tripdata_2025-10.parquet', columns=columns)
df['passenger_count'] = df['passenger_count'].fillna(0)

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)
t0 = time.time()

topic_name = 'green-trips'

print('Starting to send messages to Kafka')
for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    # print(f"Sent: {ride}")
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')