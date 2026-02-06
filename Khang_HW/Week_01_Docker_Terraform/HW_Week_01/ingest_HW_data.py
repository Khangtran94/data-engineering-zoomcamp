import pandas as pd
from sqlalchemy import create_engine
import click

# Download and save the Parquet file
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
df_green = pd.read_parquet(parquet_url)
# df_green.to_parquet('green_tripdata_2025-11.parquet')

# Download and save the CSV file
csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
df_zones = pd.read_csv(csv_url)
df_zones.to_csv('taxi_zone_lookup.csv', index=False)

# print("Files downloaded successfully!")
from sqlalchemy import create_engine
DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/ny_taxi"
engine = create_engine(DATABASE_URL)

### Ingest zone data to database
df_zones.to_sql(name='zone_data', con=engine, if_exists='replace', index=False)
print("Zone data ingested successfully!")

DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

for column, dtype in DTYPE.items():
    if column in df_green.columns:  # Check if the column exists
        df_green[column] = df_green[column].astype(dtype)

df_green.head(n=0).to_sql(name='nyc_taxi_green_nov_2025', con=engine, if_exists='replace')

chunksize = 10000
num_chunks = len(df_green) // chunksize + (1 if len(df_green) % chunksize != 0 else 0)
for i in range(num_chunks):
    # Select the current chunk using iloc
    start = i * chunksize
    end = min((i + 1) * chunksize, len(df_green))  # Ensure not exceeding the DataFrame size
    chunk = df_green.iloc[start:end]
    
    # Write the current chunk to the database
    chunk.to_sql(name='nyc_taxi_green_nov_2025', con=engine, if_exists='append', index=False)
    print(f"Inserted chunk {i+1} of size {len(chunk)} rows.")

print("All chunks have been ingested into the database.")