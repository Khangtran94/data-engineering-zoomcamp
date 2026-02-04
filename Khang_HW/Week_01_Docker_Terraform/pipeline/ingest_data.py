import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Column dtypes
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

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():
    # Postgres config
    pg_user = "root"
    pg_password = "root"
    pg_host = "localhost"
    pg_port = 5433
    pg_db = "ny_taxi"

    # Data config
    year = 2021
    month = 1
    chunksize = 100_000
    table_name = "yellow_taxi_data"

    url_prefix = (
        "https://github.com/DataTalksClub/nyc-tlc-data/"
        "releases/download/yellow/"
    )
    url = f"{url_prefix}yellow_tripdata_{year:04d}-{month:02d}.csv.gz"

    # DB engine
    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )

    # Read CSV in chunks
    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize
    )

    for i, df_chunk in enumerate(tqdm(df_iter), start=1):
        if_exists = "replace" if i == 1 else "append"

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi"
        )


if __name__ == "__main__":
    run()
