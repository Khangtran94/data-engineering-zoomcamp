import pandas as pd
from sqlalchemy import create_engine
import click

# Schema mapping to handle Nulls in integer columns correctly
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

PARSE_DATES = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

@click.command()
@click.option("--pg-user", required=True)
@click.option("--pg-pass", required=True)
@click.option("--pg-host", default="localhost")
@click.option("--pg-port", default=5433, type=int) # Updated default to 5433
@click.option("--pg-db", default="ny_taxi")
@click.option("--target-table", required=True)
@click.option("--year", default=2021, type=int)
@click.option("--month", default=1, type=int)
@click.option("--chunksize", default=100_000, type=int)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month, chunksize):
    
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year:04d}-{month:02d}.csv.gz"
    
    # Connection string using psycopg2
    engine = create_engine(f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    print(f"Connecting to database at {pg_host}:{pg_port}...")
    
    # Create the iterator
    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize
    )

    # Ingestion loop
    try:
        for i, df in enumerate(df_iter):
            # 'replace' for the first chunk to create table/headers, 'append' thereafter
            mode = 'replace' if i == 0 else 'append'
            
            df.to_sql(name=target_table, con=engine, if_exists=mode, index=False)
            
            print(f"Chunk {i+1} successfully injected into {target_table}")
            
    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    run()