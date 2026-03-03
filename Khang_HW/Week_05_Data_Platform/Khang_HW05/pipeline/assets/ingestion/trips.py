"""@bruin

name: ingestion.trips
type: python

image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

# Define output columns (names + types) for metadata, lineage, and quality checks.
# Marking a few columns that form a stable composite key (vendor + pickup/dropoff datetimes)
# so they could later be used for deduplication if needed.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: integer
    description: "Taxi vendor identifier"
    primary_key: true
  - name: tpep_pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp"
    primary_key: true
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp"
    primary_key: true
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: payment_type
    type: integer
    description: "Payment method code"
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
  - name: total_amount
    type: float
    description: "Total charged amount (fare + extras)"
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow/green)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the row was extracted from source"

@bruin"""

import os
import json
from datetime import datetime
import io

import pandas as pd
import requests
from dateutil import parser

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# Only implement `materialize()` if you are using Bruin Python materialization.
# This function will be called by Bruin; it must return a pandas DataFrame.
def materialize():
    """
    Ingest trip parquet files from the public NYC taxi API.

    This implementation uses the date window variables and the `taxi_types`
    pipeline variable to build a list of URLs to fetch. All files in the
    window are combined into a single DataFrame with an extra
    `extracted_at` column for lineage.
    """

    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    vars_obj = json.loads(bruin_vars)
    taxi_types = vars_obj.get("taxi_types", ["yellow"])

    def month_iterator(start: str, end: str):
        # yield (year, month) tuples between start and end inclusive
        s = parser.parse(start).date().replace(day=1)
        e = parser.parse(end).date()
        while s <= e:
            yield s.year, s.month
            # advance to next month
            month = s.month + 1
            year = s.year + (month - 1) // 12
            month = ((month - 1) % 12) + 1
            s = s.replace(year=year, month=month)

    frames = []
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    extracted_at = datetime.utcnow()

    for taxi in taxi_types:
        for year, month in month_iterator(start_date, end_date):
            filename = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
            url = base_url + filename
            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                df = pd.read_parquet(io.BytesIO(resp.content))
                df["taxi_type"] = taxi
                df["extracted_at"] = extracted_at
                frames.append(df)
            except Exception as e:
                # log and continue; missing months are okay
                print(f"warning: failed to fetch {url}: {e}")
                continue

    if not frames:
        # return empty dataframe with expected columns
        cols = [
            "vendor_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "payment_type",
            "fare_amount",
            "total_amount",
            "taxi_type",
            "extracted_at",
        ]
        return pd.DataFrame(columns=cols)

    result = pd.concat(frames, ignore_index=True)
    return result


