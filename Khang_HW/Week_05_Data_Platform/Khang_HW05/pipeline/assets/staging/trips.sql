/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: create+replace

columns:
  - name: vendor_id
    type: integer
    description: "Taxi vendor identifier"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp"
    primary_key: true
    checks:
      - name: not_null
  - name: passenger_count
    type: float
    description: "Number of passengers"
    checks:
      - name: positive
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: pickup_location_id
    type: integer
    description: "NYC location zone ID for pickup"
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "NYC location zone ID for dropoff"
    checks:
      - name: not_null
  - name: payment_type_id
    type: integer
    description: "Payment method code"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment method"
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Fare amount in USD"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total charged amount (fare + extras)"
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow/green)"
    checks:
      - name: not_null

@bruin */


-- Simple staging layer: filter and add taxi_type column
SELECT
  vendor_id,
  tpep_pickup_datetime AS pickup_datetime,
  tpep_dropoff_datetime AS dropoff_datetime,
  passenger_count,
  trip_distance,
  pu_location_id AS pickup_location_id,
  do_location_id AS dropoff_location_id,
  payment_type AS payment_type_id,
  CAST(0 AS VARCHAR) AS payment_type_name,  -- placeholder
  fare_amount,
  total_amount,
  taxi_type
FROM ingestion.trips
WHERE 
  tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
  -- Filter invalid records
  AND tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND pu_location_id IS NOT NULL
  AND do_location_id IS NOT NULL
  AND payment_type IS NOT NULL
  AND fare_amount >= 0
  AND total_amount >= 0
  AND passenger_count > 0
