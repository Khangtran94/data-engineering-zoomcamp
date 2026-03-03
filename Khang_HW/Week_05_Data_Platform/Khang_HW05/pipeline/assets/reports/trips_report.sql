/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: report_date
    type: date
    description: "Date of the trip (based on pickup_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow/green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment method"
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: "Number of trips in this group"
    checks:
      - name: not_null
      - name: positive
  - name: total_fare
    type: float
    description: "Sum of all fares for this group"
    checks:
      - name: non_negative
  - name: avg_fare
    type: float
    description: "Average fare for this group"
    checks:
      - name: non_negative
  - name: total_trip_distance
    type: float
    description: "Total distance traveled in this group"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Average distance per trip"
    checks:
      - name: non_negative
  - name: avg_passenger_count
    type: float
    description: "Average passengers per trip"
    checks:
      - name: non_negative

custom_checks:
  - name: trip_count_consistency
    description: "Verify trip count matches expected positive integer"
    query: |
      SELECT COUNT(*) as violations
      FROM reports.trips_report
      WHERE trip_count <= 0

@bruin */

-- Aggregate staging trips by date, taxi_type, and payment_type
SELECT
  DATE(t.pickup_datetime) AS report_date,
  t.taxi_type,
  t.payment_type_name,
  COUNT(*) AS trip_count,
  SUM(t.fare_amount) AS total_fare,
  AVG(t.fare_amount) AS avg_fare,
  SUM(t.trip_distance) AS total_trip_distance,
  AVG(t.trip_distance) AS avg_trip_distance,
  AVG(t.passenger_count) AS avg_passenger_count
FROM staging.trips t
WHERE DATE(t.pickup_datetime) >= DATE('{{ start_datetime }}')
  AND DATE(t.pickup_datetime) < DATE('{{ end_datetime }}')
GROUP BY 1, 2, 3

