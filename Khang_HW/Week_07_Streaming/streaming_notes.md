# Introduction to Streaming Data Engineering

Welcome to the world of real-time data! Since you are new to Streaming, this note will walk you through the core concepts and how they are implemented in this repository's Week 7 workshop.

## What is Streaming Data?
Traditional data engineering often relies on **Batch Processing**: running jobs periodically (e.g., every night or every hour) to process a large chunk of historical data all at once.
**Streaming (or Real-Time Processing)** is different. In streaming, data is processed continuously as it arrives, piece by piece. This allows for immediate actions, like alerting on fraud the moment a transaction happens, or showing live dashboards of current taxi wait times.

## The Core Components of a Streaming Architecture

A typical streaming architecture has three main pillars:
1. **Producers (The Source):** Applications or services that generate and send events (e.g., a taxi app sending a message every time a ride starts).
2. **Message Broker / Transport Layer:** The central hub that receives messages from producers and stores them temporarily so consumers can read them. **Apache Kafka** is the industry standard for this.
3. **Consumers / Processing Engines (The Destination):** Applications that read the messages from the broker, either to just save them or to perform complex aggregations on the fly (like calculating average fare over the last 5 minutes).

---

## How This Works in Your Workshop Repository

Based on the files in `Week_07_Streaming/workshop`, here is exactly how your local streaming stack is set up:

### 1. The Message Broker: Redpanda
In your `docker-compose.yml`, you are running **Redpanda**. Redpanda is a modern, faster, lightweight alternative to Apache Kafka, but it works exactly the same way and uses the same Python libraries. It listens on port `9092` to receive your streaming events into a channel known as a "topic". Specifically, your topic is named `rides`.

### 2. The Producer: `src/producers/producer.py`
This script simulates a real-time data source. 
- It downloads a static Parquet file containing historical NYC Yellow Taxi trips.
- It iterates through each row one by one, converts the ride information into a JSON text message.
- It sends these JSON messages to the Redpanda broker into the `rides` topic, pausing for 0.01 seconds between each message to mimic real-time, steady event generation.

### 3. Stream Processing Engine: PyFlink (`src/job/aggregation_job.py`)
This is where the magic happens. You are using **Apache Flink** (via PyFlink for Python), which is a powerful engine for processing data streams in real-time.
- **Source:** Flink connects to your Redpanda `rides` topic and reads streams of taxi rides as they come in.
- **Windowing:** Streaming data never stops, so you can't just run a `SUM()` over all data. Instead, Flink groups the continuous stream into time "windows". In `aggregation_job.py`, it uses a **1-minute Tumbling Window** (`INTERVAL '1' MINUTE`). This means it looks at 60 seconds of taxi rides at a time.
- **Aggregation:** Inside each window, it calculates the number of trips (`COUNT(*)`) and the total revenue (`SUM(total_amount)`) for each pickup location (`PULocationID`).
- **Sink (Destination):** Finally, Flink writes these real-time 1-minute aggregated summaries directly into your running PostgreSQL database table called `processed_events_aggregated`.

### 4. Simple Consumer: `src/consumers/consumer_postgres.py`
Sometimes you don't need complex window aggregations; you just want to save the raw stream directly to a database. This script is a simple consumer that reads each raw JSON message from Redpanda and inserts it into a PostgreSQL table (`processed_events`). It's a great example of a simple streaming ingestion pipeline without PyFlink.

## Summary of Your Data Flow

1. **`producer.py`** -> (sends JSON rides) -> **Redpanda** (`rides` topic)
2. **Redpanda** -> (read by PyFlink) -> **`aggregation_job.py`** -> (1-minute aggregations) -> **PostgreSQL** (`processed_events_aggregated`)
3. **Redpanda** -> (read by Python) -> **`consumer_postgres.py`** -> (raw row ingestion) -> **PostgreSQL** (`processed_events`)

---

## Deep Dive: Sinks, Windows, and Watermarks

### 1. Why do we need 2 Sinks?
In this workshop, you have two different PostgreSQL tables acting as destinations (sinks): `processed_events` and `processed_events_aggregated`. They represent two entirely different use cases for streaming data:

*   **Sink 1: `processed_events` (Pass-Through / Raw ETL)**
    *   **Where it's used:** In `pass_through_job.py` or your simple Python `consumer_postgres.py`.
    *   **Why we need it:** This is for **record-at-a-time** processing. You read a taxi ride from Redpanda, maybe change the timestamp format, and insert it directly into Postgres. You use this when you just want a real-time replica of every single raw event in your database without any math applied to it.
*   **Sink 2: `processed_events_aggregated` (Stateful Aggregation)**
    *   **Where it's used:** In `aggregation_job.py`.
    *   **Why we need it:** This is for **real-time analytics**. You don't want a billion raw rows; you want to know "how much money did we make per location *in the last minute*?" Flink holds the stream in memory, calculates the math (`COUNT` and `SUM`), and only writes the final summarized row to this sink.

### 2. What is Tumble (Tumbling Window)?
In streaming, data never stops arriving. Because it never stops, you can't say "Sum up all the data" like you do in standard SQL, because Flink doesn't know when the data ends!

To do math, we chop the infinite stream into finite pieces called **Windows**.
A **Tumbling Window** is a window of a *fixed size* that *does not overlap*.

In your `aggregation_job.py`, you have:
```sql
TUMBLE(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '1' MINUTE)
```

*   **How it works:** Flink watches the clock. It gathers all taxi rides from `10:00:00` to `10:00:59`. At `10:01`, the window "tumbles" over. It calculates the `SUM` and `COUNT` for that minute, writes the result to PostgreSQL, and then starts a completely fresh, empty window for `10:01:00` to `10:01:59`.

### 3. What is a Watermark?
In the real world, data gets delayed. Imagine a taxi loses cell service and its "trip finished" message is sent 4 seconds late to Redpanda.
If Flink strictly closed the 1-minute Tumbling Window exactly at `10:01:00`, that delayed taxi ride would be missed from the calculation!

A **Watermark** is a buffer that tells Flink how long to wait for late data before officially closing a window and doing the math.

In your `aggregation_job.py`, you have this line:
```sql
WATERMARK for event_timestamp as event_timestamp - INTERVAL '5' SECOND
```

*   **How it works here:** This tells Flink to expect data to be up to **5 seconds late**.
*   **The resulting logic:** If Flink is processing a 1-minute window that ends at `10:01:00`, it **will not** close that window at `10:01:00`. Instead, it will keep the window open until it sees an event with a timestamp of `10:01:05` arrive in the stream. Once it sees that `10:01:05` event, Flink says: *"Okay, my watermark is 5 seconds, so I am confident all the data for 10:01:00 has arrived. Now I will close the `10:00-10:01` window, do the math, and write to Postgres!"*

## Next Steps to Run
1. Start your infrastructure using `docker-compose up -d`.
2. Ensure you have the required python packages (`kafka-python`, `pandas`, `psycopg2`).
3. Run the producer to start sending data.
4. Run either the simple consumer or submit the PyFlink job to see the data land in Postgres in real-time!
