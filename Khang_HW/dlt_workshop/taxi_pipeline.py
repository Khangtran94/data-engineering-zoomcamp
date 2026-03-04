import dlt
from dlt.sources.helpers.rest_client import RESTClient


@dlt.resource(write_disposition="replace")
def taxi_pipeline():

    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    )

    page = 1  # pagination starts at 1

    while True:
        response = client.get("", params={"page": page})
        data = response.json()

        print(f"Page {page} -> {len(data)} records")

        if not data:
            break

        yield from data

        page += 1


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data"
    )

    load_info = pipeline.run(
        taxi_pipeline(),
        table_name="taxi_rides"
    )

    print(load_info)