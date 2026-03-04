import dlt
import json

pipeline_name = "taxi_pipeline"
pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")

# Export schema as JSON
schema_dict = pipeline.default_schema.to_dict()

# Save to file
with open("schema.json", "w") as f:
    json.dump(schema_dict, f, indent=2)

print("✅ Schema saved to schema.json")
print(f"Pipeline: {pipeline_name}")
print(f"Tables: {list(pipeline.default_schema.tables.keys())}")
print("\nUpload schema.json to https://dbdiagram.io/ for visual diagram")