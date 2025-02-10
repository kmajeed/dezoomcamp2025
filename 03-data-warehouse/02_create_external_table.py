from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

dataset_id = "taxi_data"  # Replace with your dataset ID
table_id = "yellow_taxi_2024"

table = bigquery.Table(f"{client.project}.{dataset_id}.{table_id}")  # API reference

external_config = bigquery.ExternalConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    source_uris=[  # Set source_uris DIRECTLY in ExternalConfig!
        "gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-01.parquet",
        "gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-02.parquet",
        "gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-03.parquet",
        "gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-04.parquet",
        "gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-05.parquet",
        "gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-06.parquet",
        "gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-07.parquet",
    ],
)

table.external_data_configuration = external_config

table = client.create_table(table)  # Make an API request.

print(f"Created external table {table.full_table_id}")