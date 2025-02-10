from google.cloud import bigquery, storage

CREDENTIALS_FILE = "gcs.json"
BUCKET_NAME = "dezoomcamp_hw3_2025_guidocaru"
TABLE_NAME = "yellow_taxi_external"
DATASET_NAME = "zoomcamp"
EXTERNAL_SOURCE_FORMAT = "PARQUET"

bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

PROJECT_ID = bq_client.project
DATASET_ID = f"{PROJECT_ID}.{DATASET_NAME}"


def get_or_create_dataset(dataset_id: str) -> bigquery.Dataset:
    """Retrieve an existing BigQuery dataset or create a new one."""
    try:
        dataset = bq_client.get_dataset(dataset_id)
        print(f"Dataset '{dataset_id}' already exists.")
    except Exception as e:
        print(f"Creating dataset '{dataset_id}'...")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = bq_client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' created.")
    return dataset


def get_gcs_file_uris(bucket_name: str) -> list:
    """Retrieve a list of GCS file URIs with '.parquet' extension."""
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    file_uris = [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]

    if not file_uris:
        raise ValueError(f"No Parquet files found in bucket '{bucket_name}'.")

    return file_uris


def create_external_table(dataset_id: str, table_name: str, source_uris: list):
    table_id = f"{dataset_id}.{table_name}"
    table = bigquery.Table(table_id)

    external_config = bigquery.ExternalConfig(EXTERNAL_SOURCE_FORMAT)
    external_config.source_uris = source_uris
    table.external_data_configuration = external_config

    bq_client.create_table(table, exists_ok=True)
    print(f"External table '{table_id}' created or already exists.")


dataset = get_or_create_dataset(DATASET_ID)
print(f"Using dataset: {dataset.dataset_id}")

try:
    source_uris = get_gcs_file_uris(BUCKET_NAME)
    create_external_table(DATASET_ID, TABLE_NAME, source_uris)
except ValueError as e:
    print(f"Error: {e}")
