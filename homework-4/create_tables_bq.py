from google.cloud import bigquery, storage


CREDENTIALS_FILE = "gcs.json"
BUCKET_NAME = "dezoomcamp_hw3_2025_guidocaru"
DATASET_NAME = "zoomcamp"
SERVICE = "fhv"  # green #yellow
SOURCE_FORMAT = "PARQUET"

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


def get_gcs_file_uris(bucket_name: str, service: str) -> list:
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    file_uris = [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.startswith(f"{service}/")
    ]

    return file_uris


def create_bq_table(dataset_id: str, service: str, source_uris: list):
    table_id = f"{dataset_id}.{service}_trips"

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = SOURCE_FORMAT
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    load_job = bq_client.load_table_from_uri(
        source_uris, table_id, job_config=job_config
    )

    load_job.result()
    print(f"Table '{table_id}' created or loaded with data from GCS.")


dataset = get_or_create_dataset(DATASET_ID)
print(f"Using dataset: {dataset.dataset_id}")

try:
    source_uris = get_gcs_file_uris(BUCKET_NAME, SERVICE)
    create_bq_table(DATASET_ID, SERVICE, source_uris)
except ValueError as e:
    print(f"Error: {e}")
