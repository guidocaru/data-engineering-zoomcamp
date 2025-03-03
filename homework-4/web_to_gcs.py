import io
import os
import requests
import pandas as pd
from google.cloud import storage


INIT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
CREDENTIALS_FILE = "gcs.json"
BUCKET = "dezoomcamp_hw3_2025_guidocaru"


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):

        month = "0" + str(i + 1)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        local_file = f"data/{file_name}"

        request_url = f"{INIT_URL}/{file_name}"
        r = requests.get(request_url)
        open(local_file, "wb").write(r.content)
        print(f"Local: {file_name}")

        df = pd.read_parquet(local_file, engine="pyarrow")
        if service == "green":
            df = df.drop("ehail_fee", axis=1)
        if service == "yellow":
            df = df.drop("airport_fee", axis=1)
        if service == "fhv":
            df = df.drop("SR_Flag", axis=1)
            df["PUlocationID"] = df["PUlocationID"].astype(float)
            df["DOlocationID"] = df["DOlocationID"].astype(float)

        df.to_parquet(local_file, engine="pyarrow")

        upload_to_gcs(BUCKET, f"{service}/{file_name}", local_file)
        print(f"GCS: {service}/{file_name}")


# web_to_gcs("2019", "green")
# web_to_gcs("2020", "green")
# web_to_gcs("2019", "yellow")
# web_to_gcs("2020", "yellow")
web_to_gcs("2019", "fhv")
