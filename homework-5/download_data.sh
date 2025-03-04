#!/bin/bash

DATA_DIR="data/raw"

mkdir -p "$DATA_DIR"

URLS=(
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet"
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)

for URL in "${URLS[@]}"; do
  wget -P "$DATA_DIR" "$URL"
done
