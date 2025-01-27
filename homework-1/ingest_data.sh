#!/bin/bash

# Build Docker image
echo "Building Docker image..."
docker build -t ingest-scripts .

# Ingest trips data
echo "Ingesting trips data..."
docker run -it --network=homework-1_default ingest-scripts \
  ingest_trips.py \
  --user=postgres \
  --password=postgres \
  --host=db \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_trips \
  --url="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-10.parquet"

# Ingest zones data
echo "Ingesting zones data..."
docker run -it --network=homework-1_default ingest-scripts \
  ingest_zones.py \
  --user=postgres \
  --password=postgres \
  --host=db \
  --port=5432 \
  --db=ny_taxi \
  --table_name=zones \
  --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

echo "Ingestion completed"
