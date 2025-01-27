FROM python:3.9

RUN apt-get update && apt-get install -y wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app

COPY ingest_zones.py ingest_zones.py
COPY ingest_trips.py ingest_trips.py

ENTRYPOINT ["python"]
