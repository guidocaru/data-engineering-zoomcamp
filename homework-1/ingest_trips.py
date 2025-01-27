#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from pyarrow.parquet import ParquetFile
import pyarrow as pa
from sqlalchemy import create_engine
import argparse
import os


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    filename = "trips.parquet"

    os.system(f"wget {url} -O {filename}")

    parquet_file = ParquetFile(filename)
    first_hundred = next(parquet_file.iter_batches(batch_size=100))
    df_trips = pa.Table.from_batches([first_hundred]).to_pandas()

    df_trips = pd.read_parquet(filename)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    df_trips.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df_trips.to_sql(name=table_name, con=engine, if_exists="append")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="ingest trips data to postgres db")

    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--db")
    parser.add_argument("--table_name")
    parser.add_argument("--url")

    args = parser.parse_args()
    main(args)
