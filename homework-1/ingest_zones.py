#!/usr/bin/env python
# coding: utf-8


import pandas as pd
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
    filename = "zones.csv"

    os.system(f"wget {url} -O {filename}")

    df_zones = pd.read_csv(filename)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    df_zones.to_sql(name=table_name, con=engine, if_exists="replace")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="ingest zones data to postgres db")

    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--db")
    parser.add_argument("--table_name")
    parser.add_argument("--url")

    args = parser.parse_args()
    main(args)
