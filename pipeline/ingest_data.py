import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Trip data configuration

trips_data_prefix = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
)

trips_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

trip_parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


# Zone lookup configuration
zone_lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

lookup_zones_dtypes = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

def ingest_zone_lookup(engine):
    df_zones = pd.read_csv(zone_lookup_url, dtype=lookup_zones_dtypes)

    df_zones.to_sql(
        name="lookup_zones",
        con=engine,
        if_exists="replace",
        index=False,
    )

def ingest_trip_data(
    engine, year, month, target_table, chunksize
):
    """Ingest monthly NYC yellow taxi trip data."""
    url = f"{trips_data_prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    df_iter = pd.read_csv(
        url,
        dtype=trips_dtype,
        parse_dates=trip_parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
        )


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Trip data table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
    target_table,
    chunksize,
):
    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}"
        f"@{pg_host}:{pg_port}/{pg_db}"
    )

    ingest_zone_lookup(engine)

    ingest_trip_data(
        engine=engine,
        year=year,
        month=month,
        target_table=target_table,
        chunksize=chunksize,
    )


if __name__ == "__main__":
    run()
