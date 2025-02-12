import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb


@dlt.resource(name="rides")
def get_taxi_data():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/",
        paginator=PageNumberPaginator(
            base_page=1,
            # total_path=None,
            maximum_page=1,
        ),
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page


pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline", destination="duckdb", dataset_name="ny_taxi_data"
)

load_info = pipeline.run(get_taxi_data)
pipeline.dataset(dataset_type="default").rides.df()
