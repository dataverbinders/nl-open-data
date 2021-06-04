import requests
from datetime import datetime

from prefect import Flow
from prefect.tasks.prefect import StartFlowRun
from prefect import Client

from nl_open_data.config import config as CONFIG
from nl_open_data.utils import get_gcs_uris
from nl_open_data.ckan import get_datasets

# GCP env parameters
GCP_ENV = "dev"
PROD_ENV = None

# General script parameters
SOURCE = "uwv"
RUN_TIME = f"{datetime.today().date()}_{datetime.today().time()}"
PROJECT = "nl_open_data"

######################################################
# Zipped csv folder flow (to gcs)

# Get urls
datasets = get_datasets(
    base_url="https://data.overheid.nl/data/api/3", search_terms=["uwv", "open_match"]
)
assert len(datasets) == 1, "More then 1 datasets returned for UWV Open Match"
resources = datasets[0]["resources"]
# flow parameters
UWV_OPEN_MATCH_URLS = [
    resource["url"] for resource in resources if ".zip" in resource["url"]
]
CSV_DELIMITER = ";"
CSV_ENCODING = "8859"
DATASET_NAME = "open_match_data"
GCS_FOLDER = SOURCE + "/" + DATASET_NAME + "/" + datetime.today().strftime("%Y%m%d")

# run parameters
ZIP_VERSION_GROUP_ID = "zipped_file"
ZIP_RUN_NAME = (
    f"uwv_open_match_data_{datetime.today().date()}_{datetime.today().time()}"
)

zip_parameters = {
    "urls": UWV_OPEN_MATCH_URLS,
    "csv_delimiter": CSV_DELIMITER,
    "csv_encoding": CSV_ENCODING,
    "gcs_folder": GCS_FOLDER,
}

zip_flow = StartFlowRun(
    flow_name=ZIP_VERSION_GROUP_ID,
    project_name=PROJECT,
    run_name=ZIP_RUN_NAME,
    parameters=zip_parameters,
    wait=True,
)

######################################################

## gcs_to_bq
# flow parameters
URIS = [
    uri
    for uri in get_gcs_uris(
        gcs_folder=GCS_FOLDER, source=SOURCE, config=CONFIG, gcp_env=GCP_ENV
    )
    if uri.split(".")[-1] == ".parquet"
]
BQ_DATASET_NAME = f"{SOURCE}_{DATASET_NAME}"
BQ_DATASET_DESCRIPTION = """
    
"""

# run parameters
VERSION_GROUP_ID = "gcs_to_bq"
BQ_RUN_NAME = f"gcs_to_bq_uwv_{RUN_TIME}"


PARAMETERS = {
    "uris": URIS,
    "dataset_name": BQ_DATASET_NAME,
    # "config": CONFIG, #BUG
    "gcp_env": GCP_ENV,
    "prod_env": PROD_ENV,
    "description": BQ_DATASET_DESCRIPTION,
}

# Schedule run
gcs_to_bq_flow = StartFlowRun(
    flow_name=VERSION_GROUP_ID,
    project_name=PROJECT,
    run_name=BQ_RUN_NAME,
    parameters=PARAMETERS,
    wait=True,
)

######################################################

## Flow of flows

with Flow("parent-flow") as flow:
    uwv_flow = gcs_to_bq_flow(upstream_tasks=[zip_flow])

flow.run()
