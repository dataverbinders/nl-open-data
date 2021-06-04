from datetime import datetime
import sys

from prefect import Flow
from prefect.tasks.prefect import StartFlowRun
from prefect.triggers import all_finished

from nl_open_data.config import config as CONFIG
from nl_open_data.utils import get_gcs_uris
from nl_open_data.ckan import get_datasets

# By Default use prod GCP env parameters
GCP_ENV = "prod"
PROD_ENV = "external"
# If dev
print(sys.argv)
if len(sys.argv) != 1:
    if sys.argv[1] == "--dev":
        GCP_ENV = sys.argv[1]
        PROD_ENV = None
    else:
        raise ValueError("Only '--dev' can be provided as an argument")

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
    "gcp_env": GCP_ENV,
    "prod_env": PROD_ENV,
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
# URIS = [
#     uri
#     for uri in get_gcs_uris(
#         gcs_folder=GCS_FOLDER, source=SOURCE, config=CONFIG, gcp_env=GCP_ENV,
#     )
#     # if uri.split(".")[-1] == "parquet"  # Igonre metadata (json files)
# ]
# BQ_DATASET_NAME = f"{SOURCE}_{DATASET_NAME}"
BQ_DATASET_DESCRIPTION = """
Deze dataset start per draaidatum 25-11-2019 en komt in plaats van de UWV Beroepenkaart-data (actueel t/m 20-11-2018) .

De gegevens onder de open match data zijn vacatures en geanonimiseerde CVs in werk.nl. Voor deze set worden deze geaggregeerd 1) per beroep en 2) per viercijferig postcodegebied.
"""  # source: https://data.overheid.nl/dataset/uwv-open-match-data

# run parameters
VERSION_GROUP_ID = "gcs_folder_to_bq_flow"
BQ_RUN_NAME = f"gcs_to_bq_uwv_{RUN_TIME}"


PARAMETERS = {
    "gcs_folder": GCS_FOLDER,
    "dataset_name": DATASET_NAME,
    "source": SOURCE,
    # "config": CONFIG, #BUG
    "gcp_env": GCP_ENV,
    "prod_env": PROD_ENV,
    "description": BQ_DATASET_DESCRIPTION,
}

# Schedule run
gcs_folder_to_bq_flow = StartFlowRun(
    flow_name=VERSION_GROUP_ID,
    project_name=PROJECT,
    run_name=BQ_RUN_NAME,
    parameters=PARAMETERS,
    wait=True,
)
gcs_folder_to_bq_flow.trigger = all_finished  # Always run

######################################################

## Flow of flows

with Flow("parent-flow") as flow:
    uwv_flow = gcs_folder_to_bq_flow(upstream_tasks=[zip_flow])

flow.run()
