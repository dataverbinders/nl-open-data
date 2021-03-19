## the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from datetime import datetime

from google.cloud import bigquery
from prefect import Client as PrefectClient

# Schedules multiple flow-runs (10 datasets per flow_run) on prefect cloud to upload the entire CBS core statline repository
# TODO: Currently only v3, add v4

## Get all v3 dataset ids
bq_client = bigquery.Client()

# NOTE:
# CBS provides both 'Catalog' and 'Source' fields. Both provide mostly similar, but not identical information.
# 'Catalog' seems more strictly defined and additionally 'Source' is not present in the v4 catalog,
# so we choose to use 'Catalog' for now, but that could change.

query = """
    SELECT `Identifier`, `Catalog`
    FROM `dataverbinders-dev.None_catalogs.cbs_v3`
"""

query_job = bq_client.query(query)

cbs_v3_ids = []
# cbs_v3_sources = []

for row in query_job:
    cbs_v3_ids.append(row[0])
    # cbs_v3_sources.append(row[1])

# Prefect client parameters
TENANT_SLUG = "dataverbinders"

prefect_client = PrefectClient()  # Local api key has been stored previously
prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

# flow parameters
DATA = cbs_v3_ids
SOURCE = "cbs"  # TODO: connect to sources above
THIRD_PARTY = False
GCP_ENV = "prod"
FORCE = False

## Schedule flow-runs
for i in range(len(DATA) // 10 + 1):
    # run parameters
    VERSION_GROUP_ID = "statline_bq"
    CBS_V3_RUN_NAME = (
        f"cbs_v3_batch_{i}_{datetime.today().date()}_{datetime.today().time()}"
    )

    cbs_v3_parameters = {
        "ids": DATA[(10 * i) : (10 * i + 10)],
        "source": SOURCE,
        "third_party": THIRD_PARTY,
        "gcp_env": GCP_ENV,
        "force": FORCE,
    }

    flow_run_id = prefect_client.create_flow_run(
        version_group_id=VERSION_GROUP_ID,
        parameters=cbs_v3_parameters,
        run_name=CBS_V3_RUN_NAME,
    )
