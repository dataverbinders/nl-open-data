## the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from datetime import datetime

from google.cloud import bigquery
from prefect import Client as PrefectClient

# Schedules a flow-run on prefect cloud to upload the entire statline repository (odata v3) #TODO: v4

# Get all v3 dataset ids
bq_client = bigquery.Client()

# NOTE:
# CBS provides both 'Catalog' and 'Source' fields. Both provide mostly similar, but not identical information.
# 'Catalog' seems more strictly defined and additionally 'Source' is not present in the v4 catalog,
# so we choose to use 'Catalog' for now, but that could change.

# TODO: DRY
cbs_v3_query = """
    SELECT `Identifier`, `Catalog`
    FROM `dataverbinders-dev.None_catalogs.cbs_v3`
"""
# TODO: Rename iv3 here (meaning when uploading catalogs to BQ) to something else (maybe 'external') - since iv3 is one of the external sources, like mlz or rivm
iv3_v3_query = """
    SELECT `Identifier`, `Catalog`
    FROM `dataverbinders-dev.None_catalogs.iv3_v3`
"""

cbs_v3_query_job = bq_client.query(cbs_v3_query)
iv3_v3_query_job = bq_client.query(iv3_v3_query)

cbs_v3_ids = []
cbs_v3_sources = []
iv3_v3_ids = []
iv3_v3_sources = []

for row in cbs_v3_query_job:
    cbs_v3_ids.append(row[0])
    cbs_v3_sources.append(row[1])
for row in iv3_v3_query_job:
    iv3_v3_ids.append(row[0])
    iv3_v3_sources.append(row[1])

# Separate to different source
external_v3_datasets = dict(zip(iv3_v3_ids, iv3_v3_sources))
iv3_v3_sources_set = set(iv3_v3_sources)

external_v3_ids = {}
for source in iv3_v3_sources_set:
    external_v3_ids[source] = [
        k for k, v in external_v3_datasets.items() if v == source
    ]
#################################################################################
# General

# Prefect client parameters
TENANT_SLUG = "dataverbinders"

prefect_client = PrefectClient()  # Local api key has been stored previously
prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

#################################################################################
# Core CBS

# cbs_v3 flow parameters
DATA = cbs_v3_ids
SOURCE = "cbs"  # TODO: connect to sources above
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = False

for i in range(len(DATA) // 10 + 1):
    # run parameters
    VERSION_GROUP_ID = "statline_bq"
    CBS_V3_RUN_NAME = (
        f"cbs_v3_batch_{i}_{datetime.today().date()}_{datetime.today().time()}"
    )
    # IV3_V3_RUN_NAME = f"iv3_v3_{datetime.today().date()}_{datetime.today().time()}"

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

#################################################################################
# Externals

for source in iv3_v3_sources_set:
    # cbs_v3 flow parameters
    DATA = external_v3_ids[source]
    SOURCE = source.lower()
    THIRD_PARTY = True
    GCP_ENV = "dev"
    FORCE = False

    for i in range(len(DATA) // 10 + 1):
        # run parameters
        VERSION_GROUP_ID = "statline_bq"
        EXTERNAL_V3_RUN_NAME = (
            f"{SOURCE}_v3_batch_{i}_{datetime.today().date()}_{datetime.today().time()}"
        )

        external_v3_parameters = {
            "ids": DATA[(10 * i) : (10 * i + 10)],
            "source": SOURCE,
            "third_party": THIRD_PARTY,
            "gcp_env": GCP_ENV,
            "force": FORCE,
        }

        flow_run_id = prefect_client.create_flow_run(
            version_group_id=VERSION_GROUP_ID,
            parameters=external_v3_parameters,
            run_name=EXTERNAL_V3_RUN_NAME,
        )
