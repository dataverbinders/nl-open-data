## the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config
from datetime import datetime

from google.cloud import bigquery
from prefect import Client as PrefectClient

from nl_open_data.utils import query_cbs_catalogs

# Prefect client parameters
TENANT_SLUG = "dataverbinders"

prefect_client = PrefectClient()  # Local api key has been stored previously
prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token
# Schedules multiple flow-runs (10 datasets per flow_run) on prefect cloud to upload the entire external statline repository (odata v3)
# TODO: Currently only v3, add v4

external_v3_datasets = query_cbs_catalogs(
    third_party=True, odata_version="v3", source=None
)
mirror_time = f"{datetime.today().date()}_{datetime.today().time()}"

## Schedule flow-runs
for source in set(external_v3_datasets.keys()):

    # flow parameters
    DATA = external_v3_datasets[source]
    SOURCE = source.lower()
    THIRD_PARTY = True
    GCP_ENV = "prod"
    FORCE = False

    for i in range(len(DATA) // 10 + 1):
        # run parameters
        VERSION_GROUP_ID = "statline_bq"
        EXTERNAL_V3_RUN_NAME = f"external_{SOURCE}_v3_batch_{i}_{mirror_time}"

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
