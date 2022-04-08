## the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config
from datetime import datetime

from prefect import Client as PrefectClient

from nl_open_data.utils import query_cbs_catalogs

# Schedules multiple flow-runs (10 datasets per flow_run) on prefect cloud to upload the entire CBS core statline repository
# TODO: Currently only v3, add v4

# Prefect client parameters
TENANT_SLUG = "dataverbinders"

prefect_client = PrefectClient()  # Local api key has been stored previously
prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

# flow parameters
SOURCE = "cbs"
THIRD_PARTY = False
GCP_ENV = "prod"
FORCE = False
CONFIG = config
ODATA_CBS = query_cbs_catalogs(third_party=THIRD_PARTY, source=SOURCE)[SOURCE]

mirror_time = f"{datetime.today().date()}_{datetime.today().time()}"

## Schedule flow-runs
for i in range(len(ODATA_CBS) // 10 + 1):
    # for i in range(5):
    # run parameters
    VERSION_GROUP_ID = "statline_bq"
    CBS_V3_RUN_NAME = f"cbs_v3_batch_{i}_{mirror_time}"

    cbs_v3_parameters = {
        "ids": ODATA_CBS[(10 * i) : (10 * i + 10)],
        "source": SOURCE,
        "third_party": THIRD_PARTY,
        "gcp_env": GCP_ENV,
        "force": FORCE,
        "config": CONFIG
    }

    flow_run_id = prefect_client.create_flow_run(
        version_group_id=VERSION_GROUP_ID,
        parameters=cbs_v3_parameters,
        run_name=CBS_V3_RUN_NAME,
    )
