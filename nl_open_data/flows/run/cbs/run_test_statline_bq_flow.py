# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from datetime import datetime

from prefect import Client as PrefectClient
from dask.distributed import Client as DaskClient
from box import Box

# Schedules a flow-run on prefect cloud

# Prefect client parameters
TENANT_SLUG = "dataverbinders"

# # dask parameters
# IP = "127.0.0.1:8888"

# flow parameters
DATA = ["83583NED", "83765NED"]
# DATA = ["84750NED"]
SOURCE = "cbs"
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = True
# CONFIG = Box(config)  # BUG: If config is provided here, an error occurs. If it is provided as a default in the Register script, it works

# run parameters
STATLINE_VERSION_GROUP_ID = "statline_bq"
RUN_NAME = f"test_statline-bq_{datetime.today().date()}_{datetime.today().time()}"

statline_parameters = {
    "ids": DATA,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
    # "config": CONFIG,
}

if __name__ == "__main__":
    # cluster = LocalCluster(host=IP)
    # dask_client = DaskClient(cluster)

    prefect_client = PrefectClient()  # Local api key has been stored previously
    prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

    flow_run_id = prefect_client.create_flow_run(
        version_group_id=STATLINE_VERSION_GROUP_ID,
        parameters=statline_parameters,
        run_name=RUN_NAME,
    )
