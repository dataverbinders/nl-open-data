from nl_open_data.config import config
from datetime import datetime
from prefect import Client

# Schedules a flow-run on prefect cloud

# client parameters
TENANT_SLUG = "dataverbinders"

# flow parameters
DATA = ["83583NED"]
SOURCE = "cbs"
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = True

# run parameters
STATLINE_VERSION_GROUP_ID = "statline_bq"
RUN_NAME = f"test_statline-bq_{datetime.today().date()}_{datetime.today().time()}"

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

statline_parameters = {
    "ids": DATA,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}
flow_run_id = client.create_flow_run(
    version_group_id=STATLINE_VERSION_GROUP_ID, parameters=statline_parameters
)
