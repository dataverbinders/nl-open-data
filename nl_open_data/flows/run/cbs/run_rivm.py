"""Use statline-bq flow to upload all RIVM datasets to GBQ:

TODO: Add docstring?

[^rivm]: https://statline.rivm.nl/#/RIVM/nl/
"""
## the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config
from datetime import datetime

from prefect import Client

from nl_open_data.utils import query_cbs_catalogs

# client parameters
TENANT_SLUG = "dataverbinders"

# flow parameters
SOURCE = "rivm"
THIRD_PARTY = True
GCP_ENV = "prod"
FORCE = False
CONFIG = config
ODATA_RIVM = query_cbs_catalogs(third_party=THIRD_PARTY, source=SOURCE)[SOURCE]

# run parameters
VERSION_GROUP_ID = "statline_bq"
RUN_NAME = f"rivm_{datetime.today().date()}_{datetime.today().time()}"

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token
parameters = {
    "ids": ODATA_RIVM,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=parameters
)
