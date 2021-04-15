"""Use statline-bq flow to upload the following datasets to GBQ:

TODO: Add docstring?

[^mlz]: https://mlzopendata.cbs.nl/#/MLZ/nl/
"""
from nl_open_data.config import config
from datetime import datetime
from prefect import Client as PrefectClient

# client parameters
TENANT_SLUG = "dataverbinders"

# flow parameters
ODATA_MLZ = ["40060NED", "40061NED"]
SOURCE = "mlz"
THIRD_PARTY = True
GCP_ENV = "dev"
FORCE = True
CONFIG = config

# run parameters
VERSION_GROUP_ID = "statline_bq"
RUN_NAME = f"mlz_{datetime.today().date()}_{datetime.today().time()}"

if __name__ == "__main__":
    prefect_client = PrefectClient()  # Local api key has been stored previously
    prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token
    parameters = {
        "ids": ODATA_MLZ,
        "source": SOURCE,
        "third_party": THIRD_PARTY,
        "config": CONFIG,  # TODO: Why does this create an error?
        "gcp_env": GCP_ENV,
        "force": FORCE,
    }
    flow_run_id = prefect_client.create_flow_run(
        version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=parameters
    )
