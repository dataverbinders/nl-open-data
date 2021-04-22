"""Use statline-bq flow to all MLZ datasets to GBQ

TODO: Add docstring?

[^mlz]: https://mlzopendata.cbs.nl/#/MLZ/nl/
"""
## the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config
from datetime import datetime

from prefect import Client as PrefectClient

from nl_open_data.utils import query_cbs_catalogs

# client parameters
TENANT_SLUG = "dataverbinders"

# flow parameters
SOURCE = "mlz"
THIRD_PARTY = True
GCP_ENV = "prod"
FORCE = False
CONFIG = config
ODATA_MLZ = query_cbs_catalogs(third_party=THIRD_PARTY, source=SOURCE)[SOURCE]

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
        # "config": CONFIG, #BUG
        "gcp_env": GCP_ENV,
        "force": FORCE,
    }
    flow_run_id = prefect_client.create_flow_run(
        version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=parameters
    )
