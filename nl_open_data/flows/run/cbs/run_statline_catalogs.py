from nl_open_data.config import config
from datetime import datetime
from prefect import Client as PrefectClient
from dask.distributed import Client as DaskClient

# Schedules a flow-run on prefect cloud

# Prefect client parameters
TENANT_SLUG = "dataverbinders"


# flow parameters
CBS_CATALOGS = {
    "cbs_v3": "https://opendata.cbs.nl/ODataCatalog/Tables?$format=json",
    "iv3_v3": "https://dataderden.cbs.nl/ODataCatalog/Tables?$format=json",
    "cbs_v4": "https://odata4.cbs.nl/CBS/Datasets",
}
URLS = list(CBS_CATALOGS.values())
NAMES = list(CBS_CATALOGS.keys())
GCP_ENV = "prod"
PROD_ENV = "dwh"


# run parameters
VERSION_GROUP_ID = "statline_catalogs"
RUN_NAME = f"statline_catalogs_{datetime.today().date()}_{datetime.today().time()}"

parameters = {
    "catalog_urls": URLS,
    "catalog_names": NAMES,
    "gcp_env": GCP_ENV,
    "prod_env": PROD_ENV,
}

if __name__ == "__main__":
    prefect_client = PrefectClient()  # Local api key has been stored previously
    prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

    flow_run_id = prefect_client.create_flow_run(
        version_group_id=VERSION_GROUP_ID, parameters=parameters, run_name=RUN_NAME,
    )
