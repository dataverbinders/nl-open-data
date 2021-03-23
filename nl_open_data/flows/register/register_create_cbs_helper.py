# # # the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from prefect import Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

with Flow("cbs-helper") as cbs_helper_flow:

    cbs_v3_catalog = nlt.get_from_cbs_url(
        "https://opendata.cbs.nl/ODataCatalog/Tables?$format=json", True
    )
    cbs_v3_catalog_file = nlt.list_of_dicts_to_parquet(
        struct=cbs_v3_catalog, file_name="cbs_v3_catalog"
    )
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=cbs_v3_catalog_file,
        gcs_folder=unmapped("_catalogs"),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
    )
