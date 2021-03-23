# # # the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from prefect import Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

with Flow("cbs-helper") as cbs_helper_flow:

    upload_folder = nlt.create_dir()
    cbs_v3_catalog = nlt.get_from_cbs_url(
        "https://opendata.cbs.nl/ODataCatalog/Tables?$format=json", True
    )
    cbs_v3_catalog_file = nlt.struct_to_parquet(
        struct=cbs_v3_catalog, file_name="cbs_v3_catalog"
    )

    # Upload all relevant tables
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=cbs_v3_catalog_file,
        gcs_folder=unmapped("_catalogs"),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
    )

    ##############
    catalog_urls = Parameter("catalog_urls")
    catalog_names = Parameter("catalog_names")
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)

    catalogs = nlt.get_from_cbs_url.map(url=catalog_urls, get_value_only=unmapped(True))
    catalog_files = nlt.list_of_dicts_to_parquet.map(  # TODO - verify that the order remains intact
        struct=catalogs, file_name=catalog_names
    )
    # catalog_files = list_of_dicts_to_parquet.map(  # TODO - verify that the order remains intact
    #     struct=catalogs, folder_name=unmapped("catalogs"), file_name=catalog_names
    # )
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=catalog_files,
        gcs_folder=unmapped("_catalogs"),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        prod_env=unmapped(prod_env),
    )
    bq_tables = nlt.gcs_to_bq.map(
        gcs_folder=unmapped("_catalogs"),
        dataset_name=unmapped("catalogs"),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        prod_env=unmapped(prod_env),
        upstream_tasks=[gcs_ids],
    )
