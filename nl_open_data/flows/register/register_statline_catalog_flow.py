# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from prefect import task, Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

with Flow("statline-catalogs") as st_catalogs_flow:
    catalog_urls = Parameter("catalog_urls")
    catalog_names = Parameter("catalog_names")
    gcp_env = Parameter("gcp_env", default="dev")

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
    )
    bq_tables = nlt.gcs_to_bq.map(
        gcs_folder=unmapped("_catalogs"),
        dataset_name=unmapped("catalogs"),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[gcs_ids],
    )

if __name__ == "__main__":
    # Register flow
    st_catalogs_flow.storage = GCS(
        project="dataverbinders-dev",
        bucket="dataverbinders-dev-prefect",  # TODO: Switch to using config (config.gcp.dev.project_id, etc.)
    )
    st_catalogs_flow.run_config = LocalRun(labels=["nl-open-data-preemptible-1"])
    st_catalogs_flow.executor = DaskExecutor(
        # cluster_class="LocalCluster",
        cluster_kwargs={"n_workers": 8},
        # debug=True,
        # processes=True,
        # silence_logs=100, # TODO (?) : find out what the number stands for
    )
    flow_id = st_catalogs_flow.register(
        project_name="nl_open_data", version_group_id="statline_catalogs"
    )
