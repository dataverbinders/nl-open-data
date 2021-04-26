# Registers a flow that takes a single gcs folder, and creates a dataset, where each parquet file in the folder is a table.

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from prefect import Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

with Flow("gcs_folder_to_bq_flow") as gcs_folder_to_bq_flow:
    gcs_folder = Parameter("gcs_folder")
    dataset_name = Parameter("dataset_name")
    source = Parameter("source", default=None)
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)

    nlt.gcs_folder_to_bq(
        gcs_folder=gcs_folder,
        dataset_name=dataset_name,
        config=config,
        source=source,
        gcp_env=gcp_env,
        prod_env=prod_env,
    )

if __name__ == "__main__":

    ## Register flow
    #     gcs_folder_to_bq_flow.storage = GCS(
    #         project="dataverbinders-dev",
    #         bucket="dataverbinders-dev-prefect",  # TODO: Switch to using config (config.gcp.dev.project_id, etc.)
    #     )
    #     gcs_folder_to_bq_flow.run_config = LocalRun(labels=["nl-open-data-vm-1"])
    #     gcs_folder_to_bq_flow.executor = DaskExecutor(
    #         # cluster_class="LocalCluster",
    #         cluster_kwargs={"n_workers": 8},
    #         # debug=True,
    #         # processes=True,
    #         # silence_logs=100, # TODO (?) : find out what the number stands for
    #     )
    #     flow_id = gcs_folder_to_bq_flow.register(
    #         project_name="nl_open_data", version_group_id="gcs_folder_to_bq"
    #     )

    # Run locally
    GCS_FOLDER = "cbs/kwb"
    DATASET_NAME = "cbs_kwb"
    params = {"gcs_folder": GCS_FOLDER, "dataset_name": DATASET_NAME}
    state = gcs_folder_to_bq_flow.run(parameters=params)
