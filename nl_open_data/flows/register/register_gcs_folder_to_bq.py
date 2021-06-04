# Registers a flow that takes a single gcs folder, and creates a dataset, where each parquet file in the folder is a table.
from prefect import Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

from nl_open_data.config import config
import nl_open_data.tasks as nlt

with Flow("gcs_folder_to_bq_flow") as gcs_folder_to_bq_flow:
    gcs_folder = Parameter("gcs_folder")
    dataset_name = Parameter("dataset_name")
    source = Parameter("source", default=None)
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)
    description = Parameter("description", default="")

    nlt.gcs_folder_to_bq(
        gcs_folder=gcs_folder,
        dataset_name=dataset_name,
        config=config,
        source=source,
        gcp_env=gcp_env,
        prod_env=prod_env,
        description=description,
    )

if __name__ == "__main__":

    ## Register flow
    gcs_folder_to_bq_flow.storage = GCS(
        project=config.gcp.dev.project_id, bucket=f"{config.gcp.dev.bucket}-prefect",
    )
    gcs_folder_to_bq_flow.run_config = LocalRun(labels=["nl-open-data-vm-1"])
    gcs_folder_to_bq_flow.executor = DaskExecutor()
    flow_id = gcs_folder_to_bq_flow.register(
        project_name="nl_open_data", version_group_id="gcs_folder_to_bq"
    )

    # # Run locally
    # GCS_FOLDER = "cbs/kwb"
    # DATASET_NAME = "cbs_kwb"
    # DESCRIPTION = "SOME DESCRIPTION"
    # params = {
    #     "gcs_folder": GCS_FOLDER,
    #     "dataset_name": DATASET_NAME,
    #     "description": DESCRIPTION,
    # }
    # state = gcs_folder_to_bq_flow.run(parameters=params)
