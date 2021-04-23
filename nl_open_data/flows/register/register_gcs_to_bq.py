# Registers a flow that takes different gcs parquet files, and creates a single dataset, where each parquet file is a table.

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config as CONFIG

from prefect import Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

with Flow("gcs_to_bq") as gcs_to_bq_flow:
    uris = Parameter("uris")
    dataset_name = Parameter("dataset_name")
    config = Parameter("config", default=CONFIG)
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)
    description = Parameter("description", default="")

    nlt.create_linked_dataset(
        dataset_name=unmapped(dataset_name),
        gcs_uris=unmapped(uris),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        prod_env=unmapped(prod_env),
        description=unmapped(description),
    )

if __name__ == "__main__":

    # Register flow
    gcs_to_bq_flow.storage = GCS(
        project=CONFIG.gcp.dev.project_id, bucket=f"{CONFIG.gcp.dev.bucket}-prefect",
    )
    gcs_to_bq_flow.run_config = LocalRun(labels=["nl-open-data-vm-1"])
    gcs_to_bq_flow.executor = DaskExecutor()
    flow_id = gcs_to_bq_flow.register(
        project_name="nl_open_data", version_group_id="gcs_to_bq"
    )

    # # Run locally
    # URIS = [
    #     "gs://dataverbinders-dev/cbs/kwb/kwb_2013.parquet",
    #     "gs://dataverbinders-dev/cbs/kwb/kerncijfers_wijken_en_buurten_2014.parquet",
    # ]
    # DATASET_NAME = "CBS_KWB_TEST"
    # DATASET_DESCRIPTION = "MY DESCRIPTION TEXT"

    # params = {
    #     "uris": URIS,
    #     "dataset_name": DATASET_NAME,
    #     "description": DATASET_DESCRIPTION,
    # }
    # state = gcs_to_bq_flow.run(parameters=params)
