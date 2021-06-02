# Registers a flow that takes different gcs parquet files, and creates a single dataset, where each parquet file is a table.

from prefect import Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

from nl_open_data.config import config as CONFIG
import nl_open_data.tasks as nlt

with Flow("gcs_to_bq") as gcs_to_bq_flow:
    """A flow to create a BQ dataset from a list of GCS uris to parquet files.

    This flows takes in a list of GS uris, assumes that each uri points at a parquet file,
    and creates a new BQ dataset, where each uri is an external table in the dataset.
    
    uris : list[str]
        List of GS uris to parquet files
    dataset_name : str
        The dataset name to use when creating in BQ
    config : Config object
        Config object holding GCP and local paths.
    gcp_env : str
        Determines which GCP environment to use from config.gcp
    prod_env : str
        If gcp_env = "prod", determines which GCP environemnt to use from config.gcp.prod
    description : str
        The dataset description to use when creating in BQ
    """
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
