# Registers a flow that takes different gcs parquet files, and creates a single dataset, where each parquet file is a table.

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

# from tempfile import gettempdir
# from pathlib import Path

from prefect import Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

with Flow("gcs_to_bq") as gcs_to_bq_flow:
    blobs = Parameter("blobs")
    dataset_name = Parameter("dataset_name")
