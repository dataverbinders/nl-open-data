"""Registering a Prefect Flow downloading a zipped folder with csv files.

TODO: Docstring

"""
# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from pathlib import Path

from prefect import Flow, unmapped, Parameter
from prefect.tasks.shell import ShellTask
from prefect.triggers import all_finished
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

# Prefect flow parameters
PROJECT_NAME = "nl_open_data"
VERSION_GROUP_ID = "zipped_csv"

curl_download = ShellTask(name="curl_download")

# Allow skipping unzip (if folder already locally unzipped)
nlt.unzip.skip_on_upstream_skip = False
# Always clean up at end
nlt.remove_dir.trigger = all_finished

with Flow("zipped_csv") as zip_flow:
    """[SUMMARY]

    Parameters
    ----------
    url : str
        The url of the zip file
    csv_delimiter : str
        The delimiter used in the zipped csv files
    gcs_folder : str
        The gcs_folder to upload the table into
    gcp_env : str
        Determines which GCP configuration to use from config.gcp
    bq_dataset_name : str
        The dataset name to use when creating in BQ
    bq_dataset_description : str
        The dataset description to use when creating in BQ
    source : str
        The source of the data, used for naming and folder placements in GCS and BQ
    """

    url = Parameter("url")
    csv_delimiter = Parameter("csv_delimiter", default=".")
    gcs_folder = Parameter("gcs_folder")
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)
    # TODO: move following 3 gcs_to_bq parameters to a separate flow
    bq_dataset_name = Parameter("bq_dataset_name")
    bq_dataset_description = Parameter("bq_dataset_description", default=None)
    source = Parameter("source", required=False)

    # For local testing
    local_folder = nlt.create_dir(Path("." / Path("zipped_csv_flow")))
    # local_folder = nlt.create_temp_dir("zipped_csv_flow")
    upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    download_folder = nlt.create_dir(local_folder / Path("download"))
    csv_folder = nlt.create_dir(local_folder / Path("csv"))
    zip_filename = nlt.get_filename_from_url(url)
    zip_filepath = download_folder / nlt.path_wrap(zip_filename)
    curl_command = nlt.curl_cmd(url, zip_filepath, limit_retries=False)
    curl_download = curl_download(
        command=curl_command, upstream_tasks=[download_folder]
    )
    unzipped_folder = nlt.unzip(
        zip_filepath, out_folder=unmapped(csv_folder), upstream_tasks=[curl_download]
    )
    csv_files = nlt.list_dir(unzipped_folder, upstream_tasks=[unzipped_folder])
    pq_filenames = nlt.replace_suffix.map(
        filepath=csv_files, new_suffix=unmapped(".parquet")
    )
    pq_filepaths = nlt.create_path.map(pq_filenames, unmapped(upload_folder))
    pq_files = nlt.csv_to_parquet.map(
        file=csv_files,
        out_file=pq_filepaths,
        delimiter=unmapped(csv_delimiter),
        # upstream_tasks=[pq_filepaths, csv_files],
    )
    clean_files = nlt.clean_file_name.map(pq_files)
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=clean_files,
        gcs_folder=unmapped(gcs_folder),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        # upstream_tasks=[clean_files],
    )
    tables = nlt.gcs_folder_to_bq(  # TODO: separate to different flow
        gcs_folder=gcs_folder,
        dataset_name=bq_dataset_name,
        config=config,  # TODO: Why is this not unmapped, and it works??
        gcp_env=gcp_env,
        source=source,
        description=bq_dataset_description,
        upstream_tasks=[gcs_ids],
    )
    nlt.remove_dir(local_folder, upstream_tasks=[gcs_ids])

if __name__ == "__main__":
    # Register flow
    zip_flow.storage = GCS(
        project="dataverbinders-dev",
        bucket="dataverbinders-dev-prefect",  # TODO: Switch to using config (config.gcp.dev.project_id, etc.)
    )
    # zip_flow.run_config = LocalRun(labels=["nl-open-data-preemptible-1"])
    zip_flow.run_config = LocalRun(labels=["nl-open-data-vm-1"])
    zip_flow.executor = DaskExecutor(cluster_kwargs={"n_workers": 8},)
    flow_id = zip_flow.register(
        project_name=PROJECT_NAME, version_group_id=VERSION_GROUP_ID
    )

    ###############################################################################

    # # Run Locally
    # # zip_flow.executor = LocalDaskExecutor()
    # # flow parameters
    # SOURCE = "cbs"
    # URL_PC6HUISNR = (
    #     "https://www.cbs.nl/-/media/_excel/2019/42/2019-cbs-pc6huisnr20190801_buurt.zip"
    # )
    # # LOCAL_FOLDER = str(
    # #     Path(__file__).parent / config.paths.temp
    # # )  # TODO: organize better for deployment?
    # CSV_DELIMITER = ";"
    # BQ_DATASET_NAME = "buurt_wijk_gemeente_pc"
    # GCS_FOLDER = SOURCE + "/" + BQ_DATASET_NAME
    # BQ_DATASET_DESCRIPTION = "CBS definitions for geographical division on various granularity levels"  # TODO: Better description
    # zip_flow.run(
    #     parameters={
    #         "url": URL_PC6HUISNR,
    #         "csv_delimiter": CSV_DELIMITER,
    #         "gcs_folder": GCS_FOLDER,
    #         "bq_dataset_name": BQ_DATASET_NAME,
    #         "bq_dataset_description": BQ_DATASET_DESCRIPTION,
    #         "source": SOURCE,
    #     }
    # )
