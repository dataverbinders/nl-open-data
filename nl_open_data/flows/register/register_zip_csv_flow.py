"""Registering a Prefect Flow downloading a zipped folder containing csv files.
"""
# the config object must be imported from config.py before any Prefect imports
from pathlib import Path

from prefect import Flow, unmapped, Parameter, flatten
from prefect.tasks.shell import ShellTask
from prefect.triggers import all_finished
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

from nl_open_data.config import config
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
    """a Prefect Flow downloading a zipped folder containing csv files.
    
    This flow takes a list of urls, and assumes each url points to a zip file. It further assumes
    the zip file contains one or more csv files, in an arbitrarly deep folder.

    The csv files are converted to parquet files, and uploaded to GCS, into a single specific folder.
    In other words - this flow should be used to process multiple csv files all pertaining to a single dataset.

    Parameters
    ----------
    urls : str
        The urls of the zipped files
    csv_delimiter : str, default = ","
        The delimiter used in the zipped csv files
    csv_encoding : str, default="utf-8"
        The encoding of the csv files
    gcs_folder : str
        The gcs_folder to upload the table into
    gcp_env : str
        Determines which GCP environment to use from config.gcp
    prod_env : str
        If gcp_env = "prod", determines which GCP environemnt to use from config.gcp.prod
    """

    urls = Parameter("urls")
    csv_delimiter = Parameter("csv_delimiter", default=",")
    encoding = Parameter("encoding", default="utf-8")
    gcs_folder = Parameter("gcs_folder")
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)
    # For local testing
    # local_folder = nlt.create_dir(Path("." / Path("zipped_csv_flow")))
    local_folder = nlt.create_temp_dir("zipped_csv_flow")
    zip_filenames = nlt.get_filename_from_url.map(urls)
    download_folder = nlt.create_dir(local_folder / Path("download"))
    unzip_folder = nlt.create_dir(local_folder / Path("unzipped"))
    unzip_folder_names = nlt.stem_wrap.map(zip_filenames)
    unzip_folder_paths = nlt.create_path.map(unmapped(unzip_folder), unzip_folder_names)
    unzip_folders = nlt.create_dir.map(unzip_folder_paths)
    upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    upload_folder_paths = nlt.create_path.map(
        unmapped(upload_folder), unzip_folder_names
    )
    upload_folders = nlt.create_dir.map(upload_folder_paths)
    zip_filepaths = nlt.create_path.map(unmapped(download_folder), zip_filenames)
    curl_commands = nlt.curl_cmd.map(urls, zip_filepaths, limit_retries=unmapped(False))
    curl_downloads = curl_download.map(
        command=curl_commands, upstream_tasks=[unmapped(download_folder)]
    )
    unzipped_folders = nlt.unzip.map(
        zip_filepaths, out_folder=unzip_folders, upstream_tasks=[curl_downloads],
    )
    csv_files = nlt.list_dir.map(
        folder=unzipped_folders,
        suffix=unmapped(".csv"),
        upstream_tasks=[unzipped_folders],
    )
    pq_filenames = nlt.replace_suffix.map(
        filepath=flatten(csv_files), new_suffix=unmapped(".parquet")
    )
    pq_filepaths = nlt.create_path.map(upload_folders, pq_filenames)
    pq_files = nlt.csv_to_parquet.map(
        file=flatten(csv_files),
        out_file=pq_filepaths,
        delimiter=unmapped(csv_delimiter),
        encoding=unmapped(encoding),
    )
    clean_files = nlt.clean_file_name.map(pq_files)
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=clean_files,
        gcs_folder=unmapped(gcs_folder),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        prod_env=unmapped(prod_env),
    )
    nlt.remove_dir(local_folder, upstream_tasks=[gcs_ids])

zip_flow.set_reference_tasks([gcs_ids])

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
    # # flow parameters
    # SOURCE = "uwv"
    # # URL_PC6HUISNR = [
    # #     "https://www.cbs.nl/-/media/_excel/2019/42/2019-cbs-pc6huisnr20190801_buurt.zip"
    # # ]
    # UWV_URLS_SAMPLE = [
    #     "https://data.overheid.nl/sites/default/files/dataset/92f2ea2e-2490-49e0-ad4b-cb48e1ba6840/resources/UWVopenmatch%2020191126.zip",
    #     "https://data.overheid.nl/sites/default/files/dataset/92f2ea2e-2490-49e0-ad4b-cb48e1ba6840/resources/UWVopenmatch%2020191203.zip",
    # ]
    # CSV_DELIMITER = ";"
    # ENCODING = "8859"
    # # BQ_DATASET_NAME = "buurt_wijk_gemeente_pc"
    # BQ_DATASET_NAME = "open_match_data"
    # GCS_FOLDER = (
    #     SOURCE
    #     + "/"
    #     + BQ_DATASET_NAME
    #     + "/"
    #     + str(datetime.today().date().strftime("%Y%m%d"))
    # )
    # state = zip_flow.run(
    #     parameters={
    #         "urls": UWV_URLS_SAMPLE,
    #         "csv_delimiter": CSV_DELIMITER,
    #         "encoding": ENCODING,
    #         "gcs_folder": GCS_FOLDER,
    #     }
    # )
    # ref = zip_flow.get_tasks()
