# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from tempfile import gettempdir
from pathlib import Path

from prefect import Flow, unmapped, Parameter
from prefect.tasks.shell import ShellTask
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

curl_download = ShellTask(name="curl_download")

with Flow("xls") as xls_flow:

    # gcp_env = Parameter("gcp_env", default="dev")
    # prod_env = Parameter("prod_env")
    urls = Parameter("urls")

    ## Staging locally

    # local_folder = nlt.create_dir(Path(gettempdir() / Path("tmp_gcs_helper")))
    # upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    # xls_folder = nlt.create_dir(local_folder / Path("xls"))

    # TEST #TODO: Remove when done
    local_folder = nlt.create_dir(Path("." / Path("tmp_gcs_helper")))
    upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    xls_folder = nlt.create_dir(local_folder / Path("xls"))

    xls_filepaths = [xls_folder / Path(url.split("/")[-1]) for url in urls]
    pq_filenames = nlt.replace_suffix.map(
        filepath=xls_filepaths, new_suffix=unmapped(".parquet")
    )
    pq_filepaths = nlt.create_path.map(pq_filenames, unmapped(upload_folder))
    curl_commands = nlt.curl_cmd.map(urls, xls_filepaths, limit_retries=unmapped(False))
    download_xls = curl_download.map(
        command=curl_commands
        # command=curl_commands, upstream_tasks=[xls_folder, upload_folder]
    )
    xls_files = nlt.list_dir(xls_folder, upstream_tasks=[download_xls])
    pq_files = nlt.xls_to_parquet.map(
        xls_files, pq_filepaths, upstream_tasks=[xls_files]
    )
    # TODO: Combine into one table??

    # ## To GCS #TODO
    # gcs_ids = nlt.upload_to_gcs.map(
    #     to_upload=cbs_v3_catalog_file,
    #     gcs_folder=unmapped("_catalogs"),
    #     config=unmapped(config),
    #     gcp_env=unmapped(gcp_env),
    # )

    # bq_tables = nlt.gcs_to_bq.map(
    #     gcs_folder=unmapped("_catalogs"),
    #     dataset_name=unmapped("catalogs"),
    #     config=unmapped(config),
    #     gcp_env=unmapped(gcp_env),
    #     prod_env=unmapped(prod_env),
    #     upstream_tasks=[gcs_ids],
    # )

    ## Clean up
# %%

if __name__ == "__main__":

    URLS = [
        f"https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-{year}.xls"
        # for year in range(2013, 2014)
        for year in range(2013, 2021)
    ]
    params = {"urls": URLS, "gcp_env": "dev", "prod_env": None}
    state = xls_flow.run(parameters=params)
