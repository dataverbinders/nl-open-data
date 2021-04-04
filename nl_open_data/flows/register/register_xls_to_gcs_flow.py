# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from tempfile import gettempdir
from pathlib import Path

from prefect import Flow, unmapped, Parameter, task
from prefect.tasks.shell import ShellTask
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

curl_download = ShellTask(name="curl_download")

PROJECT_NAME = "nl_open_data"
VERSION_GROUP_ID = "xls_to_gcs"

with Flow("xls_flow") as xls_flow:

    urls = Parameter("urls")
    gcs_folder = Parameter("gcs_folder")
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)

    # TODO: LOCAL TEST REMOVE WHEN DONE
    # local_folder = nlt.create_dir(Path("." / Path("tmp_gcs_helper")))
    local_folder = nlt.create_temp_dir("xls_to_gcs_flow")
    upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    xls_folder = nlt.create_dir(local_folder / Path("xls"))
    xls_filenames = nlt.get_filename_from_url.map(urls)
    xls_filepaths = nlt.add_folder_to_filename.map(unmapped(xls_folder), xls_filenames)
    pq_filenames = nlt.replace_suffix.map(
        filepath=xls_filepaths, new_suffix=unmapped(".parquet")
    )
    pq_filepaths = nlt.create_path.map(pq_filenames, unmapped(upload_folder))
    curl_commands = nlt.curl_cmd.map(urls, xls_filepaths, unmapped(False))
    download_xls = curl_download.map(
        command=curl_commands,
        upstream_tasks=[unmapped(xls_folder), unmapped(upload_folder)],
    )
    xls_files = nlt.list_dir(xls_folder, upstream_tasks=[download_xls])
    pq_files = nlt.xls_to_parquet.map(
        xls_files, pq_filepaths, upstream_tasks=[xls_files]
    )

    # ## To GCS
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=pq_files,
        gcs_folder=unmapped(gcs_folder),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        prod_env=unmapped(prod_env),
        upstream_tasks=[pq_files],
    )

    ## Clean up
    nlt.clean_up_dir(local_folder, upstream_tasks=[gcs_ids])

xls_flow.set_reference_tasks([gcs_ids])

if __name__ == "__main__":
    # Register flow
    xls_flow.storage = GCS(
        project="dataverbinders-dev",
        bucket="dataverbinders-dev-prefect",  # TODO: Switch to using config (config.gcp.dev.project_id, etc.)
    )
    # statline_flow.run_config = LocalRun(labels=["nl-open-data-preemptible-1"])
    xls_flow.run_config = LocalRun(labels=["nl-open-data-vm-1"])
    xls_flow.executor = DaskExecutor(
        # cluster_class="LocalCluster",
        cluster_kwargs={"n_workers": 8},
        # debug=True,
        # processes=True,
        # silence_logs=100, # TODO (?) : find out what the number stands for
    )
    flow_id = xls_flow.register(
        project_name=PROJECT_NAME, version_group_id=VERSION_GROUP_ID
    )

# # Run locally
# kwb_urls = [
#     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2013.xls",
#     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kerncijfers-wijken-en-buurten-2014.xls",  # TODO: PyArrow error in conversion to Parquet
#     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2015.xls",
#     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2016.xls",
#     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2017.xls",
#     "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2018.xls",
#     "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2019.xls",
#     "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2020.xls",
# ]

# URLS = kwb_urls[:2]
# GCS_FOLDER = "cbs/kwb"

# # params = {"urls": URLS, "gcp_env": "dev", "prod_env": None}
# # params = {"urls": URLS, "output_file_name": "cbs.kwb"}
# params = {"urls": URLS, "gcs_folder": GCS_FOLDER}
# state = xls_flow.run(parameters=params)
# ref = xls_flow.get_tasks()
