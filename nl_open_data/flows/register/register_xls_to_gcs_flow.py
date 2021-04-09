# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from pathlib import Path

from prefect import Flow, unmapped, Parameter, task
from prefect.tasks.shell import ShellTask
from prefect.triggers import all_finished, any_successful
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor

import nl_open_data.tasks as nlt

# Prefect flow parameters
PROJECT_NAME = "nl_open_data"
VERSION_GROUP_ID = "xls_to_gcs"

curl_download = ShellTask(name="curl_download")

# Allow skipping download  without skipping rest of flow (if file already exists locally)
nlt.list_dir.skip_on_upstream_skip = False
nlt.xls_to_parquet.skip_on_upstream_skip = False
# Allow some downloads to fail
nlt.list_dir.trigger = any_successful
# Always clean up at end
nlt.remove_dir.trigger = all_finished

with Flow("xls_flow") as xls_flow:

    urls = Parameter("urls")
    gcs_folder = Parameter("gcs_folder")
    gcp_env = Parameter("gcp_env", default="dev")
    prod_env = Parameter("prod_env", default=None)
    read_excel_kwargs = Parameter("read_excel_kwargs", default=None)

    # # For local testing
    # local_folder = nlt.create_dir(Path("." / Path("xls_to_gcs_flow")))
    local_folder = nlt.create_temp_dir("xls_to_gcs_flow")
    upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    xls_folder = nlt.create_dir(local_folder / Path("xls"))
    xls_filenames = nlt.get_filename_from_url.map(urls)
    xls_filepaths = nlt.add_folder_to_filename.map(unmapped(xls_folder), xls_filenames)
    curl_commands = nlt.curl_cmd.map(
        url=urls, filepath=xls_filepaths, limit_retries=unmapped(False),
    )
    download_xls = curl_download.map(
        command=curl_commands,
        upstream_tasks=[unmapped(xls_folder), unmapped(upload_folder)],
    )
    # xls_files = nlt.list_dir(xls_folder, upstream_tasks=[download_xls])
    pq_filenames = nlt.replace_suffix.map(
        filepath=xls_filepaths, new_suffix=unmapped(".parquet")
    )
    pq_filepaths = nlt.create_path.map(pq_filenames, unmapped(upload_folder))
    pq_files = nlt.xls_to_parquet.map(
        file=xls_filepaths,
        out_file=pq_filepaths,
        read_excel_kwargs=read_excel_kwargs,
        # skiprows=unmapped(
        # read_excel_kwargs["skiprows"]
        # ),
        # na_values=unmapped(read_excel_kwargs["na_values"]),
        upstream_tasks=[download_xls],
    )
    clean_files = nlt.clean_file_name.map(pq_files)
    ## To GCS
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=clean_files,
        gcs_folder=unmapped(gcs_folder),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        prod_env=unmapped(prod_env),
    )
    # Clean up
    # nlt.remove_dir(local_folder, upstream_tasks=[gcs_ids])

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
    ###############################################################################
    # Run locally
    # kwb_urls = [
    #     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2013.xls",
    #     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kerncijfers-wijken-en-buurten-2014.xls",
    #     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2015.xls",
    #     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2016.xls",
    #     "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2017.xls",
    #     "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2018.xls",
    #     "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2019.xls",  # BUG: Unexpected error: ArrowInvalid('Could not convert 5,0 with type str: tried to convert to double', 'Conversion failed for column p_stadsv with type object')
    #     "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2020.xls",
    # ]
    # nbh_urls = [
    #     "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2006-2016-04-18.xls",
    #     "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2007-2016-04-18.xls",  # TODO: pd.read_excel ERROR: WARNING *** File is truncated, or OLE2 MSAT is corrupt!!   INFO: Trying to access sector 13103 but only 9751 available
    #     "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2008-2016-04-18.xls",
    #     "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2009-2016-04-18.xls",
    #     "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2010-2016-04-18.xls",
    #     "https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2011-2016-04-18.xls",
    #     "https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2012-2016-04-18.xls",
    #     "https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2013-2016-12-19.xls",
    #     "https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2014-2016-10-11-(1).xls",
    #     "https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_wijkbuurt_2015v3.xls",
    #     "https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_2016.xls",
    # ]

    # URLS = kwb_urls
    # GCS_FOLDER = "cbs/kwb"
    # # KWARGS = {"skiprows": None, "na_values": [".", "        .", "        .       "]}
    # KWARGS = [
    #     {"na_values": [".", "        .", "        .       "]},
    #     {"na_values": [".", "        .", "        .       "]},
    #     {"na_values": [".", "        .", "        .       "]},
    #     {"na_values": [".", "        .", "        .       "]},
    #     {"na_values": [".", "        .", "        .       "]},
    #     {"na_values": [".", "        .", "        .       "]},
    #     {"na_values": [".", "        .", "        .       "]},
    #     {"na_values": [".", "        .", "        .       "]},
    # ]

    # URLS = nbh_urls[:1]
    # GCS_FOLDER = "cbs/nbh"
    # KWARGS = {"skiprows": [1, 2], "na_values": [".", "        .", "        .       "]}

    # # params = {"urls": URLS, "gcp_env": "dev", "prod_env": None}
    # # params = {"urls": URLS, "output_file_name": "cbs.kwb"}
    # params = {"urls": URLS, "gcs_folder": GCS_FOLDER}
    # params = {"urls": URLS, "gcs_folder": GCS_FOLDER, "read_excel_kwargs": KWARGS}
    # state = xls_flow.run(parameters=params)
    # ref = xls_flow.get_tasks()
