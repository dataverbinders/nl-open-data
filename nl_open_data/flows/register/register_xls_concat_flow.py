# %%
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

with Flow("xls_concat_flow") as xls_concat_flow:

    # gcp_env = Parameter("gcp_env", default="dev")
    # prod_env = Parameter("prod_env")
    output_file_name = Parameter("output_file_name")
    urls = Parameter("urls")

    # TODO: LOCAL TEST REMOVE WHEN DONE
    local_folder = nlt.create_dir(Path("." / Path("tmp_gcs_helper")))
    # local_folder = nlt.create_dir(Path(gettempdir() / Path("tmp_gcs_helper")))
    parquet_folder = nlt.create_dir(local_folder / Path("parquet"))
    upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    xls_folder = nlt.create_dir(local_folder / Path("xls"))
    xls_filenames = nlt.get_filename_from_url.map(urls)
    # xls_filepaths = task(lambda name: xls_folder / Path(name.split("/")[-1])).map(
    #     xls_filenames
    # )

    xls_filepaths = nlt.add_folder_to_filename.map(unmapped(xls_folder), xls_filenames)
    # xls_filepaths = nlt.list_comprehension(
    #     xls_filenames,
    #     lambda name: f"{xls_folder} / {name.split('/')[-1]}"
    #     # xls_filenames, lambda name: xls_folder / Path(name.split("/")[-1])
    # )
    # xls_filepaths = [xls_folder / Path(url.split("/")[-1]) for url in xls_filenames]

    pq_filenames = nlt.replace_suffix.map(
        filepath=xls_filepaths, new_suffix=unmapped(".parquet")
    )
    pq_filepaths = nlt.create_path.map(pq_filenames, unmapped(parquet_folder))
    curl_commands = nlt.curl_cmd.map(urls, xls_filepaths, unmapped(False))
    download_xls = curl_download.map(
        command=curl_commands,
        upstream_tasks=[],
        # upstream_tasks=[xls_folder, parquet_folder], #TODO: setting upstream tasks fails the task (does not even map). WHY? Is it a problem?
    )

    xls_files = nlt.list_dir(xls_folder, upstream_tasks=[download_xls])
    # pq_files = nlt.xls_to_parquet.map(
    #     xls_files, pq_filepaths, upstream_tasks=[xls_files]
    # )
    # out_file_path = upload_folder / Path(str(output_file_name))
    # combined_file = nlt.concat_parquet_files(
    #     pq_files, upload_folder, output_file_name
    # )  # TODO: Schema is different

    # ## To GCS #TODO
    # gcs_ids = nlt.upload_to_gcs.map(
    #     to_upload=cbs_v3_catalog_file,
    #     gcs_folder=unmapped("_catalogs"),
    #     config=unmapped(config),
    #     gcp_env=unmapped(gcp_env),
    # )

    ## Clean up

# %%
if __name__ == "__main__":
    kwb_urls = [
        "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2013.xls",
        "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kerncijfers-wijken-en-buurten-2014.xls",
        "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2015.xls",
        "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2016.xls",
        "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2017.xls",
        "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2018.xls",
        "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2019.xls",
        "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2020.xls",
    ]
    # URLS = kwb_urls
    URLS = kwb_urls
    # params = {"urls": URLS, "gcp_env": "dev", "prod_env": None}
    # params = {"urls": URLS, "output_file_name": "cbs.kwb"}
    params = {"urls": URLS}
    state = xls_concat_flow.run(parameters=params)
    ref = xls_concat_flow.get_tasks()

# %%
