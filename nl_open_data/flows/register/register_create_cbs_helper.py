# %%
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

with Flow("cbs-helper") as cbs_helper_flow:

    # gcp_env = Parameter("gcp_env", default="dev")
    # prod_env = Parameter("prod_env", default="dwh")

    ## Staging locally

    # local_folder = nlt.create_dir(Path(gettempdir() / Path("tmp_gcs_helper")))
    # upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    # xls_folder = nlt.create_dir(local_folder / Path("xls"))

    local_folder = nlt.create_dir(Path("." / Path("tmp_gcs_helper")))
    upload_folder = nlt.create_dir(local_folder / Path("upload_to_gcs"))
    xls_folder = nlt.create_dir(local_folder / Path("xls"))

    # CBS catalog (v3)
    cbs_v3_catalog = nlt.get_from_cbs_url(
        url="https://opendata.cbs.nl/ODataCatalog/Tables?$format=json",
        get_value_only=True,
    )
    cbs_v3_catalog_file = nlt.struct_to_parquet(
        struct=cbs_v3_catalog, file_name="cbs_v3_catalog", folder_name=upload_folder
    )

    # Kerncijfers wijken and buurten
    # Taking 2013-2020
    # Taking 2013 here, because earlier data has different format, so we leave integration of those for later. #TODO
    # https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2020
    kwb_years = range(2013, 2021)
    kwb_urls = [
        f"https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-{year}.xls"
        for year in kwb_years
    ]
    https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2006-2016-04-18.xls
    xls_filepaths = [xls_folder / Path(url.split("/")[-1]) for url in kwb_urls]
    pq_filenames = nlt.replace_suffix.map(
        filepath=xls_filepaths, new_suffix=unmapped(".parquet")
    )
    pq_filepaths = nlt.create_path.map(pq_filenames, unmapped(upload_folder))
    curl_commands = nlt.curl_cmd.map(
        kwb_urls, xls_filepaths, limit_retries=unmapped(False)
    )
    download_xls = curl_download.map(
        command=curl_commands
        # command=curl_commands, upstream_tasks=[xls_folder, upload_folder]
    )
    xls_files = nlt.list_dir(xls_folder, upstream_tasks=[download_xls])
    pq_files = nlt.xls_to_parquet.map(
        xls_files, pq_filepaths, upstream_tasks=[xls_files]
    )
    #TODO: Combine into one table??
    
    # Nabijheidsstatistieken

    ##########
    # 2006-2016 figures are excel files
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2006-2016-04-18.xls"
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2007-2016-04-18.xls"
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2008-2016-04-18.xls"
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2009-2016-04-18.xls"
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2010-2016-04-18.xls"
    "https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2011-2016-04-18.xls"
    "https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2012-2016-04-18.xls"
    "https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2013-2016-12-19.xls"
    "https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2014-2016-10-11-(1).xls"
    "https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_wijkbuurt_2015v3.xls"
    "https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_2016.xls"
    # 2017 onwards in datasets:
    "84334NED"
    "84463NED"
    "84718NED"
    ###########


    nbh_years = range(2006, 2021)
    nbh_urls = [
        f"https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2006-2016-04-18.xls"
        f"https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-{year}.xls"
        for year in nbh_years
    ]

    # Bevolkingsstatistieken per pc4

    # Mapping pc6huisnummer tot buurten and wijken

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
    params = {"gcp_env": "dev", "prod_env": None}
    state = cbs_helper_flow.run(parameters=params)
