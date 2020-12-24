from prefect import Flow, unmapped, Parameter
from prefect.tasks.shell import ShellTask
from prefect.triggers import all_finished

import nl_open_data.tasks as nlt

# Allow skipping unzip (if folder already locally unzipped)
nlt.unzip.skip_on_upstream_skip = False
nlt.remove_dir.trigger = all_finished


curl_download = ShellTask(name="curl_download")

with Flow("PC6HUISNR") as zip_flow:

    config = Parameter("config")
    filepath = Parameter("filepath", required=True)
    local_folder = Parameter("local_folder")
    url = Parameter("url")
    csv_delimiter = Parameter("csv_delimiter", default=".")
    gcs_folder = Parameter("gcs_folder")
    gcp_env = Parameter("gcp_env", default="dev")
    dataset_name = Parameter("dataset_name")
    dataset_description = Parameter("dataset_description", default=None)
    source = Parameter("source", required=False)

    local_dir = nlt.create_dir(local_folder)
    curl_command = nlt.curl_cmd(url, filepath, limit_retries=False)
    curl_download = curl_download(command=curl_command, upstream_tasks=[local_dir])
    unzipped_folder = nlt.unzip(filepath, upstream_tasks=[curl_download])
    csv_files = nlt.list_dir(unzipped_folder, upstream_tasks=[unzipped_folder])
    pq_files = nlt.csv_to_parquet.map(
        csv_files, delimiter=unmapped(csv_delimiter), upstream_tasks=[csv_files]
    )
    gcs_ids = nlt.upload_to_gcs.map(
        to_upload=pq_files,
        gcs_folder=unmapped(gcs_folder),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[pq_files],
    )
    tables = nlt.gcs_to_bq(
        gcs_folder=gcs_folder,
        dataset_name=dataset_name,
        config=config,
        gcp_env=gcp_env,
        source=source,
        upstream_tasks=[gcs_ids],
    )
    nlt.remove_dir(local_dir, upstream_tasks=[gcs_ids])

if __name__ == "__main__":
    from pathlib import Path

    from nl_open_data.config import get_config

    config_file = Path.home() / Path("Projects/nl-open-data/nl_open_data/config.toml")

    config = get_config(config_file)
    URL_PC6HUISNR = (
        "https://www.cbs.nl/-/media/_excel/2019/42/2019-cbs-pc6huisnr20190801_buurt.zip"
    )

    local_folder = (
        Path.home() / config.paths.root / config.paths.temp / config.paths.cbs
    )
    filepath = local_folder / Path(URL_PC6HUISNR.split("/")[-1])
    dataset_name = "buurt_wijk_gemeente_pc"
    gcs_folder = "cbs/" + dataset_name

    state = zip_flow.run(
        parameters={
            "config": config,
            "filepath": filepath,
            "local_folder": local_folder,
            "url": URL_PC6HUISNR,
            "csv_delimiter": ";",
            "gcs_folder": gcs_folder,
            "dataset_name": dataset_name,
            "source": "cbs",
        }
    )
