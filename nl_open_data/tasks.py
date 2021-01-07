from typing import Union
from pathlib import Path
import os
from shutil import rmtree
from zipfile import ZipFile

from google.cloud import storage
from pyarrow import csv
import pyarrow.parquet as pq
from prefect.engine.signals import SKIP
from prefect import task
from statline_bq.config import Config

import nl_open_data.utils as nlu


@task
def skip_task(x):
    if x:
        raise SKIP
    else:
        return None


@task
def remove_dir(path: Union[str, Path]) -> None:
    rmtree(Path(path))
    return None


@task
def create_dir(path: Union[Path, str]) -> Path:
    """Checks whether a path exists and is a directory, and creates it if not.

    Parameters
    ----------
    path: Path
        A path to the directory.

    Returns
    -------
    path: Path
        The same input path, to new or existing directory.
    """

    try:
        path = Path(path)
        if not (path.exists() and path.is_dir()):
            path.mkdir(parents=True)
        return path
    except TypeError as error:
        print(f"Error trying to find {path}: {error!s}")
        return None


@task
def curl_cmd(
    url: str, filepath: Union[str, Path], limit_retries: bool = True, **kwargs
) -> str:
    """Template for curl command to download file.

    Uses `curl -fL -o` that fails silently and follows redirects.

    Parameters
    ----------
    url : str
        Url to download
    filepath : str or Path
        File for saving fecthed url
    **kwargs
        Keyword arguments passed to Task constructor

    Returns
    -------
    str
        curl command

    Raises
    ------
    SKIP
        if filepath exists
    
    Example
    -------
    ```
    from pathlib import Path
    
    from prefect import Parameter, Flow
    from prefect.tasks.shell import ShellTask

    curl_download = ShellTask(name='curl_download')
    
    with Flow('test') as flow:
        filepath = Parameter("filepath", required=True)
        curl_command = curl_cmd("https://some/url", filepath)
        curl_download = curl_download(command=curl_command)
    
    flow.run(parameters={'filepath': Path.home() / 'test.zip'})
    ```
    """
    if Path(filepath).exists():
        raise SKIP(f"File {filepath} already exists.")
    return (
        f"curl -fL -o {filepath} {url}"
        if limit_retries
        else f"curl --max-redirs -1 -fL -o {filepath} {url}"
    )


@task
def unzip(zipfile: Union[Path, str], out_folder: Union[Path, str] = None):
    if out_folder is not None:
        out_folder = Path(out_folder)
    else:
        zipfile = Path(zipfile)
        out_folder = zipfile.parents[0] / zipfile.stem

    out_folder = nlu.create_dir_util(out_folder)

    with ZipFile(zipfile, "r") as zipfile:
        zipfile.extractall(out_folder)
    return out_folder


@task
def list_dir(dir: Union[Path, str]):
    full_paths = [Path(dir) / file for file in os.listdir(dir)]
    return full_paths


@task
def csv_to_parquet(
    file: Union[str, Path],
    out_file: Union[str, Path] = None,
    # out_folder: Union[str, Path] = None,
    delimiter: str = ",",
) -> Path:

    file = Path(file)

    if file.suffix == ".csv":
        if out_file is not None:
            out_file = Path(out_file)
        else:
            folder = nlu.create_dir_util(file.parents[0] / "parquet")
            out_file = folder / (file.stem + ".parquet")
            # out_file = Path("".join(str(file).split(".")[:-1]) + ".parquet")
        table = csv.read_csv(file, parse_options=csv.ParseOptions(delimiter=delimiter))
        pq.write_table(table, out_file)  # TODO -> set proper data types in parquet file
        os.remove(file)

        return out_file

    # # If given a zip file with multiple csvs
    # if file.suffix == ".zip":

    #     if out_folder is not None:
    #         out_folder = Path(out_folder)
    #     else:
    #         out_folder = file.parents[0] / file.stem

    #     out_folder = create_dir_fun(out_folder)
    #     # csv_dir = create_dir_fun(out_folder / "csv")

    #     with ZipFile(file, "r") as zipfile:
    #         zipfile.extractall(out_folder)
    #     for csv_file in os.listdir(out_folder):
    #         full_path = Path(os.path.join(out_folder, csv_file))
    #         print()
    #         print(full_path)
    #         print()
    #         pq_file = csv_to_parquet(file=full_path, delimiter=delimiter)
    #         os.remove(full_path)

    #     return out_folder

    else:
        print(file)
        raise TypeError("Only file extensions '.csv' are allowed")

        # raise TypeError("Only file extensions '.csv' and '.zip' are allowed")


@task
def upload_to_gcs(
    to_upload: Union[str, Path], gcs_folder: str, config: Config, gcp_env: str = "dev",
) -> list:

    to_upload = Path(to_upload)

    # Set GCP params
    gcp = nlu.set_gcp(config=config, gcp_env=gcp_env)
    gcs_folder = gcs_folder.rstrip("/")
    gcs = storage.Client(project=gcp.project_id)
    gcs_bucket = gcs.get_bucket(gcp.bucket)
    # List to return blob ids
    ids = []
    # Upload file(s)
    if to_upload.is_dir():
        for pfile in os.listdir(to_upload):
            gcs_blob = gcs_bucket.blob(gcs_folder + "/" + pfile)
            gcs_blob.upload_from_filename(to_upload / pfile)
            ids.append(gcs_blob.id)
    elif to_upload.is_file():
        gcs_blob = gcs_bucket.blob(gcs_folder + "/" + to_upload.name)
        gcs_blob.upload_from_filename(to_upload)
        ids.append(gcs_blob.id)

    return ids


@task
def gcs_to_bq(
    gcs_folder: str,
    dataset_name: str,
    config: Config = None,
    gcp_env: str = "dev",
    **kwargs,
):
    gcp = nlu.set_gcp(config=config, gcp_env=gcp_env)

    # If source was given through kwargs, use to cunstruct full dataset_id
    try:
        dataset_id = f"{kwargs['source']}_{dataset_name}"
    except KeyError:
        dataset_id = dataset_name

    # Check if dataset exists and delete if it does TODO: maybe delete anyway (deleting currently uses not_found_ok to ignore error if does not exist)
    if nlu.check_bq_dataset(dataset_id=dataset_id, gcp=gcp):
        nlu.delete_bq_dataset(dataset_id=dataset_id, gcp=gcp)

    # Create dataset and reset dataset_id to new dataset
    dataset_id = nlu.create_bq_dataset(name=dataset_name, gcp=gcp, **kwargs)

    # Link parquet files in GCS to tables in BQ dataset
    tables = nlu.link_parquet_to_bq_dataset(
        gcs_folder=gcs_folder, gcp=gcp, dataset_id=dataset_id
    )

    return tables
