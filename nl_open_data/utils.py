from nl_open_data.config import Config, GcpProject
from typing import Union
from pathlib import Path, PurePath
import os
from shutil import rmtree
from zipfile import ZipFile

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import exceptions
from pyarrow import csv
import pyarrow.parquet as pq

from prefect.engine.signals import SKIP


def remove_dir(path: Union[str, Path]) -> None:
    try:
        rmtree(Path(path))
    except:
        pass
    return None


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


def set_gcp(config: Config, gcp_env: str) -> GcpProject:
    gcp_env = gcp_env.lower()
    config_envs = {
        "dev": config.gcp.dev,
        "test": config.gcp.test,
        "prod": config.gcp.prod,
    }
    return config_envs[gcp_env]


def curl_cmd(url: str, filepath: Union[str, Path], **kwargs) -> str:
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
    return f"curl -fL -o {filepath} {url}"


def unzip(zipfile: Union[Path, str], out_folder: Union[Path, str] = None):
    if out_folder is not None:
        out_folder = Path(out_folder)
    else:
        zipfile = Path(zipfile)
        out_folder = zipfile.parents[0] / zipfile.stem

    out_folder = create_dir(out_folder)

    with ZipFile(zipfile, "r") as zipfile:
        zipfile.extractall(out_folder)
    return out_folder


def list_dir(dir: Union[Path, str]):
    full_paths = [Path(dir) / file for file in os.listdir(dir)]
    return full_paths


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
            folder = create_dir(file.parents[0] / "parquet")
            out_file = folder / (file.stem + ".parquet")
            # out_file = Path("".join(str(file).split(".")[:-1]) + ".parquet")
        table = csv.read_csv(file, parse_options=csv.ParseOptions(delimiter=delimiter))
        pq.write_table(table, out_file)  # TODO -> set proper data types in parquet file
        return out_file

    # # If given a zip file with multiple csvs
    # if file.suffix == ".zip":

    #     if out_folder is not None:
    #         out_folder = Path(out_folder)
    #     else:
    #         out_folder = file.parents[0] / file.stem

    #     out_folder = create_dir(out_folder)
    #     # csv_dir = create_dir(out_folder / "csv")

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


def upload_to_gcs(
    to_upload: Union[str, Path], gcs_folder: str, config: Config, gcp_env: str = "dev",
) -> list:

    to_upload = Path(to_upload)

    # Set GCP params
    gcp = set_gcp(config=config, gcp_env=gcp_env)
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


def check_bq_dataset(dataset_id: str, gcp: GcpProject) -> bool:
    """Check if dataset exists in BQ.

    Parameters
    ----------
        - dataset_id : str
            A BQ dataset id
        - gcp: GcpProject
            An `nl_open_data.config.GcpProject` object, holding GCP project parameters

    Returns
    -------
        - True if exists, False if does not exists
    """

    client = bigquery.Client(project=gcp.project_id)

    try:
        client.get_dataset(dataset_id)  # Make an API request.
        return True
    except exceptions.NotFound:
        return False


def delete_bq_dataset(dataset_id: str, gcp: GcpProject) -> None:
    """Delete an exisiting dataset from Google Big Query.

    If dataset does not exists, does nothing.

    Parameters
    ----------
        dataset_id : str
            A BQ dataset id
        gcp : GcpProject
            An `nl_open_data.config.GcpProject` object, holding GCP project parameters

    Returns
    -------
        None
    """

    # Construct a bq client
    client = bigquery.Client(project=gcp.project_id)

    # Delete the dataset and its contents
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    return None


def create_bq_dataset(
    name: str, gcp: GcpProject, source: str = None, description: str = None,
) -> str:
    """Creates a dataset in Google Big Query. If dataset exists already exists, does nothing.

    Parameters
    ----------
    name : str
        The name of the dataset. If no source is given will be used as dataset_id
    gcp : GcpProject
        An `nl_open_data.config.GcpProject` object, holding GCP project parameters
    description : str, default = None
        The description of the dataset
    source: str, default = None
        The source of the dataset. If given, dataset_id will be {source}_{name}


    Returns:
    dataset.dataset_id: str
        The id of the created BQ dataset
    """

    # Construct a BigQuery client object.
    client = bigquery.Client(project=gcp.project_id)

    # Set dataset_id to the ID of the dataset to create.
    dataset_id = f"{client.project}.{source}_{name}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = gcp.location

    # Add description if provided
    dataset.description = description

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    try:
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print(f"Created dataset {client.project}.{dataset.dataset_id}")
    except exceptions.Conflict:
        print(f"Dataset {client.project}.{dataset.dataset_id} already exists")
    finally:
        return dataset.dataset_id


def link_parquet_to_bq_dataset(gcs_folder: str, gcp: GcpProject, dataset_id: str):

    # Get blobs within gcs_folder
    storage_client = storage.Client(project=gcp.project_id)
    blobs = storage_client.list_blobs(gcp.bucket, prefix=gcs_folder)
    names = [blob.name for blob in blobs]

    # Initialize client
    bq_client = bigquery.Client(project=gcp.project_id)

    # Configure the external data source
    dataset_ref = bigquery.DatasetReference(gcp.project_id, dataset_id)

    tables = []
    # Loop over all Parquet files in GCS Folder
    for name in names:
        if name.split(".")[-1] == "parquet":
            # Configure the external data source per table id
            table_id = name.split("/")[-1].split(".")[-2]
            table = bigquery.Table(dataset_ref.table(table_id))

            external_config = bigquery.ExternalConfig("PARQUET")
            external_config.source_uris = [
                f"https://storage.cloud.google.com/{gcp.bucket}/{name}"
            ]
            table.external_data_configuration = external_config
            # table.description = description

            # Create a permanent table linked to the GCS file
            table = bq_client.create_table(table, exists_ok=True)
            tables.append(table)

    return tables


def gcs_to_bq(
    gcs_folder: str,
    dataset_name: str,
    config: Config = None,
    gcp_env: str = "dev",
    **kwargs,
):
    gcp = set_gcp(config=config, gcp_env=gcp_env)

    # If source was given through kwargs, use to cunstruct full dataset_id
    try:
        dataset_id = f"{kwargs['source']}_{dataset_name}"
    except KeyError:
        dataset_id = dataset_name

    # Check if dataset exists and delete if it does TODO: maybe delete anyway (deleting currently uses not_found_ok to ignore error if does not exist)
    if check_bq_dataset(dataset_id=dataset_id, gcp=gcp):
        delete_bq_dataset(dataset_id=dataset_id, gcp=gcp)

    # Create dataset and reset dataset_id to new dataset
    dataset_id = create_bq_dataset(name=dataset_name, gcp=gcp, **kwargs)

    # Link parquet files in GCS to tables in BQ dataset
    tables = link_parquet_to_bq_dataset(
        gcs_folder=gcs_folder, gcp=gcp, dataset_id=dataset_id
    )

    return tables
