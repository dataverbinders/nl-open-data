from typing import Union
from pathlib import Path

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import exceptions

from nl_open_data.config import Config, GcpProject


def create_dir_util(path: Union[Path, str]) -> Path:
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
    print()
    print()
    print("IN CREATE_DIR_UTIL FUNCTION")
    print()
    print()
    return path
    # try:
    #     path = Path(path)
    #     if not (path.exists() and path.is_dir()):
    #         path.mkdir(parents=True)
    #     return path
    # except TypeError as error:
    #     print(f"Error trying to find {path}: {error!s}")
    #     return None


def set_gcp(config: Config, gcp_env: str) -> GcpProject:
    gcp_env = gcp_env.lower()
    config_envs = {
        "dev": config.gcp.dev,
        "test": config.gcp.test,
        "prod": config.gcp.prod,
    }
    return config_envs[gcp_env]


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
