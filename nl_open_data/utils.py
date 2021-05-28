from typing import Union, List, Mapping, Sequence
from pathlib import Path

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import exceptions
import google.api_core.exceptions as google_execptions


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

    try:
        path = Path(path)
        if not (path.exists() and path.is_dir()):
            path.mkdir(parents=True)
        return path
    except TypeError as error:
        print(f"Error trying to find {path}: {error!s}")
        return None


def set_gcp(
    config: Mapping, gcp_env: str, source: str = None, prod_env: str = None
) -> Mapping:
    # TODO: Complete docstring
    """Sets the desired GCP donciguration

    Parameters
    ----------
    config : Box
        A box object holding configuration
    gcp_env : str
        String representing the desired environment between ['dev', 'test', 'prod']
    source : str
        The high level source of the dataset ("external" or "cbs")

    Returns
    -------
    A Box object holding GCP Project parameters (project id, bucket)
    """
    config_envs = {
        "dev": config.gcp.dev,
        "test": config.gcp.test,
        "prod": {
            "cbs": config.gcp.prod.cbs_dl,
            "external": config.gcp.prod.external_dl,
            "dwh": config.gcp.prod.dwh,
        },
    }
    gcp_env = gcp_env.lower()
    ## For "dev" or "test" envs
    if gcp_env != "prod":
        return config_envs[gcp_env]
    ## For "prod" env
    elif source is None and prod_env is None:
        raise ValueError(
            "One of 'source' OR 'prod_env' MUST be specified for 'prod' env"
        )
    elif source is not None and prod_env is not None:
        raise ValueError(
            "ONLY ONE of 'source' OR 'prod_env' can be specified for 'prod' env"
        )
    ## If source is specified, inner "prod" selection is automatic
    # currently, only "cbs" and external applies (within "external", different sources are handled later, not here)
    elif source is not None:
        source = source.lower()
        return config_envs[gcp_env][source]
    ## A specific prod_env can also be selected manually
    elif prod_env is not None:
        prod_env = prod_env.lower()
        return config_envs[gcp_env][prod_env]


def check_bq_dataset(dataset_id: str, gcp: Mapping) -> bool:
    """Check if dataset exists in BQ.

    Parameters
    ----------
        - dataset_id : str
            A BQ dataset id
        - gcp: Box
            A box object, holding GCP project parameters

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


def delete_bq_dataset(dataset_id: str, gcp: Mapping) -> None:
    """Delete an exisiting dataset from Google Big Query.

    If dataset does not exists, does nothing.

    Parameters
    ----------
        dataset_id : str
            A BQ dataset id
        gcp : Box
            A Box object, holding GCP project parameters

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
    name: str, gcp: Mapping, source: str = None, description: str = None,
) -> str:
    """Creates a dataset in Google Big Query. If dataset exists already exists, does nothing.

    Parameters
    ----------
    name : str
        The name of the dataset. If no source is given will be used as dataset_id
    gcp : Box
        A Box object, holding GCP project parameters
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
    if source:
        dataset_id = f"{client.project}.{source}_{name}"
    else:
        dataset_id = f"{client.project}.{name}"

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


def link_pq_folder_to_bq_dataset(gcs_folder: str, gcp: Mapping, dataset_id: str):

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


def get_gcs_uris(
    gcs_folder: str, source: str, config: Mapping, gcp_env: str
) -> Sequence:
    """Returns all uris of files in a GCS folder

    Parameters
    ----------
    gcs_folder : str
        folder in GCS
    source : str
        source of files in folder
    config : Mapping
        configuration object containing GCP environments details
    gcp_env : str
        string to determine which GCP environment to use

    Returns
    -------
    Sequence
        [description]
    """
    gcp = set_gcp(config=config, gcp_env=gcp_env, source=source)
    client = storage.Client(project=gcp.project_id)
    blobs = client.list_blobs(gcp.bucket, prefix=gcs_folder)
    uris = ["gs://" + gcp.bucket + "/" + blob.name for blob in blobs]
    return uris


def create_linked_tables(source_uris: List[str], gcp: Mapping, dataset_id: str):
    """Takes a list of GCS uris and creates a linked table per uri nested under the given dataset_id

    Parameters
    ----------
    source_uris : List[str]
        [description]
    gcp : Box
        [description]
    dataset_id : str
        [description]

    Returns
    -------
    [type]
        [description]
    """

    # Initialize client
    bq_client = bigquery.Client(project=gcp.project_id)

    # Initialize the external data source
    dataset_ref = bigquery.DatasetReference(gcp.project_id, dataset_id)
    tables = []
    for uri in source_uris:
        table_id = uri.split("/")[-1].split(".")[-2]
        table = bigquery.Table(dataset_ref.table(table_id))
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [uri]
        table.external_data_configuration = external_config
        try:
            table = bq_client.create_table(table, exists_ok=True)
            tables.append(table)
        except google_execptions.NotFound:
            # TODO: Better handling?
            print(f"URI {uri} Not found")
            pass

    return tables


def query_cbs_catalogs(
    third_party: bool = False, odata_version: str = "v3", source: str = None
) -> dict:
    """Queries dataset ids from CBS catalog and returns a dict with source name as key a list of all related dataset ids as value

    Parameters
    ----------
    third_party : bool, default=False
        Flag to indicate whether to query core or external catatlog
    odata_version : str, default="v3"
        version of the odata for this dataset - must be either "v3" or "v4"
    source : str, default=None
        The source of the dataset to be used as a WHERE caluse. If None, returns results from all sources

    Returns
    -------
    ids : dict
        collection with source name as keys and all dataset ids from source as list
    """

    ## Get all v3 dataset ids #TODO: add v4 support
    bq_client = bigquery.Client()

    # NOTE:
    # CBS provides both 'Catalog' and 'Source' fields. Both provide mostly similar, but not identical information.
    # 'Catalog' seems more strictly defined and additionally 'Source' is not present in the v4 catalog,
    # so we choose to use 'Catalog' for now, but that could change.

    if third_party:
        select_string = f"""
            SELECT `Identifier`, `Catalog`
            FROM `dataverbinders-open-dwh.catalogs.external_{odata_version}`
        """
    else:
        select_string = f"""
            SELECT `Identifier`, `Catalog`
            FROM `dataverbinders-open-dwh.catalogs.cbs_{odata_version}`
        """
    if source:
        where_string = f"""
            WHERE LOWER(Catalog)='{source.lower()}'
        """
    else:
        where_string = ""

    query = select_string + "\n" + where_string
    query_job = bq_client.query(query)
    ids = []
    sources = []
    for row in query_job:
        ids.append(row[0])
        sources.append(row[1])

    # Separate to different source
    datasets = dict(zip(ids, sources))
    sources_set = set(sources)

    ids = {}
    for source in sources_set:
        ids[source.lower()] = [k for k, v in datasets.items() if v == source]

    return ids


# if __name__ == "__main__":
#     from nl_open_data.config import config

#     gcp = set_gcp(config, "prod", source="external")
#     gcs_folder = "uwv/open_match_data/20210525"

#     # storage_client = storage.Client(project=gcp.project_id)
#     # blobs = storage_client.list_blobs(gcp.bucket, prefix=gcs_folder)
#     # names = [blob.name for blob in blobs]
#     # print(names)

#     link_pq_folder_to_bq_dataset(
#         gcs_folder=gcs_folder, gcp=gcp, dataset_id="uwv_open_match_data",
#     )

