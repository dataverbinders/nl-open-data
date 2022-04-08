from typing import Union, Mapping, Sequence, Dict
from pathlib import Path
import os
from shutil import rmtree
from tempfile import gettempdir, mkdtemp
from zipfile import ZipFile
import requests

from box import Box
from google.cloud import storage
import pandas as pd
from pyarrow import Table as PA_Table
from pyarrow import csv
import pyarrow.parquet as pq
from prefect import task, case
from prefect.tasks.control_flow import merge
from prefect.engine.signals import SKIP

import nl_open_data.utils as nlu


@task
def dict_to_box(dict: Dict, frozen_box: bool = False):
    return Box(dict, frozen_box=frozen_box)


@task
def same_path(path):
    return path


@task
def path_wrap(string):
    return Path(string)


@task
def stem_wrap(p: Union[Path, str]):
    return Path(p).stem


@task
def suffix_wrap(p: Union[Path, str]):
    return Path(p).suffix


@task
def relative_to_wrap(p: Union[Path, str], relative_to: Union[Path, str]):
    return Path(p).relative_to(Path(relative_to))


@task
def get_parent(dir: Union[str, Path], level: int = 1):
    dir = Path(dir)
    for _ in range(level):
        dir = dir.parent
    return dir


def list_comprehension(original_list, lambda_func):
    return [lambda_func(x) for x in original_list]


@task
def add_folder_to_filename(folder: Union[str, Path], filename: Union[str, Path]):
    return Path(folder) / Path(filename)


@task
def get_filename_from_url(url):
    return url.split("/")[-1]


@task
def upper(string):
    return string.upper()


@task
def lower(string):
    return string.lower()


@task
def skip_task(x):
    if x:
        raise SKIP
    else:
        return None


@task
def get_wrap(obj, key):
    return obj.get(key)


@task
def clean_folder_names(folder: Union[str, Path], extra_chars: str = ""):
    folder = Path(folder)
    for p in folder.rglob("*"):
        no_suffix = str(p.parents[0] / p.stem)
        clean_path = nlu.clean_string(no_suffix, extra_chars) + p.suffix
        p.rename(clean_path)
    return folder


# @task
# def clean_file_name(file: Union[str, Path], chars: str = None) -> Path:
#     """Renames files by replacing certain characters from the filename with an underscore.

#     By default, replaces all occurrences of: hyphen, dot or parentheses.
#     To replace different characters, provide a single string with desired characters.

#     Original filepath must exist.

#     Parameters
#     ----------
#     file : Union[str, Path]
#         The file to clean its name.
#     extra_chars : str, optional
#         Additional characters to replace.

#     Returns
#     -------
#     Path
#         The filepath to the renamed file.

#     Examples
#     --------
#     >>> path = "/some-folder/another.folder/some-file$@(1).txt"
#     >>> new_path = clean_file_name(path)
#     >>> new_path
#     PosixPath('/some-folder/another.folder/some_file$@_1_.txt')
#     >>> special_chars = "-()@$"
#     >>> special_new_path = clean_file_name(path, special_chars)
#     PosixPath('/some-folder/another.folder/some_file___1_.txt')
#     """
#     path = Path(file)
#     if not chars:
#         chars = "-.()%"
#     # new_stem = path.stem
#     for char in chars:
#         new_path = path.replace(char, "_")
#         # new_stem = new_stem.replace(char, "_")
#     # new_path = path.parent / Path(new_stem + path.suffix)
#     new_path = path.rename(new_path)
#     return new_path


@task
def remove_dir(path: Union[str, Path]) -> None:
    if path:
        try:
            rmtree(Path(path))
        except FileNotFoundError:
            pass
    return None


@task
def create_temp_dir(name: str) -> Path:
    """Creates a dir in the local temp folder

    Parameters
    ----------
    name: str
        Name of the folder to be created

    Returns
    -------
    path: Path
        The path to the temp folder.
    """

    try:
        path = Path(mkdtemp(prefix=name))
        # path = Path(gettempdir() / Path(name))
        if not (path.exists() and path.is_dir()):
            path.mkdir(parents=True)
        return path
    except TypeError as error:
        print(f"Error trying to find {path}: {error!s}")
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


@task(log_stdout=True)
def curl_cmd(
    url: str,
    filepath: Union[str, Path],
    limit_retries: bool = True,
    std_out: bool = False,
    **kwargs,
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
    cmd = (
        f"curl -fL '{url}' -o '{filepath}'"
        if limit_retries
        else f"curl --max-redirs -1 -fL '{url}' -o '{filepath}'"
    )
    if std_out:
        print(cmd)
    return cmd


@task
def get_from_cbs_url(url: str, get_value_only: bool):
    r = requests.get(url).json()
    if get_value_only:
        return r["value"]
    else:
        return r


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


@task()
def list_dir(folder: Union[Path, str], suffix: str = None):
    folder = Path(folder)
    if not suffix:
        full_paths = [file for file in folder.rglob("*") if file.is_file()]
    else:
        if suffix[0] != ".":
            suffix = "." + suffix
        full_paths = [
            file
            for file in folder.rglob("*")
            if (file.is_file() and file.suffix == suffix)
        ]
    if full_paths:
        return full_paths
    else:
        return None


@task()
def fwf_to_ndjson(
    file: Union[str, Path], out_file: Union[str, Path] = None, **kwargs
) -> Path:
    if not file.suffix == ".txt":
        raise TypeError("Only txt files are allowed")
    if out_file is not None:
        out_file = Path(out_file)
        folder = nlu.create_dir_util(out_file.parents[0])
    else:
        folder = nlu.create_dir_util(file.parents[0] / "json")
        out_file = folder / (file.stem + ".json")
    df = pd.read_fwf(file, **kwargs)
    df.to_json(out_file, orient="records", lines=True)
    os.remove(file)
    return out_file


@task()
def fwf_to_parquet(
    file: Union[str, Path], out_file: Union[str, Path] = None, **kwargs
) -> Path:
    if not file.suffix == ".txt":
        raise TypeError("Only txt files are allowed")
    if out_file is not None:
        out_file = Path(out_file)
        folder = nlu.create_dir_util(out_file.parents[0])
    else:
        folder = nlu.create_dir_util(file.parents[0] / "parquet")
        out_file = folder / (file.stem + ".parquet")
    df = pd.read_fwf(file, **kwargs)
    df.to_parquet(out_file)
    os.remove(file)
    return out_file


@task()
def csv_to_parquet(
    file: Union[str, Path],
    out_file: Union[str, Path] = None,
    # out_folder: Union[str, Path] = None,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> Path:
    file = Path(file)

    if not file.suffix == ".csv":
        raise TypeError("Only csv files are allowed")
    if out_file is not None:
        out_file = Path(out_file)
        folder = nlu.create_dir_util(out_file.parents[0])
    else:
        folder = nlu.create_dir_util(file.parents[0] / "parquet")
        out_file = folder / (file.stem + ".parquet")
    table = csv.read_csv(
        file,
        read_options=csv.ReadOptions(encoding=encoding),
        parse_options=csv.ParseOptions(delimiter=delimiter),
    )
    pq.write_table(table, out_file)  # TODO -> set proper data types in parquet file
    os.remove(file)
    return out_file


@task()
def xls_to_parquet(
    file: Union[str, Path], out_file: Union[str, Path] = None, read_excel_kwargs=None,
) -> Path:

    file = Path(file)

    if file.suffix == ".xls":
        if out_file is not None:
            out_file = Path(out_file)
        else:
            folder = nlu.create_dir_util(file.parent / "parquet")
            out_file = folder / (file.stem + ".parquet")
        if read_excel_kwargs:
            df = pd.read_excel(file, **read_excel_kwargs)
        else:
            df = pd.read_excel(file)
        df.to_parquet(out_file)
        os.remove(file)

        return out_file
    else:
        print(file)
        raise TypeError("Only file extensions '.xls' are allowed")


def convert_files_switch(file, out_file, **kwargs):
    suffix = suffix_wrap(file)
    with case(suffix, ".csv"):
        n_out_file = replace_suffix(out_file, ".parquet")
        out_1 = csv_to_parquet(file, n_out_file, **kwargs)
    with case(suffix, ".txt"):
        out_file = replace_suffix(out_file, ".json")
        out_2 = fwf_to_ndjson(file, out_file)
    return merge(out_1, out_2)


# @task
# def concat_parquet_files(
#     pq_files: List[Union[str, Path]],
#     out_folder: Union[str, Path],
#     out_file: Union[str, Path],
# ) -> Path:
#     tables = [pq.read_table(file) for file in pq_files]
#     full_table = concat_tables(tables)
#     o = pq.write_table(full_table, Path(out_folder) / Path(out_file))

#     return o


@task()
def replace_suffix(filepath: Union[str, Path], new_suffix: str):
    # if isinstance(filepath, list):
    #     return [replace_suffix(Path(path), new_suffix) for path in filepath]
    # else:
    # filepath = Path(filepath)
    return Path(filepath).with_suffix(new_suffix)


@task()
def create_path(left, right):
    return Path(left) / Path(right)


@task
def struct_to_parquet(struct: list, file_name: str, folder_name: str = None):
    df = pd.DataFrame(struct)
    table = PA_Table.from_pandas(df)
    if folder_name:
        pq_file = Path(gettempdir()) / Path(folder_name) / Path(file_name + ".parquet")
    else:
        pq_file = Path(gettempdir()) / Path(file_name + ".parquet")
    with open(pq_file, "wb+") as f:
        pq.write_table(table, f)
    return pq_file


# @task
# def upload_to_gcs(
#     to_upload: Union[str, Path],
#     top_folder: Union[str, Path],
#     config: Box,
#     source: str = None,
#     gcp_env: str = "dev",
#     prod_env: str = None,
# ) -> list:

#     to_upload = Path(to_upload)

#     # Set GCP params
#     gcp = nlu.set_gcp(config=config, gcp_env=gcp_env, source=source, prod_env=prod_env)
#     client = storage.Client(project=gcp.project_id)
#     gcs_bucket = client.get_bucket(gcp.bucket)
#     # List to return blob ids
#     ids = []

#     gcs_blob = gcs_bucket.blob(gcs_folder + "/" + to_upload.name)
#     gcs_blob.upload_from_filename(to_upload)
#     ids.append(gcs_blob.id)

#     return ids


@task
def upload_to_gcs(
    to_upload: Union[str, Path],
    local_parent: Path,
    gcs_folder: str,
    config: Box,
    source: str = None,
    gcp_env: str = "dev",
    prod_env: str = None,
) -> list:

    to_upload = Path(to_upload)

    # Set GCP params
    gcp = nlu.set_gcp(config=config, gcp_env=gcp_env, source=source, prod_env=prod_env)
    gcs_folder = gcs_folder.rstrip("/")
    gcs = storage.Client(project=gcp.project_id)
    gcs_bucket = gcs.get_bucket(gcp.bucket)
    # List to return blob ids
    ids = []
    # Upload file(s)
    # if to_upload.is_dir():
    #     for pfile in os.listdir(to_upload):
    #         gcs_blob = gcs_bucket.blob(gcs_folder + "/" + pfile)
    #         gcs_blob.upload_from_filename(to_upload / pfile)
    #         ids.append(gcs_blob.id)
    # elif to_upload.is_file():
    gcs_blob = gcs_bucket.blob(
        gcs_folder + "/" + str(to_upload.relative_to(local_parent))
    )
    gcs_blob.upload_from_filename(to_upload)
    ids.append(gcs_blob.id)

    return ids


@task
def gcs_folder_to_bq(
    gcs_folder: str,
    dataset_name: str,
    config: Box = None,
    source: str = None,
    gcp_env: str = "dev",
    prod_env: str = None,
    **kwargs,
):
    gcp = nlu.set_gcp(config=config, gcp_env=gcp_env, source=source, prod_env=prod_env)

    # If source was given, use to cunstruct full dataset_id
    dataset_id = f"{source}_{dataset_name}" if source else dataset_name

    # Check if dataset exists and delete if it does TODO: maybe delete anyway (deleting currently uses not_found_ok to ignore error if does not exist)
    if nlu.check_bq_dataset(dataset_id=dataset_id, gcp=gcp):
        nlu.delete_bq_dataset(dataset_id=dataset_id, gcp=gcp)

    # Create dataset and reset dataset_id to new dataset
    dataset_id = nlu.create_bq_dataset(name=dataset_id, gcp=gcp, **kwargs)

    uris = nlu.get_gcs_uris(
        gcs_folder=gcs_folder,
        source=source,
        config=config,
        gcp_env=gcp_env,
        prod_env=prod_env,
    )

    # Link parquet files in GCS to tables in BQ dataset
    tables = nlu.create_linked_tables(uris, gcp, dataset_id)
    # tables = nlu.link_pq_folder_to_bq_dataset(
    #     gcs_folder=gcs_folder, gcp=gcp, dataset_id=dataset_id
    # )

    return tables


@task()
def create_linked_dataset(
    dataset_name: str,
    gcs_uris: list,
    config: Box,
    gcp_env: str = "dev",
    prod_env: str = None,
    **kwargs,
):
    """Creates a BQ dataset and nests tables linked to GCS parquet files.

    Parameters
    ----------
    dataset_name : str
        Name of dataset in BQ
    gcs_uris : list of str
        List contiaing the gsutil URIS to parquet file to be linked as tables
    config : Config
        Config object
    gcp_env: str, default='dev'
        determines which GCP configuration to use from config.gcp. Options: ['dev', 'test', 'prod']
    prod_env : str, default=None
        Determines which production environmnet to use, if using gcp_env='prod'

    Returns
    -------
    tables
        [description]
    """
    gcp = nlu.set_gcp(config=config, gcp_env=gcp_env, prod_env=prod_env)
    dataset_id = dataset_name

    # Check if dataset exists and delete if it does
    # TODO: maybe delete anyway (deleting currently uses not_found_ok to ignore error if does not exist)
    if nlu.check_bq_dataset(dataset_id=dataset_id, gcp=gcp):
        nlu.delete_bq_dataset(dataset_id=dataset_id, gcp=gcp)

    # Create dataset and reset dataset_id to new dataset
    dataset_id = nlu.create_bq_dataset(name=dataset_name, gcp=gcp, **kwargs)

    tables = nlu.create_linked_tables(gcs_uris, gcp, dataset_id)

    return tables
