"""A template of a Prefect flow uploading datasets from statline to Google BigQuery.

A Prefect based equivalent of the standlone `statline_bq.utils.cbsodata_to_gbq()`.

The GCP configuration as well as local paths used for download, should be defined
in 'user_config.toml', which is imported and coupled to the Prefect config object
inside 'config.py'. Therefore, anything that is defined in the 'user_config.toml'
can be accessed by accessing `config`. For example, `config.gcp.dev`.
"""

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from prefect import task, Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor
from statline_bq.utils import (
    check_gcp_env,
    check_v4,
    set_gcp,
    get_urls,
    get_metadata_cbs,
    get_metadata_gcp,
    get_from_meta,
    get_gcp_modified,
    skip_dataset,
    create_named_dir,
    get_main_table_shape,
    tables_to_parquet,
    get_column_descriptions,
    dict_to_json_file,
    upload_to_gcs,
    get_file_names,
    gcs_to_gbq,
    get_col_descs_from_gcs,
    bq_update_main_table_col_descriptions,
)

from nl_open_data.tasks import upper, lower, remove_dir, skip_task

# Converting statline-bq functions to tasks
check_gcp_env = task(check_gcp_env)
check_v4 = task(check_v4)
set_gcp = task(set_gcp)
get_urls = task(get_urls)
get_metadata_cbs = task(get_metadata_cbs)
get_metadata_gcp = task(get_metadata_gcp)
get_from_meta = task(get_from_meta)
get_gcp_modified = task(get_gcp_modified)
skip_dataset = task(skip_dataset)
create_named_dir = task(create_named_dir)
tables_to_parquet = task(tables_to_parquet, log_stdout=True)
get_column_descriptions = task(get_column_descriptions)
dict_to_json_file = task(dict_to_json_file)
get_file_names = task(get_file_names)
upload_to_gcs = task(upload_to_gcs)
gcs_to_gbq = task(gcs_to_gbq)
get_col_descs_from_gcs = task(get_col_descs_from_gcs)
bq_update_main_table_col_descriptions = task(bq_update_main_table_col_descriptions)

with Flow("statline-bq") as statline_flow:
    """A Prefect flow to upload datasets from CBS Statline to Google BigQuery.

    A Prefect based equivalent of the standlone `statline_bq.utils.cbsodata_to_gbq()`.

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED".

    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.

    third_party: bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true
        to use dataderden.cbs.nl as base url (not available in v4 yet).

    gcp_env: str
        determines which GCP configuration to use from config.gcp
    
    force : bool, default = False
        If set to True, processes datasets, even if Modified dates are
        identical in source and target locations.
    """

    ids = Parameter("ids")
    source = Parameter("source", default="cbs")
    third_party = Parameter("third_party", default=False)
    gcp_env = Parameter("gcp_env", default="dev")
    force = Parameter("force", default=False)

    # ids = upper.map(
    #     ids
    # )  # TODO: Do we need a different variable name here (ids_upper = ...)?
    gcp_env = lower(gcp_env)  # TODO: Do we need a different variable name here?
    odata_versions = check_v4.map(ids)
    gcp = set_gcp(config, gcp_env)
    urls = get_urls.map(
        ids, odata_version=odata_versions, third_party=unmapped(third_party),
    )
    source_metas = get_metadata_cbs.map(
        id=ids, odata_version=odata_versions, third_party=unmapped(third_party)
    )
    gcp_metas = get_metadata_gcp.map(
        id=ids, source=unmapped(source), odata_version=odata_versions, gcp=unmapped(gcp)
    )  # TODO: skip if force=True
    cbs_modifieds = get_from_meta.map(meta=source_metas, key=unmapped("Modified"))
    gcp_modifieds = get_gcp_modified.map(
        gcp_meta=gcp_metas, force=unmapped(force)
    )  # TODO: skip if force=True
    skips = skip_dataset.map(
        cbs_modified=cbs_modifieds, gcp_modified=gcp_modifieds, force=unmapped(force)
    )
    go_nogo = skip_task.map(x=skips)
    pq_dir = create_named_dir.map(
        id=ids,
        odata_version=odata_versions,
        source=unmapped(source),
        config=unmapped(config),
        upstream_tasks=[go_nogo],
    )
    files_parquet = tables_to_parquet.map(
        id=ids,
        urls=urls,
        main_table_shape=get_main_table_shape.map(source_metas),
        odata_version=odata_versions,
        third_party=unmapped(third_party),
        source=unmapped(source),
        pq_dir=pq_dir,
        upstream_tasks=[go_nogo],
    )
    col_descriptions = get_column_descriptions.map(
        urls=urls, odata_version=odata_versions, upstream_tasks=[go_nogo]
    )
    meta_files = dict_to_json_file.map(
        id=ids,
        dict=source_metas,
        dir=pq_dir,
        suffix=unmapped("Metadata"),
        source=unmapped(source),
        odata_version=odata_versions,
        upstream_tasks=[go_nogo],
    )
    col_desc_files = dict_to_json_file.map(
        id=ids,
        dict=col_descriptions,
        dir=pq_dir,
        suffix=unmapped("ColDescriptions"),
        source=unmapped(source),
        odata_version=odata_versions,
        upstream_tasks=[go_nogo],
    )
    gcs_folders = upload_to_gcs.map(
        dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        id=ids,
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[files_parquet, col_desc_files, go_nogo],
    )
    file_names = get_file_names.map(files_parquet, upstream_tasks=[go_nogo],)
    dataset_refs = gcs_to_gbq.map(
        id=ids,
        source=unmapped(source),
        odata_version=odata_versions,
        third_party=unmapped(third_party),
        config=unmapped(config),
        gcs_folder=gcs_folders,
        file_names=file_names,
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[gcs_folders, go_nogo],
    )
    desc_dicts = get_col_descs_from_gcs.map(
        id=ids,
        source=unmapped(source),
        odata_version=odata_versions,
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        gcs_folder=gcs_folders,
        upstream_tasks=[gcs_folders, go_nogo],
    )
    bq_updates = bq_update_main_table_col_descriptions.map(
        dataset_ref=dataset_refs,
        descriptions=desc_dicts,
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[desc_dicts, go_nogo],
    )
    remove = remove_dir.map(
        pq_dir, upstream_tasks=[gcs_folders]
    )  # TODO: better(=more reliable) implementation for dir tree removal might be considered?


if __name__ == "__main__":
    from dask.distributed import Client, LocalCluster

    cluster = LocalCluster(ip="127.0.0.1:61611")
    dask_client = Client(cluster)
    # Register flow
    statline_flow.executor = DaskExecutor(
        address="tcp://127.0.0.1:61611",
        # cluster_class="LocalCluster",
        # n_workers=2,
        debug=True,
        # processes=True,
        # silence_logs=100,
    )
    # flow_id = statline_flow.register(
    #     project_name="nl_open_data", version_group_id="statline_bq"
    # )
    # print(f" └── Registered on: {datetime.today()}")

    """
    Output last registration
    ------------------------
    Flow URL: https://cloud.prefect.io/dataverbinders/flow/eef07631-c5d3-4313-9b2c-41b1e8d180a8
    └── ID: 2dedcace-27ec-42b9-8be7-dcdd954078e4
    └── Project: nl_open_data
    └── Labels: ['tud0029822']
    └── Registered on: 2021-01-12 14:52:31.387941
    """

    # Run locally
    ids = ["83583ned"]
    # ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]
    state = statline_flow.run(parameters={"ids": ids, "force": True})
