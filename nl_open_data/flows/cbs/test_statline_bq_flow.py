"""A Prefect flow to download XXXXX and upload to Google Cloud Platform.

The GCP configuration as well as local paths used for download, can be defined
in 'user_config.toml', which is imported and coupled to the Prefect config
object inside 'config.py'. Therefore, anything that is defined in
the 'user_config.toml' can be accessed by accessing `config`. For
example, `config.gcp.dev`.
"""

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config

from box import Box
from prefect import task, Flow, unmapped, Parameter
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
    tables_to_parquet,
    get_column_descriptions,
    dict_to_json_file,
    upload_to_gcs,
    get_file_names,
    gcs_to_gbq,
    get_col_descs_from_gcs,
    bq_update_main_table_col_descriptions,
)

from nl_open_data.tasks import remove_dir, is_true

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
tables_to_parquet = task(tables_to_parquet)
get_column_descriptions = task(get_column_descriptions)
dict_to_json_file = task(dict_to_json_file)
get_file_names = task(get_file_names)
upload_to_gcs = task(upload_to_gcs)
gcs_to_gbq = task(gcs_to_gbq)
get_col_descs_from_gcs = task(get_col_descs_from_gcs)
bq_update_main_table_col_descriptions = task(bq_update_main_table_col_descriptions)

with Flow("CBS") as statline_flow:
    source = Parameter("source", default="cbs")
    ids = Parameter("ids")
    third_party = Parameter("third_party", default="False")
    gcp_env = Parameter("gcp_env", default="dev")
    config = Box({"paths": config.paths, "gcp": config.gcp})
    force = Parameter("force", default=False)

    odata_versions = check_v4.map(ids)
    gcp = set_gcp(config, gcp_env)
    urls = get_urls.map(
        ids, odata_version=odata_versions, third_party=unmapped(third_party),
    )
    source_metas = get_metadata_cbs.map(urls=urls, odata_version=odata_versions)
    gcp_metas = get_metadata_gcp.map(
        id=ids, source=unmapped(source), odata_version=odata_versions, gcp=unmapped(gcp)
    )
    cbs_modifieds = get_from_meta.map(meta=source_metas, key=unmapped("Modified"))
    gcp_modifieds = get_gcp_modified.map(gcp_meta=gcp_metas, force=unmapped(force))
    # skips = skip_dataset.map(
    #     cbs_modified=cbs_modifieds, gcp_modified=gcp_modifieds, force=unmapped(force)
    # )
    with case(skip_dataset.map(cbs_modified=cbs_modifieds, gcp_modified=gcp_modifieds, force=unmapped(force), False):
    # with case(skip_dataset, False):
        pq_dir = create_named_dir.map(
            id=ids,
            odata_version=odata_versions,
            source=unmapped(source),
            config=unmapped(config),
        )
        files_parquet = tables_to_parquet.map(
            id=ids,
            urls=urls,
            odata_version=odata_versions,
            source=unmapped(source),
            pq_dir=pq_dir,
        )
        col_descriptions = get_column_descriptions.map(
            urls=urls, odata_version=odata_versions
        )
        meta_files = dict_to_json_file.map(
            id=ids,
            dict=source_metas,
            dir=pq_dir,
            suffix="ColDescriptions",
            source=unmapped(source),
            odata_version=odata_versions,
            # upstream_tasks=[source_metas],
        )
        col_desc_files = dict_to_json_file.map(
            id=ids,
            dict=col_descriptions,
            dir=pq_dir,
            suffix="ColDescriptions",
            source=unmapped(source),
            odata_version=odata_versions,
            # upstream_tasks=[col_descriptions],
        )
        gcs_folders = upload_to_gcs.map(
            dir=pq_dir,
            source=unmapped(source),
            odata_version=odata_versions,
            id=ids,
            config=unmapped(config),
            gcp_env=unmapped(gcp_env),
            upstream_tasks=[files_parquet, col_desc_files],
        )
        file_names = get_file_names.map(files_parquet)
        dataset_refs = gcs_to_gbq.map(
            id=ids,
            source=unmapped(source),
            odata_version=odata_versions,
            third_party=unmapped(third_party),
            config=unmapped(config),
            gcs_folder=gcs_folders,
            file_names=file_names,
            gcp_env=unmapped(gcp_env),
            upstream_tasks=[gcs_folders],
        )
        desc_dicts = get_col_descs_from_gcs.map(
            id=ids,
            source=unmapped(source),
            odata_version=odata_versions,
            config=unmapped(config),
            gcp_env=unmapped(gcp_env),
            gcs_folder=gcs_folders,
            upstream_tasks=[gcs_folders],
        )
        bq_updates = bq_update_main_table_col_descriptions.map(
            dataset_ref=dataset_refs,
            descriptions=desc_dicts,
            config=unmapped(config),
            gcp_env=unmapped(gcp_env),
            upstream_tasks=[desc_dicts],
        )
        remove = remove_dir.map(pq_dir, upstream_tasks=[gcs_folders])


if __name__ == "__main__":
    # ids = ["83583NED"]
    ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]

    statline_flow.executor = DaskExecutor()
    statline_flow.register(project_name="nl_open_data")
    # state = statline_flow.run(parameters={"ids": ids})

