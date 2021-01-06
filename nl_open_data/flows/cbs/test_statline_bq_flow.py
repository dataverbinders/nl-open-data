"""A Prefect flow to download XXXXX and upload to Google Cloud Platform.

The GCP configuration as well as local paths used for download, can be defined
in 'user_config.toml', which is imported and coupled to the Prefect config
object at the first import of Prefect. Therefore, anything that is defined in
the 'user_config.toml' can be accessed by accessing `prefect.config`. For
example, `prefect.config.gcp.dev`.
"""

import prefect
from box import Box
from prefect import task, Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from statline_bq.utils import (
    check_v4,
    get_urls,
    create_named_dir,
    tables_to_parquet,
    get_dataset_description,
    write_description_to_file,
    get_column_descriptions,
    write_col_decription_to_file,
    get_file_names,
    upload_to_gcs,
    gcs_to_gbq,
    set_gcp,
    get_col_descs_from_gcs,
    bq_update_main_table_col_descriptions,
)

from nl_open_data.tasks import remove_dir

# Converting statline-bq functions to tasks
check_v4 = task(check_v4)
get_urls = task(get_urls)
create_named_dir = task(create_named_dir)
tables_to_parquet = task(tables_to_parquet)
get_dataset_description = task(get_dataset_description)
write_description_to_file = task(write_description_to_file)
get_column_descriptions = task(get_column_descriptions)
write_col_decription_to_file = task(write_col_decription_to_file)
get_file_names = task(get_file_names)
upload_to_gcs = task(upload_to_gcs)
gcs_to_gbq = task(gcs_to_gbq)
set_gcp = task(set_gcp)
get_col_descs_from_gcs = task(get_col_descs_from_gcs)
bq_update_main_table_col_descriptions = task(bq_update_main_table_col_descriptions)

with Flow("CBS") as statline_flow:
    source = Parameter("source", default="cbs")
    ids = Parameter("ids")
    third_party = Parameter("third_party", default="False")
    gcp_env = Parameter("gcp_env", default="dev")
    config = Box({"paths": prefect.config.paths, "gcp": prefect.config.gcp})
    odata_versions = check_v4.map(ids)
    urls = get_urls.map(
        ids, odata_version=odata_versions, third_party=unmapped(third_party),
    )
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
    descriptions = get_dataset_description.map(urls=urls, odata_version=odata_versions)
    desc_files = write_description_to_file.map(
        description_text=descriptions,
        id=ids,
        pq_dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        upstream_tasks=[descriptions],
    )
    col_descriptions = get_column_descriptions.map(
        urls=urls, odata_version=odata_versions
    )
    col_desc_files = write_col_decription_to_file.map(
        id=ids,
        col_desc=col_descriptions,
        pq_dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        upstream_tasks=[col_descriptions],
    )
    gcs_folders = upload_to_gcs.map(
        dir=pq_dir,
        source=unmapped(source),
        odata_version=odata_versions,
        id=ids,
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        upstream_tasks=[files_parquet, desc_files, col_desc_files],
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

    ids = ["83583NED"]
    state = statline_flow.run(parameters={"ids": ids})
    # statline_flow.register(project_name="nl_open_data")
