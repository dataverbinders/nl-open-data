"""Dataflow for MLZ data using Statline from Statistics Netherlands (Centraal Bureau voor Statistiek, CBS).

Loads the following datasets into BigQuery:


"""
from prefect import task, Flow, unmapped, Parameter
from prefect.triggers import all_finished

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
from nl_open_data.config import get_config

# Converting functions to tasks
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
remove_dir = task(remove_dir, trigger=all_finished)

ODATA_MLZ = ["40061NED", "40060NED"]

with Flow("MLZ") as mlz_flow:
    ids = ODATA_MLZ
    source = Parameter("source")
    config = Parameter("config")
    third_party = Parameter("third_party")
    gcp_env = Parameter("gcp_env", default="dev")

    odata_versions = check_v4.map(ids)
    urls = get_urls.map(
        ids, odata_version=odata_versions, third_party=unmapped(third_party)
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
    remove_dir.map(pq_dir, upstream_tasks=[gcs_folders])


if __name__ == "__main__":
    from nl_open_data.config import get_config
    from pathlib import Path
    import os

    config_file = Path.home() / Path("Projects/nl-open-data/nl_open_data/config.toml")
    config = get_config(config_file)
    # state = mlz_flow.run(
    #     parameters={"config": config, "source": "mlz", "third_party": True}
    # )
    mlz_flow.register(project_name="nl_open_data")
