"""Registering a Prefect flow uploading datasets from statline to Google BigQuery.

A Prefect based equivalent of the standlone `statline_bq.utils.main()`.

The GCP configuration as well as local paths used for download, should be defined
in 'user_config.toml', which is imported and coupled to the Prefect config object
inside 'config.py'. Therefore, anything that is defined in the 'user_config.toml'
can be accessed by accessing `config`. For example, `config.gcp.dev`.
"""
from prefect import task, Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor
from prefect.triggers import all_finished
from statline_bq.statline import _check_v4, _get_urls, get_metadata_cbs
from statline_bq.gcpl import _set_gcp, _get_metadata_gcp
from statline_bq.utils import _check_gcp_env, _create_named_dir
from statline_bq.main import main, _skip_dataset

from nl_open_data.config import config as CONFIG
from nl_open_data.tasks import remove_dir
import nl_open_data.tasks as nlt

# Converting statline-bq functions to tasks
check_gcp_env = task(_check_gcp_env)
check_v4 = task(_check_v4)
set_gcp = task(_set_gcp)
get_urls = task(_get_urls)
get_metadata_cbs = task(get_metadata_cbs)
get_metadata_gcp = task(_get_metadata_gcp)
skip_dataset = task(_skip_dataset)
create_named_dir = task(_create_named_dir)
main = task(main)

# Always clean up at end
remove_dir.trigger = all_finished

VERSION_GROUP_ID = "statline_bq"
PROJECT_NAME = "nl_open_data"


with Flow("statline-bq") as statline_flow:
    """A Prefect flow to upload datasets from CBS Statline to Google BigQuery.

    A prefect wrapper for `statline_bq.main.main()`, with a mandatory clean up attempted afterwards.

    Parameters
    ----------
    id: str
        CBS Dataset id, i.e. "83583NED".

    source: str, default="cbs"
        The source of the dataset. Currently only "cbs" is relevant.

    third_party: bool, default=False
        Flag to indicate dataset is not originally from CBS. Set to true to use dataderden.cbs.nl as base url (not available in v4 yet).

    config : Config object
        Config object holding GCP and local paths.

    gcp_env: str
        determines which GCP configuration to use from config.gcp. Options: ['dev', 'test', 'prod']

    endpoint: str
        Determines the end result of the function.
        * 'local' stores the parquet files locally
        * 'gcs' uploads the parquet file to Google Cloud Storage
        * 'bq' uploads the parquet file to Google Cloud Storage and creates a linked dataset in BigQuery

    local_dir: str or Path, default=None
        If endpoint='local', determines the folder to store parquet files. If set to None,
        creates a folder within the temp directories of the OS based on the dataset source and id.

    force : bool, default = False
        If set to True, processes datasets, even if Modified dates are identical in source and target locations.
    
    credentials: Credentials, default=None
        Google oauth2 credentials, passed to google-cloud clients. If not passed,
        falls back to the default inferred from the environment.
    """
    ids = Parameter("ids")
    source = Parameter("source", default="cbs")
    third_party = Parameter("third_party", default=False)
    # BUG hotfix: If the config object is provided in a `run` script, it is translated to a 'dict' not a 'Box', and an error occurs.
    # Providing it as default in the `register` stage is a temporary hotfix (though providing a default might be considered anyway)
    config = Parameter("config", default=CONFIG)
    gcp_env = Parameter("gcp_env", default="dev")
    # endpoint = Parameter("endpoint", default="bq")
    local_dir = Parameter(
        "local_dir", default=None
    )  # TODO: how to manage in a mapped context? is it even needed here?
    force = Parameter("force", default=False)
    credentials = Parameter("credentials", default=None)

    config_box = nlt.dict_to_box(config, frozen_box=True)
    gcp_env = nlt.lower(gcp_env)
    odata_versions = check_v4.map(ids)
    gcp = set_gcp(config_box, gcp_env, source)
    skips = skip_dataset.map(
        id=ids,
        source=unmapped(source),
        third_party=unmapped(third_party),
        odata_version=odata_versions,
        gcp=unmapped(gcp),
        force=unmapped(force),
        credentials=unmapped(credentials),
    )
    go_nogo = nlt.skip_task.map(x=skips)
    pq_files = main.map(
        id=ids,
        source=unmapped(source),
        third_party=unmapped(third_party),
        config=unmapped(config_box),
        gcp_env=unmapped(gcp_env),
        force=unmapped(force),
        credentials=unmapped(credentials),
        upstream_tasks=[go_nogo],
    )
    # This returns the directories used in main, possibly recreating them if they were properly deleted in main
    local_folders = create_named_dir.map(
        id=ids,
        odata_version=odata_versions,
        source=unmapped(source),
        config=unmapped(config_box),
        upstream_tasks=[pq_files],
        # BUG: Why is this needed? setting "remove_dir.trigger = all_finished" should have been sufficient, but it isn't
        # This dependency is set to ensure that remove_dir (which has a data dependency on this task) is run after main
    )
    remove = remove_dir.map(local_folders)


if __name__ == "__main__":
    # Register flow
    statline_flow.storage = GCS(
        project=CONFIG.gcp.dev.project_id, bucket=f"{CONFIG.gcp.dev.bucket}-prefect",
    )
    statline_flow.run_config = LocalRun(labels=["nl-open-data-vm-1"])
    statline_flow.executor = DaskExecutor()
    flow_id = statline_flow.register(
        project_name=PROJECT_NAME, version_group_id=VERSION_GROUP_ID
    )
    # Run locally
    # ids = ["83583NED"]
    # ids = ["83583NED", "83765NED", "84799NED", "84583NED", "84286NED"]
    # mlz_ids = ["40015NED", "40080NED", "40081NED"]
    # statline_flow.executor = DaskExecutor(
    #     # cluster_class="LocalCluster",
    #     cluster_kwargs={"n_workers": 8},
    #     # debug=True,
    #     # processes=True,
    #     # silence_logs=100, # TODO (?) : find out what the number stands for
    # )
    # state = statline_flow.run(
    #     parameters={
    #         "ids": ids,
    #         "source": "cbs",
    #         "third_party": False,
    #         "force": True,
    #         "config": CONFIG,
    #         "gcp_env": "dev",
    #     }
    # )
    # state = statline_flow.run(
    #     parameters={
    #         "ids": mlz_ids,
    #         "source": "mlz",
    #         "third_party": True,
    #         "force": True,
    #         "gcp_env": "prod",
    #     }
    # )
