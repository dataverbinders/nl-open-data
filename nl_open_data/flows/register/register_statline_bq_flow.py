"""Registering a Prefect flow uploading datasets from statline to Google BigQuery.

A Prefect based equivalent of the standlone `statline_bq.utils.main()`.

The GCP configuration as well as local paths used for download, should be defined
in 'user_config.toml', which is imported and coupled to the Prefect config object
inside 'config.py'. Therefore, anything that is defined in the 'user_config.toml'
can be accessed by accessing `config`. For example, `config.gcp.dev`.
"""

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config as CONFIG

from prefect import task, Flow, unmapped, Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.executors import DaskExecutor
from prefect.triggers import all_finished
from statline_bq.utils import main

import nl_open_data.tasks as nlt

# Converting imported functions to tasks
main = task(main, log_stdout=True)
# Always clean up at end
nlt.remove_dir.trigger = all_finished

VERSION_GROUP_ID = "statline_bq"
PROJECT_NAME = "nl_open_data"

with Flow("statline-bq") as statline_flow:
    """A Prefect flow to upload datasets from CBS Statline to Google BigQuery.

    A prefect wrapper for `statline_bq.utils.main()`, with a mandatory clean up attempted afterwards.

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
    force : bool, default = False
        If set to True, processes datasets, even if Modified dates are identical in source and target locations.
    """
    ids = Parameter("ids")
    source = Parameter("source", default="cbs")
    third_party = Parameter("third_party", default=False)
    # BUG hotfix: If the config object is provided in a `run` script, it is translated to a 'dict' not a 'Box', and an error occurs.
    # Providing it as default in the `register` stage is a temporary hotfix (though providing a default might be considered anyway)
    config = Parameter("config", default=CONFIG)
    gcp_env = Parameter("gcp_env", default="dev")
    force = Parameter("force", default=False)

    local_folders = main.map(
        id=ids,
        source=unmapped(source),
        third_party=unmapped(third_party),
        config=unmapped(config),
        gcp_env=unmapped(gcp_env),
        force=unmapped(force),
    )
    remove = nlt.remove_dir.map(local_folders)

if __name__ == "__main__":
    # Register flow
    statline_flow.storage = GCS(
        project="dataverbinders-dev",
        bucket="dataverbinders-dev-prefect",  # TODO: Switch to using config (config.gcp.dev.project_id, etc.)
    )
    statline_flow.run_config = LocalRun(labels=["nl-open-data-vm-1"])
    statline_flow.executor = DaskExecutor(
        # cluster_class="LocalCluster",
        # cluster_kwargs={"n_workers": 8},
        # debug=True,
        # processes=True,
        # silence_logs=100, # TODO (?) : find out what the number stands for
    )
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
