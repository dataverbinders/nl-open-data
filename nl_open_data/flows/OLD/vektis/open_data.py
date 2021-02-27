from pathlib import Path

import pandas as pd
import pandas_gbq
from prefect import task, Flow, Parameter
from prefect.utilities.tasks import unmapped

from nl_open_data.config import get_config

# TODO: add this in config
DIR = Path.home() / "nl-open-data/vektis/open-data"
files_gemeente = [file for file in DIR.glob("*.csv") if "gemeente" in str(file)]
files_pc3 = [file for file in DIR.glob("*.csv") if "postcode3" in str(file)]


@task
def read_files(files):
    dfs = [
        (
            pd.read_csv(file, sep=";")
            .rename(columns=str.lower)
            .assign(jaar=lambda x: int(str(file)[-20:-15]))
            # TODO: jaar as first column
        )
        for file in files
    ]
    return pd.concat(dfs)


to_gbq = task(pandas_gbq.to_gbq)

# gemeente.to_gbq('vektis.open_data_gemeente', project_id=CONFIG.gcp.project, if_exists='replace', location=CONFIG.gcp.location)

with Flow("Vektis open data") as flow:
    gcp_project = Parameter("gcp_project", required=True)
    gcp_location = Parameter("gcp_location", required=True)
    dataframes = read_files.map([files_gemeente, files_pc3])
    to_gbq = to_gbq.map(
        dataframe=dataframes,
        destination_table=["vektis.open_data_gemeente", "vektis.open_data_pc3"],
        project_id=unmapped(gcp_project),
        if_exists=unmapped("replace"),
        location=unmapped(gcp_location),
    )


if __name__ == "__main__":
    config = get_config("dataverbinders")
    flow.run(
        parameters={
            "gcp_project": config.gcp.project,
            "gcp_location": config.gcp.location,
        }
    )
