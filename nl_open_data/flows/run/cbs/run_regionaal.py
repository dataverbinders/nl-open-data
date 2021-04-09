"""Use statline-bq flow to upload the following datasets to GBQ:

TODO: Add docstring

[^kwb]: https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2019
[^regios]: https://opendata.cbs.nl/statline/portal.html?_catalog=CBS&_la=nl&tableId=84721NED&_theme=232
[^adres]: https://www.cbs.nl/nl-nl/maatwerk/2019/42/buurt-wijk-en-gemeente-2019-voor-postcode-huisnummer

"""
from datetime import datetime
from pathlib import Path

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config
from prefect import Client

# client parameters
TENANT_SLUG = "dataverbinders"

# flow parameters
ODATA_REGIONAAL = [  # TODO: check datasets, add and organize
    # Regionale kerncijfers Nederland
    "70072NED",
    # Kerncijfers wijken en buurten
    "84799NED",  # 2020
    "84583NED",  # 2019
    "84286NED",  # 2018
    "83765NED",  # 2017
    "83487NED",  # 2016
    "83220NED",  # 2015
    "82931NED",  # 2014
    "82339NED",  # 2013
    # Regionale indelingen
    "84721NED",
    # Grote bevolkingstabel per pc4-leeftijd-geslacht vanaf 1999
    "83502NED",
    # inkomensverdeling
    "84639NED",
    # Gebruik Voorzieningen Sociaal Domein; Wijken
    "83265NED",  # 2015
    "83619NED",  # 2016
    "83817NED",  # 2017
    "84420NED",  # 2018
    "84662NED",  # 2019
]
SOURCE = "cbs"
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = False

# run parameters
STATLINE_VERSION_GROUP_ID = "statline_bq"
STATLINE_RUN_NAME = f"regionaal_{datetime.today().date()}_{datetime.today().time()}"

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

# Statline-bq flow
statline_parameters = {
    "ids": ODATA_REGIONAAL,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}
flow_run_id = client.create_flow_run(
    version_group_id=STATLINE_VERSION_GROUP_ID,
    run_name=STATLINE_RUN_NAME,
    parameters=statline_parameters,
)

#################################################################################

# Zipped csv folder flow
# flow parameters
URL_PC6HUISNR = (
    "https://www.cbs.nl/-/media/_excel/2019/42/2019-cbs-pc6huisnr20190801_buurt.zip"
)
LOCAL_FOLDER = str(
    Path(__file__).parent / config.paths.temp
)  # TODO: organize better for deployment?
CSV_DELIMITER = ";"
BQ_DATASET_NAME = "buurt_wijk_gemeente_pc"
GCS_FOLDER = SOURCE + "/" + BQ_DATASET_NAME
BQ_DATASET_DESCRIPTION = "CBS definitions for geographical division on various granularity levels"  # TODO: Better description

# run parameters
ZIP_VERSION_GROUP_ID = "zipped_csv"
ZIP_RUN_NAME = f"regionaal_zip_{datetime.today().date()}_{datetime.today().time()}"

zip_parameters = {
    "url": URL_PC6HUISNR,
    # "local_folder": LOCAL_FOLDER,
    "csv_delimiter": CSV_DELIMITER,
    "gcs_folder": GCS_FOLDER,
    "bq_dataset_name": BQ_DATASET_NAME,
    "bq_dataset_description": BQ_DATASET_DESCRIPTION,
    "source": SOURCE,
}

flow_run_id = client.create_flow_run(
    version_group_id=ZIP_VERSION_GROUP_ID,
    run_name=ZIP_RUN_NAME,
    parameters=zip_parameters,
)
