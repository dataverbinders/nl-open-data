# Uploads the following to Storage:
# - Kerncijfers wijken and buurten
# - Nabijheidsstatistieken
# - Bevolkingsstatistieken per pc4
# - Mapping pc6huisnummer tot buurten and wijken

# TODO: Creates a `CBS helper` dataset in BQ, with 4 (/5?) tables ??? (Concat?)

from datetime import datetime

# the config object must be imported from config.py before any Prefect imports
from nl_open_data.config import config
from prefect import Client

# client parameters
TENANT_SLUG = "dataverbinders"

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

################################################################################
# Upload Kerncijfers wijken and buurten to gcs (xls_flow)
# TODO: Are these the same or different then the regionaal_kwb statline datasets????
# TODO: Concating??

# Taking 2013-2020 here, because earlier data has different format, so we leave integration of those for later. #TODO
# https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2020

# flow parameters
URLS = [
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2013.xls",
    # "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kerncijfers-wijken-en-buurten-2014.xls",  # TODO: PyArrow error in conversion to Parquet
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2015.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2016.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2017.xls",
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2018.xls",
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2019.xls",
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2020.xls",
]
GCS_FOLDER = "cbs/kwb"
GCP_ENV = "dev"
PROD_ENV = None

# run parameters
VERSION_GROUP_ID = "xls_to_gcs"
RUN_NAME = f"cbs_helper_kwb_{datetime.today().date()}_{datetime.today().time()}"
PARAMETERS = {
    "urls": URLS,
    "gcs_folder": GCS_FOLDER,
    "gcp_env": GCP_ENV,
    "PROD_ENV": PROD_ENV,
}

# Schedule run
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)

################################################################################
# Upload Nabijheidsstatistieken to gcs (xls_flow + statline_gcs_flow)


######################
# xls_flow
# 2006-2016 figures are excel files

# flow parameters
URLS = [  # TODO: PyArrow error in conversion to Parquet !!ALL!!
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2006-2016-04-18.xls",
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2007-2016-04-18.xls",
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2008-2016-04-18.xls",
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2009-2016-04-18.xls",
    "https://www.cbs.nl/-/media/_excel/2016/17/nabijheid-2010-2016-04-18.xls",
    "https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2011-2016-04-18.xls",
    "https://www.cbs.nl/-/media/_excel/2016/16/nabijheid-2012-2016-04-18.xls",
    "https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2013-2016-12-19.xls",
    "https://www.cbs.nl/-/media/_excel/2016/51/nabijheid-2014-2016-10-11-(1).xls",
    "https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_wijkbuurt_2015v3.xls",
    "https://www.cbs.nl/-/media/_excel/2017/32/nabijheid_2016.xls",
]
GCS_FOLDER = "cbs/nabijheid"
GCP_ENV = "dev"
PROD_ENV = None

# run parameters
VERSION_GROUP_ID = "xls_to_gcs"
RUN_NAME = (
    f"cbs_helper_nabijheid_xls_{datetime.today().date()}_{datetime.today().time()}"
)
PARAMETERS = {
    "urls": URLS,
    "gcs_folder": GCS_FOLDER,
    "gcp_env": GCP_ENV,
    "PROD_ENV": PROD_ENV,
}

# Schedule run
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)

######################
# statline_to_gcs flow
# 2017 onwards in datasets:

IDS = [
    "84334NED",  # 2017
    "84463NED",  # 2018
    "84718NED",  # 2019
]
SOURCE = "cbs"
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = False

# run parameters
VERSION_GROUP_ID = "statline_gcs"
RUN_NAME = (
    f"cbs_helper_nabijheid_statline_{datetime.today().date()}_{datetime.today().time()}"
)
PARAMETERS = {
    "ids": IDS,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}
# Schedule run
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)

################################################################################

# Bevolkingsstatistieken per pc4 (statline_gcs_flow)
IDS = [
    "83502NED",
]
SOURCE = "cbs"
THIRD_PARTY = False
GCP_ENV = "dev"
FORCE = False

# run parameters
VERSION_GROUP_ID = "statline_gcs"
RUN_NAME = (
    f"cbs_helper_bevolking_pc4_{datetime.today().date()}_{datetime.today().time()}"
)
PARAMETERS = {
    "ids": IDS,
    "source": SOURCE,
    "third_party": THIRD_PARTY,
    "gcp_env": GCP_ENV,
    "force": FORCE,
}

# Schedule run
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)

################################################################################
# Create dataset(/s) (gcs_to_bq_flow)
# TODO: When more then 1 flow can be run at the same time, this flow should be scheduled only after the previous ones are done
# See https://docs.prefect.io/core/idioms/flow-to-flow.html#scheduling-a-flow-of-flows for more info.

################################################################################
# Mapping pc6huisnummer tot buurten and wijken (????)
