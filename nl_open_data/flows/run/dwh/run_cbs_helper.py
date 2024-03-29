# Uploads the following to Storage:
# - (A version of) CBS catalog (TODO: Decide if/what/how)
# - Kerncijfers wijken and buurten
# - Nabijheidsstatistieken
# - Bevolkingsstatistieken per pc4
# - Mapping pc6huisnummer tot buurten and wijken

# TODO: Creates a `CBS helper` dataset in BQ, with 4 (/5?) tables ??? (Concat?)

from datetime import datetime

from prefect import Flow, Client
from prefect.tasks.prefect import StartFlowRun

from nl_open_data.config import config as CONFIG
from nl_open_data.utils import get_gcs_uris

# Prefect client parameters
TENANT_SLUG = "dataverbinders"

client = Client()  # Local api key has been stored previously
client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

# GCP env parameters
GCP_ENV = "dev"
PROD_ENV = None

# General script parameters
SOURCE = "cbs"
RUN_TIME = f"{datetime.today().date()}_{datetime.today().time()}"
PROJECT = "nl_open_data"

################################################################################
# Upload Kerncijfers wijken and buurten to gcs (xls_flow)
# TODO: Are these the same or different then the regionaal_kwb statline datasets????
# TODO: How to concatanate?

# Taking 2013-2020 here, because earlier data has different format, so we leave integration of those for later. #TODO
# https://www.cbs.nl/nl-nl/reeksen/kerncijfers-wijken-en-buurten-2004-2020

# flow parameters
KWB_URLS = [
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2013.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kerncijfers-wijken-en-buurten-2014.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2015.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2016.xls",
    "https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/_exel/kwb-2017.xls",
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2018.xls",
    # "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2019.xls",  # BUG: Unexpected error: ArrowInvalid('Could not convert 5,0 with type str: tried to convert to double', 'Conversion failed for column p_stadsv with type object')
    # This issue stems from the mixed data types in Excel (NUMBER and TEXT).
    # The column is translated as a dtype=object, and then crashes when trying to convert to parquet.
    # This issue is related: https://issues.apache.org/jira/browse/ARROW-4131
    # No trivial solution (skip_columns does not exist in read_excel, trying str.replace(".", ",") also fails
    "https://www.cbs.nl/-/media/_excel/2021/12/kwb-2020.xls",
]
KWB_GCS_FOLDER = "cbs/kwb"
KWB_KWARGS = [
    {"na_values": [".", "        .", "        .       "]},  # 2013
    {"na_values": [".", "        .", "        .       "]},  # 2014
    {"na_values": [".", "        .", "        .       "]},  # 2015
    {"na_values": [".", "        .", "        .       "]},  # 2016
    {"na_values": [".", "        .", "        .       "]},  # 2017
    {"na_values": [".", "        .", "        .       "]},  # 2018
    # {"na_values": [".", "        .", "        .       "]},  # 2019 # BUG: (See above)
    {"na_values": [".", "        .", "        .       "]},  # 2020
]

# run parameters
VERSION_GROUP_ID = "xls_to_gcs"
RUN_NAME = f"cbs_helper_kwb_{RUN_TIME}"
PARAMETERS = {
    "urls": KWB_URLS,
    "gcs_folder": KWB_GCS_FOLDER,
    "read_excel_kwargs": KWB_KWARGS,
}

# Schedule run
kwb_to_gcs_flow = StartFlowRun(
    flow_name=VERSION_GROUP_ID,
    project_name=PROJECT,
    run_name=RUN_NAME,
    parameters=PARAMETERS,
    wait=True,
)
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)

################################################################################
# Upload Nabijheidsstatistieken to gcs (xls_flow + statline_gcs_flow)


######################
# xls_flow
# 2006-2016 figures are excel files

# flow parameters
NBH_URLS = [
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
NBH_GCS_FOLDER = "cbs/nbh"
GCP_ENV = "dev"
KWARGS = [
    {"na_values": [".", "        .", "        .       "]},  # 2006
    {"na_values": [".", "        .", "        .       "]},  # 2007
    {"na_values": [".", "        .", "        .       "]},  # 2008
    {"na_values": [".", "        .", "        .       "]},  # 2009
    {"na_values": [".", "        .", "        .       "]},  # 2010
    {"na_values": [".", "        .", "        .       "]},  # 2011
    {"na_values": [".", "        .", "        .       "]},  # 2012
    {"na_values": [".", "        .", "        .       "]},  # 2013
    {"na_values": [".", "        .", "        .       "]},  # 2014
    {"skiprows": [1, 2], "na_values": [".", "        .", "        .       "]},  # 2015
    {"skiprows": [1, 2], "na_values": [".", "        .", "        .       "]},  # 2016
]

# run parameters
VERSION_GROUP_ID = "xls_to_gcs"
RUN_NAME = f"cbs_helper_nbh_xls_{RUN_TIME}"
PARAMETERS = {
    "urls": NBH_URLS,
    "gcs_folder": NBH_GCS_FOLDER,
    "gcp_env": GCP_ENV,
    "PROD_ENV": PROD_ENV,
    "read_excel_kwargs": KWARGS,
}

# Schedule run
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)

######################
# statline_to_gcs flow
# 2017 onwards in datasets:

NBH_IDS = [
    "84334NED",  # 2017
    "84463NED",  # 2018
    "84718NED",  # 2019
]
NBH_SOURCE = SOURCE
THIRD_PARTY = False
GCP_ENV = "dev"
ENDPOINT = "gcs"
FORCE = False

# run parameters
VERSION_GROUP_ID = "statline_bq"
RUN_NAME = f"cbs_helper_nabijheid_statline_{RUN_TIME}"
PARAMETERS = {
    "ids": NBH_IDS,
    "source": NBH_SOURCE,
    "third_party": THIRD_PARTY,
    "endpoint": ENDPOINT,
    "force": FORCE,
}
# Schedule run
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)

################################################################################

# Bevolkingsstatistieken per pc4 (statline_gcs_flow)
BVS_IDS = [
    "83502NED",
]
BVS_SOURCE = SOURCE
THIRD_PARTY = False
ENDPOINT = "gcs"
FORCE = False

# run parameters
VERSION_GROUP_ID = "statline_bq"
RUN_NAME = f"cbs_helper_bevolking_pc4_{RUN_TIME}"
PARAMETERS = {
    "ids": BVS_IDS,
    "source": BVS_SOURCE,
    "third_party": THIRD_PARTY,
    "endpoint": ENDPOINT,
    "force": FORCE,
}

# Schedule run
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)
################################################################################
# TODO: Mapping pc6huisnummer tot buurten and wijken (????)

################################################################################
# Create dataset(/s) (gcs_to_bq_flow)
# TODO: these flows should be scheduled only after the previous ones are done
# See https://docs.prefect.io/core/idioms/flow-to-flow.html#scheduling-a-flow-of-flows for more info.

#####################
# KWB dataset
# flow parameters
URIS = get_gcs_uris(
    gcs_folder=KWB_GCS_FOLDER, source=SOURCE, config=CONFIG, gcp_env=GCP_ENV
)
DATASET_NAME = "cbs_kwb"
DESCRIPTION = "ADD DESCRIPTION HERE"  # TODO: Add description

# run parameters
VERSION_GROUP_ID = "gcs_to_bq"
RUN_NAME = f"gcs_to_bq_kwb_{RUN_TIME}"
PARAMETERS = {
    "uris": URIS,
    "dataset_name": DATASET_NAME,
    # "config": CONFIG,
    "gcp_env": GCP_ENV,
    "prod_env": PROD_ENV,
    "description": DESCRIPTION,
}

# Schedule run
kwb_gcs_to_bq_flow = StartFlowRun(
    flow_name=VERSION_GROUP_ID,
    project_name=PROJECT,
    run_name=RUN_NAME,
    parameters=PARAMETERS,
    wait=True,
)
flow_run_id = client.create_flow_run(
    version_group_id=VERSION_GROUP_ID, run_name=RUN_NAME, parameters=PARAMETERS,
)
################################################################################
# Build flow of flows

# TODO: NOt sure how this is supposed to work.
# with Flow("cbs_helper") as flow:
#     kwb_gcs_to_bq = kwb_gcs_to_bq_flow(upstream_tasks=[kwb_to_gcs_flow])

# flow.run()
# flow.register(project_name=PROJECT, version_group_id="cbs_helper")
