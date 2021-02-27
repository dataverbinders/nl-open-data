import io
from pathlib import Path
import json
import zipfile

import prefect
from prefect import task, Parameter, Flow
from prefect.tasks.shell import ShellTask
from prefect.engine.result_handlers import LocalResultHandler
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.gcp.bigquery import BigQueryLoadFile
from prefect. utilities.configuration import set_temporary_config
from lxml import etree, objectify
import xmltodict

from nl_open_data.config import get_config
from nimbletl.tasks import curl_cmd, unzip, create_dir


# TO DO: encapsulate config into initalization task so this can be provided at runtime
CONFIG = get_config("dk")

BAG_VERSION = "08042020"
BAG_URL = (
    "http://geodata.nationaalgeoregister.nl/inspireadressen/extract/inspireadressen.zip"
)

# NUM data
NUM_FILE = CONFIG.path.root / CONFIG.path.bag / (f"9999NUM{BAG_VERSION}" + ".zip")
NUM_TMP_DIR = CONFIG.path.root / CONFIG.path.tmp / Path("NUM")
NUM_TAG = "Nummeraanduiding"
NUM_XPATH = ".//" + NUM_TAG

# VBO data
VBO_FILE = CONFIG.path.root / CONFIG.path.bag / (f"9999VBO{BAG_VERSION}" + ".zip")


@task
def create_xml_list(zip_file):
    """
    Creates list of xml files from nested_zipfile which is in main BAG zipfile.
    """
    with zipfile.ZipFile(zip_file) as z:
        return [f for f in z.namelist() if f.endswith(".xml")]


## TO DO: use results handler
@task(checkpoint=True, result_handler=LocalResultHandler(dir=NUM_TMP_DIR.as_posix()))
def parse_num(xml_file, tmp_dir=NUM_TMP_DIR):
    """Parse xml file in BAG NUM zip archive.

    Args:
        - xml_file: str of XML file to be processed in NUM zip archive

    Returns:
        - Path-object to ndjson file

    """

    def remove_ns_keys(dict_, root):
        """Removes keys containing namespaces."""
        keys = list(dict_[root].keys())
        for key in keys:
            if key.startswith("@xmlns"):
                del dict_[root][key]
        return dict_

    zip = zipfile.Path(NUM_FILE, xml_file)
    with zip.open() as file_:
        xml = etree.parse(file_).getroot()

    # https://stackoverflow.com/questions/18159221/remove-namespace-and-prefix-from-xml-in-python-using-lxml
    for elem in xml.getiterator():
        elem.tag = etree.QName(elem).localname
    etree.cleanup_namespaces(xml)

    elements = [
        remove_ns_keys(xmltodict.parse(etree.tostring(element)), root=NUM_TAG)
        for element in xml.findall(NUM_XPATH)
    ]

    # https://stackoverflow.com/questions/51300674/converting-json-into-newline-delimited-json-in-python
    json_records = [json.dumps(element[NUM_TAG]) for element in elements]
    ndjson = tmp_dir / (xml_file.split(".")[0] + ".ndjson")
    with open(ndjson, "w") as file_:
        file_.write("\n".join(json_records))
    return ndjson


load_file = BigQueryLoadFile(
    dataset_id="bag",
    table="objecten",
    project=CONFIG.gcp.project,
    location=CONFIG.gcp.location,
)

curl_download = ShellTask(name="curl_download")

with Flow("BAG NUM") as flow:
    # credentials, _ = google.auth.default()
    # command = curl_cmd(BAG_URL, BAG_FILE)
    # curl = curl_download(command=command)

    # need to add switch: if zipfile exist, skip
    # extract = unzip(zipfile=BAG_FILE, upstream_tasks=[curl])
    tmp_dir = create_dir(NUM_TMP_DIR)
    gcp_credentials = PrefectSecret(name='GCP_CREDENTIALS')

    # TODO: create dataset 'bag'

    xmls = create_xml_list(NUM_FILE)
    ndjson = parse_num.map(xmls)
    load_job = load_file.map(ndjson)

if __name__ == "__main__":
    with Flow("test load file") as test_flow:

        ## TODO: how to map and run? With unmapped?
        load = load_file.run(file=NUM_TMP_DIR / '9999NUM08042020-000002.ndjson', source_format='NEWLINE_DELIMITED_JSON')

    with set_temporary_config({"cloud.use_local_secrets": True}): 
        # with prefect.context(secrets=dict(GCP_CREDENTIALS=CONFIG.gcp.credentials_info)): 
        test_flow.run()
