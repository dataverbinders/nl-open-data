"""
Starting up a VM Instance
"""
from pprint import pprint

from googleapiclient import discovery

# from oauth2client.client import GoogleCredentials
INSTANCE_NAME = "nl-open-data-preemptible-1"
PROJECT = "dataverbinders-dev"
ZONE = "europe-west4-a"

# credentials = GoogleCredentials.get_application_default()

service = discovery.build("compute", "v1")
# service = discovery.build("compute", "v1", credentials=credentials)

# request = service.instances().start(instance=INSTANCE_NAME)
request = service.instances().start(project=PROJECT, zone=ZONE, instance=INSTANCE_NAME)
response = request.execute()

# TODO: Change code below to process the `response` dict:
pprint(response)
