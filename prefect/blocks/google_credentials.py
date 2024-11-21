import json
from prefect_gcp import GcpCredentials

def load_service_account():
    with open("./google_credentials.json") as file:
        service_account_info = json.load(file)
    return service_account_info

service_account_info = load_service_account()

GcpCredentials(
    service_account_info=service_account_info
).save("gcp-creds")