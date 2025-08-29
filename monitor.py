import time
import requests
from msal import PublicClientApplication
from plyer import notification

# ==== CONFIGURATION ====
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id"
SUBSCRIPTION_ID = "your-subscription-id"
RESOURCE_GROUP = "your-resource-group"
DATA_FACTORY_NAME = "your-datafactory-name"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://management.azure.com/.default"]

# REST API Base
ADF_URL = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/{DATA_FACTORY_NAME}"

# ==== AUTHENTICATION ====
app = PublicClientApplication(CLIENT_ID, authority=AUTHORITY)

def get_token():
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPE, account=accounts[0])
    else:
        result = None

    if not result:
        print("🔑 Logging in with device code flow...")
        result = app.acquire_token_interactive(SCOPE)
    return result["access_token"]

# ==== MONITOR PIPELINES ====
def check_pipeline_failures(token):
    headers = {"Authorization": f"Bearer {token}"}

    url = f"{ADF_URL}/queryPipelineRuns?api-version=2018-06-01"
    body = {
        "lastUpdatedAfter": "2023-01-01T00:00:00Z",
        "lastUpdatedBefore": "2099-01-01T00:00:00Z"
    }

    resp = requests.post(url, headers=headers, json=body)
    resp.raise_for_status()
    runs = resp.json().get("value", [])

    failed_runs = [r for r in runs if r["status"] == "Failed"]

    if failed_runs:
        for run in failed_runs:
            msg = f"Pipeline {run['pipelineName']} FAILED (RunId: {run['runId']})"
            print(msg)
            notification.notify(
                title="⚠️ ADF Pipeline Failure",
                message=msg,
                timeout=10
            )

# ==== LOOP EVERY 5 MINUTES ====
if __name__ == "__main__":
    token = get_token()
    while True:
        try:
            check_pipeline_failures(token)
        except Exception as e:
            print(f"Error: {e}")
            # Refresh token if expired
            token = get_token()
        time.sleep(300)  # 5 minutes
