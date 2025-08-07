import requests
import json
from requests.auth import HTTPBasicAuth

# === Configuration ===
RANGER_HOST = "http://<ranger-host>:6080"  # Replace with your Ranger host
USERNAME = "admin"                         # Replace with your Ranger username
PASSWORD = "admin"                         # Replace with your Ranger password

# === Export Location ===
OUTPUT_FILE = "ranger_roles_export.json"

# === API Endpoint Base ===
ROLES_API_BASE = f"{RANGER_HOST}/service/public/v2/api/roles"

def fetch_all_roles():
    all_roles = []
    page_size = 200
    page = 0

    while True:
        params = {
            'pageSize': page_size,
            'page': page
        }

        response = requests.get(
            ROLES_API_BASE,
            params=params,
            auth=HTTPBasicAuth(USERNAME, PASSWORD)
        )

        if response.status_code != 200:
            print(f"❌ Failed to fetch roles on page {page}. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            break

        roles = response.json()
        if not roles:
            break  # No more roles to fetch

        all_roles.extend(roles)
        print(f"✅ Fetched {len(roles)} roles from page {page}")
        page += 1

    return all_roles

def export_roles_to_file(roles, filename):
    with open(filename, 'w') as f:
        json.dump(roles, f, indent=4)
    print(f"✅ Exported {len(roles)} roles to {filename}")

if __name__ == "__main__":
    roles = fetch_all_roles()
    export_roles_to_file(roles, OUTPUT_FILE)
