import requests
import json
from requests.auth import HTTPBasicAuth

# === Configuration ===
RANGER_HOST = "http://<ranger-host>:6080"  # Replace with your Ranger host
USERNAME = "admin"                         # Replace with your Ranger username
PASSWORD = "admin"                         # Replace with your Ranger password

# === API Endpoint ===
ROLES_API = f"{RANGER_HOST}/service/public/v2/api/roles"

# === Export Location ===
OUTPUT_FILE = "ranger_roles_export.json"

def export_all_ranger_roles():
    try:
        # Make API call to get all roles
        response = requests.get(
            ROLES_API,
            auth=HTTPBasicAuth(USERNAME, PASSWORD)
        )

        if response.status_code == 200:
            roles = response.json()
            with open(OUTPUT_FILE, "w") as outfile:
                json.dump(roles, outfile, indent=4)
            print(f"✅ Successfully exported {len(roles)} roles to '{OUTPUT_FILE}'")
        else:
            print(f"❌ Failed to fetch roles. Status Code: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"⚠️ Error occurred: {str(e)}")

if __name__ == "__main__":
    export_all_ranger_roles()
