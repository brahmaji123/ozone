import csv
import requests
import json

# Ranger credentials and API URL
RANGER_HOST = "http://<ranger_host>:6080"
AUTH = ("admin", "admin")  # replace with real creds
SERVICE_NAME = "cm_hdfs"   # HDFS service name in Ranger

# Permission mapping
PERM_MAP = {
    "r": "read",
    "w": "write",
    "x": "execute"
}

def parse_rwx(rwx_str):
    return [PERM_MAP[c] for c in rwx_str if c in PERM_MAP]

def get_policy_by_name(policy_name):
    url = f"{RANGER_HOST}/service/public/v2/api/service/{SERVICE_NAME}/policy/{policy_name}"
    resp = requests.get(url, auth=AUTH)
    resp.raise_for_status()
    return resp.json()

def update_policy(policy_name, group_name, permissions):
    policy = get_policy_by_name(policy_name)

    # Find if group already exists
    group_found = False
    for item in policy.get("policyItems", []):
        if group_name in item.get("groups", []):
            # Append missing permissions
            existing_perms = set(item["accesses"][0]["type"] for item in item["accesses"])
            for perm in permissions:
                if perm not in existing_perms:
                    item["accesses"].append({"type": perm, "isAllowed": True})
            group_found = True
            break

    if not group_found:
        # Add new group entry
        policy["policyItems"].append({
            "accesses": [{"type": p, "isAllowed": True} for p in permissions],
            "groups": [group_name],
            "users": [],
            "delegateAdmin": False
        })

    # PUT back the updated policy
    url = f"{RANGER_HOST}/service/public/v2/api/policy/{policy['id']}"
    resp = requests.put(url, auth=AUTH, json=policy)
    resp.raise_for_status()
    print(f"Updated policy {policy_name} with group {group_name}")

def process_csv(csv_file):
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            policy_name = row["policy_name"]
            group_name = row["group_name"]
            perms = parse_rwx(row["permissions"])
            update_policy(policy_name, group_name, perms)

if __name__ == "__main__":
    process_csv("ranger_policies.csv")
