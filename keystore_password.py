#!/usr/bin/env python3
import json
import sys
import requests
from datetime import datetime
from requests.auth import HTTPBasicAuth
import os

CONFIG_PATH = sys.argv[1]
LOG_PATH = "/tmp/keystore_password_update.log"

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

CM_HOST = config['cm_host']
CM_PORT = config['cm_port']
CLUSTER_NAME = config['cluster_name']
USERNAME = config['cm_user']
PASSWORD = config['cm_password']
NEW_PASSWORD = config['new_keystore_password']

BASE_URL = f"http://{CM_HOST}:{CM_PORT}/api/v31"
auth = HTTPBasicAuth(USERNAME, PASSWORD)

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_PATH, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")

def get_services():
    r = requests.get(f"{BASE_URL}/clusters/{CLUSTER_NAME}/services", auth=auth)
    r.raise_for_status()
    return r.json()["items"]

def get_service_config(service_name):
    url = f"{BASE_URL}/clusters/{CLUSTER_NAME}/services/{service_name}/config"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    return r.json()

def update_service_config(service_name, updates):
    url = f"{BASE_URL}/clusters/{CLUSTER_NAME}/services/{service_name}/config"
    payload = {
        "items": [{"name": key, "value": value} for key, value in updates.items()]
    }
    r = requests.put(url, auth=auth, json=payload)
    r.raise_for_status()

def get_role_config_groups(service_name):
    url = f"{BASE_URL}/clusters/{CLUSTER_NAME}/services/{service_name}/roleConfigGroups"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    return r.json()["items"]

def get_role_config(service_name, role_group_name):
    url = f"{BASE_URL}/clusters/{CLUSTER_NAME}/services/{service_name}/roleConfigGroups/{role_group_name}/config"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    return r.json()

def update_role_config(service_name, role_group_name, updates):
    url = f"{BASE_URL}/clusters/{CLUSTER_NAME}/services/{service_name}/roleConfigGroups/{role_group_name}/config"
    payload = {
        "items": [{"name": k, "value": v} for k, v in updates.items()]
    }
    r = requests.put(url, auth=auth, json=payload)
    r.raise_for_status()

def main():
    services = get_services()
    for svc in services:
        service_name = svc["name"]
        log(f"Processing service: {service_name}")

        # 1. Service-level config
        try:
            config = get_service_config(service_name)
            updates = {}
            for item in config.get("items", []):
                key = item.get("name")
                if key and "keystore" in key.lower() and "password" in key.lower():
                    updates[key] = NEW_PASSWORD
            if updates:
                update_service_config(service_name, updates)
                log(f" → Updated service config: {updates}")
        except Exception as e:
            log(f" → Error updating service config: {e}")

        # 2. RoleConfigGroup-level config
        try:
            role_groups = get_role_config_groups(service_name)
            for rg in role_groups:
                rg_name = rg["name"]
                rg_config = get_role_config(service_name, rg_name)
                rg_updates = {}
                for item in rg_config.get("items", []):
                    key = item.get("name")
                    if key and "keystore" in key.lower() and "password" in key.lower():
                        rg_updates[key] = NEW_PASSWORD
                if rg_updates:
                    update_role_config(service_name, rg_name, rg_updates)
                    log(f" → Updated role group '{rg_name}': {rg_updates}")
        except Exception as e:
            log(f" → Error in role group update for {service_name}: {e}")

    os.remove(CONFIG_PATH)

if __name__ == "__main__":
    main()
