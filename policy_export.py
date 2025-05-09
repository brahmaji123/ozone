import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from concurrent.futures import ThreadPoolExecutor

# === Config ===
RANGER_URL = 'http://<ranger-host>:6080'
USERNAME = 'admin'
PASSWORD = 'admin'
POLICIES_FILE = 'policies.json'  # JSON list of 100k policies
BATCH_SIZE = 100  # Adjust based on server tolerance
MAX_WORKERS = 10  # Parallel threads

# === HTTP Session with Retries ===
def get_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.auth = (USERNAME, PASSWORD)
    session.headers.update({'Content-Type': 'application/json'})
    return session

# === Load policies ===
with open(POLICIES_FILE, 'r') as f:
    policies = json.load(f)

# === Batch the policies ===
def batch(iterable, n):
    """Yield successive n-sized batches from iterable"""
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

# === Upload a batch ===
def upload_batch(batch_policies):
    session = get_session()
    successes, failures = 0, 0
    for policy in batch_policies:
        service_name = policy.get('service')  # required
        url = f"{RANGER_URL}/service/public/v2/api/policy"
        response = session.post(url, json=policy)
        if response.status_code in (200, 201):
            successes += 1
        else:
            failures += 1
            print(f"Failed to create policy: {response.status_code} - {response.text}")
    return successes, failures

# === Parallel uploader ===
def main():
    batches = list(batch(policies, BATCH_SIZE))
    total_success, total_fail = 0, 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_batch, b) for b in batches]
        for future in futures:
            success, fail = future.result()
            total_success += success
            total_fail += fail

    print(f"✅ Successfully imported {total_success} policies")
    if total_fail > 0:
        print(f"❌ Failed to import {total_fail} policies")

if __name__ == '__main__':
    main()
