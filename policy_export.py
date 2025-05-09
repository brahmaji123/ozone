import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from concurrent.futures import ThreadPoolExecutor
import sys

# === Config ===
RANGER_URL = 'http://<ranger-host>:6080'  # <<< Replace <ranger-host>
USERNAME = 'admin'
PASSWORD = 'admin'
POLICIES_FILE = 'policies.json'  # JSON list of 100k policies
BATCH_SIZE = 100  # Adjust based on server tolerance
MAX_WORKERS = 10  # Parallel threads
OVERRIDE_EXISTING = False  # Default (can enable with --override)

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
        service_name = policy.get('service')
        policy_name = policy.get('name')
        url = f"{RANGER_URL}/service/public/v2/api/policy"

        # First try POST (create)
        response = session.post(url, json=policy)
        if response.status_code in (200, 201):
            successes += 1
            continue

        # If POST fails and override is enabled → check if policy exists
        if OVERRIDE_EXISTING:
            get_url = f"{RANGER_URL}/service/public/v2/api/policy?serviceName={service_name}&policyName={policy_name}"
            get_resp = session.get(get_url)

            if get_resp.status_code == 200:
                existing_policy = get_resp.json()
                policy_id = existing_policy.get('id')
                if policy_id:
                    # Prepare updated policy with existing ID
                    policy['id'] = policy_id
                    put_url = f"{RANGER_URL}/service/public/v2/api/policy/{policy_id}"
                    put_resp = session.put(put_url, json=policy)
                    if put_resp.status_code in (200, 201):
                        print(f" Updated policy '{policy_name}' in service '{service_name}'")
                        successes += 1
                        continue
                    else:
                        print(f" Failed to update policy '{policy_name}': {put_resp.status_code} - {put_resp.text}")
                        failures += 1
                        continue

        # If neither POST nor PUT worked → fail
        print(f"❌ Failed to create policy '{policy_name}': {response.status_code} - {response.text}")
        failures += 1

    return successes, failures

# === Parallel uploader ===
def main():
    global OVERRIDE_EXISTING
    if '--override' in sys.argv:
        OVERRIDE_EXISTING = True
        print(" Override mode ENABLED: Existing policies will be updated if they exist")

    batches = list(batch(policies, BATCH_SIZE))
    total_success, total_fail = 0, 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_batch, b) for b in batches]
        for future in futures:
            success, fail = future.result()
            total_success += success
            total_fail += fail

    print(f"\n Successfully processed {total_success} policies")
    if total_fail > 0:
        print(f" Failed to process {total_fail} policies")

if __name__ == '__main__':
    main()
