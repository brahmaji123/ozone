import requests
import json
from concurrent.futures import ThreadPoolExecutor
import time

# Ranger API configuration
RANGER_URL = "http://your-ranger-server:6080"
AUTH = ("admin", "admin")  # Replace with your credentials
POLICY_API = f"{RANGER_URL}/service/public/v2/api/policy"

# Headers for API requests
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def import_policy(policy_data):
    """Import a single policy into Ranger"""
    try:
        response = requests.post(
            POLICY_API,
            data=json.dumps(policy_data),
            headers=HEADERS,
            auth=AUTH
        )
        
        if response.status_code in (200, 201):
            return True
        else:
            print(f"Failed to import policy {policy_data.get('name')}: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error importing policy {policy_data.get('name')}: {str(e)}")
        return False

def batch_import_policies(policies, batch_size=100, max_workers=10):
    """Import policies in batches with parallel processing"""
    success_count = 0
    failure_count = 0
    
    # Process in batches to avoid memory issues
    for i in range(0, len(policies), batch_size):
        batch = policies[i:i + batch_size]
        
        # Use thread pool for parallel processing within the batch
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(import_policy, batch))
        
        # Update counts
        success_count += sum(results)
        failure_count += len(results) - sum(results)
        
        print(f"Processed batch {i//batch_size + 1}: {success_count} success, {failure_count} failures")
        
        # Small delay between batches to avoid overwhelming the server
        time.sleep(1)
    
    return success_count, failure_count

# Main execution
if __name__ == "__main__":
    # Load your policies from JSON file
    with open('policies.json') as f:
        all_policies = json.load(f)
    
    print(f"Starting import of {len(all_policies)} policies...")
    
    # Adjust batch_size and max_workers based on your Ranger server capacity
    success, failures = batch_import_policies(
        all_policies,
        batch_size=200,  # Number of policies per batch
        max_workers=20   # Concurrent requests per batch
    )
    
    print(f"Import completed. Success: {success}, Failures: {failures}")
