import requests
import json

def export_all_roles(base_url, auth):
    all_roles = []
    page_size = 200
    page_no = 1
    
    while True:
        url = f"{base_url}?pageSize={page_size}&pageNo={page_no}"
        response = requests.get(url, auth=auth)
        data = response.json()
        
        if not data.get('vList'):
            break
            
        all_roles.extend(data['vList'])
        page_no += 1
        
        # Optional: print progress
        print(f"Fetched page {page_no} with {len(data['vList'])} roles")
    
    return all_roles

# Usage
ranger_url = "http://<ranger-host>:6080/service/roles/roles"
auth = ('admin', 'password')
all_roles = export_all_roles(ranger_url, auth)

print(f"Total roles exported: {len(all_roles)}")

# Save to file
with open('complete_roles_export.json', 'w') as f:
    json.dump({'vList': all_roles}, f, indent=2)