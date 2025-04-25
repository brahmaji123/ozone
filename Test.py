import requests
import json

ranger_url = "http://<ranger-host>:6080/service/roles/roles"
auth = ('admin', 'password')

with open('all_roles_export.json') as f:
    roles = json.load(f)['vList']

for role in roles:
    # Remove ID if exists to force creation as new role
    if 'id' in role:
        del role['id']
    
    response = requests.post(
        ranger_url,
        auth=auth,
        headers={'Content-Type': 'application/json'},
        json=role
    )
    
    if response.status_code == 200:
        print(f"Successfully imported {role['name']}")
    else:
        print(f"Failed to import {role['name']}: {response.text}")
