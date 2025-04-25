import requests
import json
import time

# Configuration
RANGER_HOST = "your.ranger.server.com"
RANGER_USER = "admin"
RANGER_PASSWORD = "password"
INPUT_FILE = "all_roles_export.json"

ranger_url = f"http://{RANGER_HOST}:6080/service/roles/roles"
auth = (RANGER_USER, RANGER_PASSWORD)

def get_roles_from_file(data):
    """Extract roles from JSON data handling different structures"""
    # Try different possible keys
    if 'vList' in data:
        return data['vList']
    elif 'roles' in data:
        return data['roles']
    elif isinstance(data, list):
        return data
    else:
        raise ValueError("Could not find roles list in JSON data")

def import_roles():
    try:
        with open(INPUT_FILE) as f:
            data = json.load(f)
            
            try:
                roles = get_roles_from_file(data)
            except ValueError as e:
                print(f"Error parsing input file: {e}")
                print("Please check your JSON file structure")
                return

            if not roles:
                print("No roles found in the input file")
                return

            print(f"Found {len(roles)} roles to import")

            for index, role in enumerate(roles, 1):
                role_data = role.copy()
                
                if 'id' in role_data:
                    del role_data['id']
                
                if index > 1:
                    time.sleep(0.5)
                
                print(f"Importing {index}/{len(roles)}: {role_data.get('name', 'unnamed-role')}")
                
                try:
                    response = requests.post(
                        ranger_url,
                        auth=auth,
                        headers={'Content-Type': 'application/json'},
                        json=role_data,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        print(f"Successfully imported {role_data.get('name', 'unnamed-role')}")
                    else:
                        print(f"Failed to import {role_data.get('name', 'unnamed-role')}: Status {response.status_code}, Response: {response.text}")
                        
                except requests.exceptions.RequestException as e:
                    print(f"Request failed for {role_data.get('name', 'unnamed-role')}: {str(e)}")
                    
    except json.JSONDecodeError:
        print("Error: Input file is not valid JSON")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    import_roles()