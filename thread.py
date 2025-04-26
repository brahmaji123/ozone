import requests
import json
import concurrent.futures
import threading
from typing import List, Dict

# Configuration
RANGER_HOST = "your.ranger.server.com"
RANGER_USER = "admin"
RANGER_PASSWORD = "password"
INPUT_FILE = "all_roles_export.json"
MAX_WORKERS = 10  # Adjust based on your server capacity (10-20 is usually safe)

# API endpoint
ranger_url = f"http://{RANGER_HOST}:6080/service/roles/roles"

# Thread-local storage for session management
thread_local = threading.local()

def get_session() -> requests.Session:
    """Get a thread-local requests session with authentication"""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.auth = (RANGER_USER, RANGER_PASSWORD)
    return thread_local.session

def get_roles_from_file(data: Dict) -> List[Dict]:
    """Extract roles from JSON data handling different structures"""
    if 'vList' in data:
        return data['vList']
    elif 'roles' in data:
        return data['roles']
    elif isinstance(data, list):
        return data
    else:
        raise ValueError("Could not find roles list in JSON data")

def prepare_role_data(role: Dict) -> Dict:
    """Prepare role data for import by removing unwanted fields"""
    role_data = role.copy()
    role_data.pop('id', None)  # Safely remove 'id' if it exists
    return role_data

def import_single_role(session: requests.Session, role_data: Dict, index: int, total: int) -> Dict:
    """Import a single role and return the result"""
    role_name = role_data.get('name', f'unnamed-role-{index}')
    
    try:
        response = session.post(
            ranger_url,
            headers={'Content-Type': 'application/json'},
            json=role_data,
            timeout=30
        )
        
        result = {
            'index': index,
            'name': role_name,
            'success': response.status_code == 200,
            'status': response.status_code,
            'message': response.text if response.status_code != 200 else None
        }
        
        # Print progress every 100 roles or for the last role
        if index % 100 == 0 or index == total:
            status = "SUCCESS" if result['success'] else "FAILED"
            print(f"Processed {index}/{total} - {role_name} - {status}")
            
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {role_name}: {str(e)}")
        return {
            'index': index,
            'name': role_name,
            'success': False,
            'status': None,
            'message': str(e)
        }

def import_roles():
    """Main function to import roles using threading"""
    try:
        # Load and parse input file
        with open(INPUT_FILE) as f:
            data = json.load(f)
            
        roles = get_roles_from_file(data)
        
        if not roles:
            print("No roles found in the input file")
            return

        total_roles = len(roles)
        print(f"Found {total_roles} roles to import")
        print(f"Using {MAX_WORKERS} parallel workers...")

        # Process roles in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            session = get_session()
            futures = []
            
            for index, role in enumerate(roles, 1):
                role_data = prepare_role_data(role)
                futures.append(
                    executor.submit(
                        import_single_role,
                        session=session,
                        role_data=role_data,
                        index=index,
                        total=total_roles
                    )
                )

            # Collect results
            success_count = 0
            failed_count = 0
            failed_roles = []
            
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result['success']:
                    success_count += 1
                else:
                    failed_count += 1
                    failed_roles.append(result)
            
            # Print summary
            print("\nImport completed!")
            print(f"Successfully imported: {success_count}")
            print(f"Failed to import: {failed_count}")
            
            if failed_roles:
                print("\nFailed roles:")
                for failed in failed_roles[:10]:  # Print first 10 failures
                    print(f"#{failed['index']} {failed['name']} - Status: {failed['status']} - Error: {failed['message']}")
                
                # Optionally save all failures to a file
                with open('failed_imports.json', 'w') as f:
                    json.dump(failed_roles, f, indent=2)
                print("\nFull list of failed imports saved to 'failed_imports.json'")
    
    except json.JSONDecodeError:
        print("Error: Input file is not valid JSON")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    import_roles()
