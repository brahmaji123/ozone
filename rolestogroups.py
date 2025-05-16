import json

def tag_roles_to_groups(export_json_path, output_path=None):
    # Load export.json
    with open(export_json_path, 'r') as f:
        data = json.load(f)

    # Ensure roles key exists
    if 'roles' not in data:
        raise ValueError("No 'roles' key found in export.json")

    updated_roles = []
    for role in data['roles']:
        role_name = role.get('name')
        groups = role.get('groups', [])

        if not isinstance(groups, list):
            print(f"Skipping role {role_name}: groups is not a list")
            continue

        # Modify the role as needed (for example, normalize group names)
        role['groups'] = sorted(set(groups))  # Remove duplicates if any
        updated_roles.append(role)

        print(f"Tagged role '{role_name}' with groups: {role['groups']}")

    # Update the data
    data['roles'] = updated_roles

    # Write output
    output_file = output_path or export_json_path.replace('.json', '_tagged.json')
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nUpdated roles saved to: {output_file}")

# Example usage
tag_roles_to_groups("export.json")
