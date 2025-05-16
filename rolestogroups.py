import json

def move_roles_to_groups(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected a list at the top level of JSON")

    for entry in data:
        roles = entry.get("roles", [])
        groups = set(entry.get("groups", []))

        # Move roles into groups
        for role in roles:
            groups.add(role)

        entry["groups"] = sorted(groups)
        entry["roles"] = []  # Clear roles

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"✅ Roles moved to groups in all entries. Output: {output_file}")

# Run it
move_roles_to_groups("export.json", "export_tagged.json")
