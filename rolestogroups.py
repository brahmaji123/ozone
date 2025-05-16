import json

def move_roles_to_groups(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)

    roles = data.get("roles", [])
    groups = set(data.get("groups", []))

    # Move all roles into groups
    for role in roles:
        groups.add(role)

    # Update JSON structure
    data["groups"] = sorted(groups)  # optional: sort for readability
    data["roles"] = []               # clear roles
    # "user" remains unchanged

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"✅ Roles moved to groups. Output written to: {output_file}")

# Run it
move_roles_to_groups("export.json", "export_tagged.json")
