import json

def move_roles_to_groups(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected a list at the top level of JSON")

    for entry in data:
        # Safely handle missing keys
        roles = entry.get("roles", [])
        groups = set(entry.get("groups", []))

        # Move each role to the groups list
        for role in roles:
            groups.add(role)

        # Update the entry
        entry["groups"] = sorted(groups)
        entry["roles"] = []  # Clear the roles

    # Write updated JSON to output file
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"✅ Done! Roles moved to groups. Output written to: {output_file}")

# Example usage
move_roles_to_groups("export.json", "export_tagged.json")
