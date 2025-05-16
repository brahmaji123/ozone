import json

def move_roles_to_groups_in_policy_items(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected a list at the top level of JSON")

    for policy in data:
        policy_items = policy.get("policyItems", [])
        for item in policy_items:
            roles = item.get("roles", [])
            groups = set(item.get("groups", []))

            # Move roles into groups and remove from roles
            groups.update(roles)
            item["groups"] = sorted(groups)
            item["roles"] = []  # Clear roles

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"✅ Roles moved to groups in policyItems. Output: {output_file}")

# Run the script
move_roles_to_groups_in_policy_items("export.json", "export_tagged.json")