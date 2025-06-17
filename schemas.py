import json
import re

# Load the input JSON file
with open("ranger_policies.json", "r") as infile:
    policies = json.load(infile)

# Process each policy
for policy in policies:
    if policy.get("service") != "cm_hive":
        continue  # Skip non-cm_hive services

    resources = policy.get("resources", {})
    
    # Only process if 'url' exists in 'resources'
    if "url" in resources:
        url_obj = resources["url"]
        values = url_obj.get("values", [])

        new_values = []
        for val in values:
            # Replace '/schemas/' with '/datafiles/' and remove trailing '.db'
            updated = val.replace("/schemas/", "/datafiles/")
            updated = re.sub(r'\.db$', '', updated)
            new_values.append(updated)

        # Update the values
        policy["resources"]["url"]["values"] = new_values

# Save updated policies
with open("ranger_policies_updated.json", "w") as outfile:
    json.dump(policies, outfile, indent=4)

print("✅ Finished updating cm_hive policies with URL paths.")