import json

input_file = 'policies.json'
output_file = 'policies_fixed.json'

with open(input_file, 'r') as f:
    data = json.load(f)

# data is a dict → extract values into list
policies_list = list(data.values())

with open(output_file, 'w') as f:
    json.dump(policies_list, f, indent=2)

print(f"Converted {len(policies_list)} policies into list format → saved as {output_file}")
