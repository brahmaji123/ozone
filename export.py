# First get total count of roles
total_roles=$(curl -u admin:password -s -X GET \
  "http://<ranger-host>:6080/service/roles/roles/count" \
  -H "accept: application/json" | jq '.value')

echo "Total roles available: $total_roles"

# Then export all roles with proper pagination
pageSize=200  # Max per page
pages=$(( (total_roles + pageSize - 1) / pageSize ))

for ((page=1; page<=pages; page++)); do
  curl -u admin:password -X GET \
    "http://<ranger-host>:6080/service/roles/roles?pageSize=$pageSize&pageNo=$page" \
    -H "accept: application/json" >> all_roles_paginated.json
  echo >> all_roles_paginated.json  # Add newline between pages
done