#!/bin/bash

# Prompt for cluster name
read -rp "Enter cluster name: " CLUSTER

# Prompt for instance type
echo "Which instance do you want to connect to?"
echo "d1 - 5432"
echo "d2 - 5433"
echo "d3 - 5434"
read -rp "Enter instance (d1/d2/d3): " INSTANCE

# Map instance to port
case "$INSTANCE" in
  d1) PORT=5432 ;;
  d2) PORT=5433 ;;
  d3) PORT=5434 ;;
  *)
    echo "ERROR: Invalid instance selection"
    exit 1
    ;;
esac

# Constants
GROUP_VARS_FILE="group_vars/${CLUSTER}"
USERNAME="postgres"

# Validate input file
if [[ ! -f "$GROUP_VARS_FILE" ]]; then
  echo "ERROR: File $GROUP_VARS_FILE not found!"
  exit 1
fi

# Check for get_pass.sh
if [[ ! -x "./get_pass.sh" ]]; then
  echo "ERROR: get_pass.sh not found or not executable"
  exit 1
fi

# Get password
PASSWORD=$(./get_pass.sh "$CLUSTER")
if [[ -z "$PASSWORD" ]]; then
  echo "ERROR: Could not retrieve password for cluster $CLUSTER"
  exit 1
fi

# Extract the second FDNS under sans.postgres
FDNS=$(awk '
  $1 == "sans:" { in_sans = 1 }
  in_sans && $1 == "postgres:" { in_pg = 1; next }
  in_pg && $1 ~ /^-/ { count++; if (count == 2) print $2; exit }
' "$GROUP_VARS_FILE")

if [[ -z "$FDNS" ]]; then
  echo "ERROR: Second FDNS not found in $GROUP_VARS_FILE"
  exit 1
fi

echo "Connecting to $FDNS on port $PORT..."

# Connect using psql
PGPASSWORD="$PASSWORD" psql -h "$FDNS" -U "$USERNAME" -p "$PORT" -d postgres -c '\l' || {
  echo "ERROR: Failed to connect to $FDNS:$PORT"
}
