#!/bin/bash

# PostgreSQL Cluster Status Checker (Generic)

# Function to check cluster status
check_cluster_status() {
    local cluster=$1
    local hget_cmd="hget -A env $cluster psql"

    echo -e "\n======= Cluster: $cluster ======="
    echo "Command: $hget_cmd"

    if ! $hget_cmd | rpusshq "systemctl status postgresql@psqld{1,2,3}" | grep active; then
        echo "[WARNING] Failed to get status for $cluster"
        return 1
    fi
    return 0
}

# Main
read -p "Enter cluster name(s) in lowercase (comma-separated): " input_clusters
[ -z "$input_clusters" ] && { echo "No clusters entered. Exiting."; exit 1; }

IFS=',' read -ra clusters <<< "$input_clusters"
for cluster in "${clusters[@]}"; do
    cluster=$(echo "$cluster" | xargs)  # trim spaces
    check_cluster_status "$cluster"
done
