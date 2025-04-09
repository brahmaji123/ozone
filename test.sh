#!/bin/bash

# PostgreSQL Cluster Status Checker
# Usage: ./pgstatus.sh [cluster1,cluster2,...] or ./pgstatus.sh and enter clusters when prompted

# Define clusters with special hget modes
declare -A CLUSTER_MODES=(
    [uatpdp]="-M"
    [uat1pdp]="-M"
    [drpdp]="-C"
    [prodpdp]="-C"
    # All others will use default "hget env"
)

# List of all valid clusters
ALL_CLUSTERS=(iapdp devpdp uatpdp uat1pdp dev1pdp drpdp prodpdp)

# Display usage information
usage() {
    echo "Usage: $0 [cluster1,cluster2,...]"
    echo "Available clusters: ${ALL_CLUSTERS[*]}"
    exit 1
}

# Main function to check cluster status
check_cluster_status() {
    local cluster=$1
    local hget_cmd
    
    # Determine hget command
    if [[ -n "${CLUSTER_MODES[$cluster]}" ]]; then
        hget_cmd="hget ${CLUSTER_MODES[$cluster]} env $cluster"
    else
        hget_cmd="hget env $cluster"  # Default mode
    fi

    # Execute and display results
    echo -e "\n======= Cluster: $cluster ======="
    echo "Command: $hget_cmd"
    
    if ! $hget_cmd | rpusshq "systemctl status postgresql@psqld{1,2,3}" | grep active; then
        echo "[WARNING] Failed to get status for $cluster"
        return 1
    fi
    return 0
}

# Process input clusters
process_clusters() {
    local input_clusters=$1
    local clusters
    local invalid_count=0
    
    IFS=',' read -ra clusters <<< "$input_clusters"
    for cluster in "${clusters[@]}"; do
        cluster=$(echo "$cluster" | xargs)  # Trim whitespace

        # Validate cluster
        if [[ " ${ALL_CLUSTERS[*]} " != *" $cluster "* ]]; then
            echo -e "\n[ERROR] Invalid cluster: $cluster (Skipping)"
            ((invalid_count++))
            continue
        fi

        check_cluster_status "$cluster"
    done
    
    return $invalid_count
}

# Main execution
main() {
    local input_clusters
    
    # Check if clusters were provided as arguments
    if [ $# -ge 1 ]; then
        input_clusters="$1"
    else
        echo "Available clusters: ${ALL_CLUSTERS[*]}"
        read -p "Enter cluster(s) (comma-separated): " input_clusters
        [ -z "$input_clusters" ] && usage
    fi

    process_clusters "$input_clusters"
}

# Execute main function
main "$@"
