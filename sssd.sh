#!/bin/bash
# Script: check_var_sssd_cleanup.sh

# Prompt for cluster name
read -rp "Enter the cluster name: " CLUSTER_NAME
if [ -z "$CLUSTER_NAME" ]; then
    echo "❌ Cluster name cannot be empty."
    exit 1
fi

# Create log file
LOG_FILE="/tmp/${CLUSTER_NAME}_sssd_cleanup_$(date +%F_%H%M%S).log"

# Check /var usage
VAR_USAGE=$(df -h /var | awk 'NR==2 {gsub("%","",$5); print $5}')
HOSTNAME=$(hostname -f)

{
    echo "=== SSSD Cleanup Script ==="
    echo "Cluster: $CLUSTER_NAME"
    echo "Host: $HOSTNAME"
    echo "Date: $(date)"
    echo "Current /var usage: ${VAR_USAGE}%"
    echo "---------------------------"

    if [ "$VAR_USAGE" -gt 95 ]; then
        echo " /var usage is ${VAR_USAGE}%, starting cleanup..."
        
        echo "Stopping sssd..."
        sudo systemctl stop sssd

        echo "Removing /var/log/sssd/* ..."
        sudo rm -rf /var/log/sssd/*

        echo "Starting sssd..."
        sudo systemctl start sssd

        echo " Cleanup completed successfully."
    else
        echo " /var usage is ${VAR_USAGE}%, no cleanup required."
    fi

    echo "Script finished at: $(date)"
} | tee "$LOG_FILE"

echo " Log saved to $LOG_FILE"
