#!/bin/bash
# Script: check_var_sssd_cleanup.sh

# Get cluster name
CLUSTER_NAME=$(hget env clustername 2>/dev/null)

# If hget fails
if [ -z "$CLUSTER_NAME" ]; then
    echo "❌ Could not retrieve cluster name using 'hget env clustername'"
    exit 1
fi

# Check /var usage
VAR_USAGE=$(df -h /var | awk 'NR==2 {gsub("%","",$5); print $5}')

if [ "$VAR_USAGE" -gt 95 ]; then
    HOSTNAME=$(hostname -f)
    echo "⚠️ /var usage is ${VAR_USAGE}%. Cluster: $CLUSTER_NAME, Host: $HOSTNAME"
    
    echo "Stopping sssd..."
    sudo systemctl stop sssd

    echo "Cleaning /var/log/sssd/* ..."
    sudo rm -rf /var/log/sssd/*

    echo "Starting sssd..."
    sudo systemctl start sssd

    echo "✅ Cleanup completed for $HOSTNAME"
else
    echo "✅ /var usage is ${VAR_USAGE}%, no action needed."
fi
