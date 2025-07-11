#!/bin/bash

# ===============================
# Script: databasehuecleanup
# Description: Runs Hue desktop document cleanup on the first Hue host of the given cluster.
# Usage: databasehuecleanup <cluster_name> <keep_days>
# Example: databasehuecleanup mycluster 30
# ===============================

LOG_DIR="/var/log/hue_cleanup"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_FILE="$LOG_DIR/hue_cleanup_${TIMESTAMP}.log"

# Function: Print usage
usage() {
  echo "Usage: $0 <cluster_name> <keep_days>"
  echo "Example: $0 cluster1 30"
  exit 1
}

# Check arguments
if [ $# -ne 2 ]; then
  usage
fi

CLUSTER_NAME="$1"
KEEP_DAYS="$2"

# Validate keep_days is a number
if ! [[ "$KEEP_DAYS" =~ ^[0-9]+$ ]]; then
  echo "Error: keep_days must be a number."
  usage
fi

echo "[$(date)] Starting cleanup for cluster: $CLUSTER_NAME with retention: $KEEP_DAYS days" | tee -a "$LOG_FILE"

# Get list of Hue hosts
HUE_HOSTS=$(hget hue "$CLUSTER_NAME")
if [ -z "$HUE_HOSTS" ]; then
  echo "[$(date)] ERROR: No Hue hosts found for cluster: $CLUSTER_NAME" | tee -a "$LOG_FILE"
  exit 1
fi

# Select the first host
HUE_HOST=$(echo "$HUE_HOSTS" | awk '{print $1}')
if [ -z "$HUE_HOST" ]; then
  echo "[$(date)] ERROR: Unable to determine the first Hue host" | tee -a "$LOG_FILE"
  exit 1
fi

echo "[$(date)] Running cleanup on host: $HUE_HOST..." | tee -a "$LOG_FILE"

# Execute the cleanup remotely
ssh "$HUE_HOST" "cd /opt/cloudera/parcels/CDH/lib/hue && DESKTOP_DEBUG=True ./build/env/bin/hue desktop_document_cleanup --keep-days $KEEP_DAYS --cm-managed" >> "$LOG_FILE" 2>&1

# Capture exit code
if [ $? -eq 0 ]; then
  echo "[$(date)] SUCCESS: Cleanup completed on $HUE_HOST" | tee -a "$LOG_FILE"
else
  echo "[$(date)] ERROR: Cleanup failed on $HUE_HOST" | tee -a "$LOG_FILE"
  exit 1
fi
