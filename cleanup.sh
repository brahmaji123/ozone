#!/bin/bash

# ===============================
# Script: databasehuecleanup
# Description:
#   - Cleans Hue desktop documents
#   - Deletes old data from RMAN and Oozie PostgreSQL databases
#   - Uses one shared DB user (from service_id_pg1)
#   - Password is retrieved from get_pass.sh
# Usage:
#   ./databasehuecleanup <cluster_name> <keep_days>
# ===============================

# ---- SSH as root helper function ----
rssh() {
  local host="$1"
  shift
  ssh "$host" -l root "$@"
}

# ---- Usage help ----
usage() {
  echo "Usage: $0 <cluster_name> <keep_days>"
  echo "Example: $0 cluster1 30"
  exit 1
}

# ---- Argument validation ----
if [ $# -ne 2 ]; then
  usage
fi

CLUSTER_NAME="$1"
KEEP_DAYS="$2"

if ! [[ "$KEEP_DAYS" =~ ^[0-9]+$ ]]; then
  echo "Error: <keep_days> must be numeric."
  usage
fi

# ---- Setup logging ----
LOG_DIR="/var/log/hue_cleanup"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_FILE="$LOG_DIR/hue_cleanup_${CLUSTER_NAME}_${TIMESTAMP}.log"

echo "[$(date)] Starting full cleanup for cluster: $CLUSTER_NAME" | tee -a "$LOG_FILE"

# ---- Load DB user from cluster vars ----
VARS_FILE="/app/admin/playbook/group_vars/$CLUSTER_NAME/vars"
if [ ! -f "$VARS_FILE" ]; then
  echo "[$(date)] ERROR: Vars file not found: $VARS_FILE" | tee -a "$LOG_FILE"
  exit 1
fi

PG_USER=$(grep -E '^service_id_pg1:' "$VARS_FILE" | awk -F': ' '{print $2}' | tr -d '"'\''[:space:]')

if [ -z "$PG_USER" ]; then
  echo "[$(date)] ERROR: service_id_pg1 not defined in $VARS_FILE" | tee -a "$LOG_FILE"
  exit 1
fi

# ---- Get password for PG_USER ----
PG_PASSWORD=$(./get_pass.sh "$PG_USER")
if [ -z "$PG_PASSWORD" ]; then
  echo "[$(date)] ERROR: Failed to get password for $PG_USER" | tee -a "$LOG_FILE"
  exit 1
fi

# ---- Get Hue host ----
HUE_HOST=$(hget hue "$CLUSTER_NAME" | grep -E '^[a-zA-Z0-9.-]+$' | head -n 1 | tr -d '"'\''[:space:]')
if [ -z "$HUE_HOST" ]; then
  echo "[$(date)] ERROR: No valid Hue host found for $CLUSTER_NAME" | tee -a "$LOG_FILE"
  exit 1
fi

# ---- Hue document cleanup ----
echo "[$(date)] Cleaning up Hue documents on $HUE_HOST (keep-days: $KEEP_DAYS)" | tee -a "$LOG_FILE"
rssh "$HUE_HOST" "cd /opt/cloudera/parcels/CDH/lib/hue && DESKTOP_DEBUG=True ./build/env/bin/hue desktop_document_cleanup --keep-days $KEEP_DAYS --cm-managed" >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
  echo "[$(date)] SUCCESS: Hue cleanup completed" | tee -a "$LOG_FILE"
else
  echo "[$(date)] ERROR: Hue cleanup failed" | tee -a "$LOG_FILE"
  exit 1
fi

# ---- Get PG host for this cluster ----
PG_HOST=$(hget -A env "${CLUSTER_NAME}psql" | grep -E '^[a-zA-Z0-9.-]+$' | head -n 1 | tr -d '"'\''[:space:]')
if [ -z "$PG_HOST" ]; then
  echo "[$(date)] ERROR: No valid PostgreSQL host found for $CLUSTER_NAME" | tee -a "$LOG_FILE"
  exit 1
fi

# ---- RMAN DB cleanup with row count ----
echo "[$(date)] Cleaning up RMAN DB on $PG_HOST" | tee -a "$LOG_FILE"
RMAN_SQL="
WITH deleted AS (
  DELETE FROM rman_usergrouphistory
  WHERE sample_date <= (extract(epoch FROM now() - interval '1 month') * 1000)
  RETURNING *
)
SELECT 'RMAN rows deleted:', COUNT(*) FROM deleted;
"
rssh "$PG_HOST" "PGPASSWORD='$PG_PASSWORD' psql -p 5432 -d rman -U $PG_USER -c \"$RMAN_SQL\"" >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
  echo "[$(date)] SUCCESS: RMAN cleanup completed" | tee -a "$LOG_FILE"
else
  echo "[$(date)] ERROR: RMAN cleanup failed" | tee -a "$LOG_FILE"
  exit 1
fi

# ---- Oozie DB cleanup with row count ----
echo "[$(date)] Cleaning up Oozie DB on $PG_HOST" | tee -a "$LOG_FILE"
OOZIE_SQL="
DO \$\$
DECLARE
  act_del INT;
  job_del INT;
BEGIN
  DELETE FROM wf_action WHERE created_time < now() - interval '1 month' RETURNING * INTO act_del;
  DELETE FROM wf_jobs WHERE created_time < now() - interval '1 month' RETURNING * INTO job_del;
  RAISE NOTICE 'wf_action rows deleted: %', act_del;
  RAISE NOTICE 'wf_jobs rows deleted: %', job_del;
END
\$\$;
"
rssh "$PG_HOST" "PGPASSWORD='$PG_PASSWORD' psql -p 5432 -d oozie -U $PG_USER -v ON_ERROR_STOP=1 -c \"$OOZIE_SQL\"" >> "$LOG_FILE" 2>&1
if [ $? -eq 0 ]; then
  echo "[$(date)] SUCCESS: Oozie cleanup completed" | tee -a "$LOG_FILE"
else
  echo "[$(date)] ERROR: Oozie cleanup failed" | tee -a "$LOG_FILE"
  exit 1
fi

# ---- Final status ----
echo "[$(date)] ✅ All cleanup tasks completed successfully for cluster: $CLUSTER_NAME" | tee -a "$LOG_FILE"
