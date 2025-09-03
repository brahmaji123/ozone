#!/bin/bash
set -euo pipefail

# Prompt the user
read -p "Enter Backup Cluster (e.g. PVOS / PTOS): " BACKUP_CLUSTER
read -p "Enter Cluster Name (e.g. ATB123 / AVB456): " CLUSTER
read -p "Enter Instance (e.g. psqld1, psqld2, psqld3): " INSTANCE
read -p "Enter Date (YYYY-MM-DD, leave empty for today): " DATE
read -p "Enter Backup Type (SQL / Filesystem / WAL): " BACKUP_TYPE

if [[ -z "$DATE" ]]; then
  DATE=$(date +%F)
fi

# Export your custom CA bundle
export AWS_CA_BUNDLE="/path/to/tls-ca-bundle.pem"
echo "Using CA bundle at: $AWS_CA_BUNDLE"

# Call the Python downloader
python3 <<EOF
import os, sys, boto3
from datetime import datetime

backup_cluster = "$BACKUP_CLUSTER"
cluster = "$CLUSTER"
instance = "$INSTANCE"
date_str = "$DATE"
backup_type = "$BACKUP_TYPE"

local_base = "/app/admin/postgresozonedownload"
local_dir = f"{local_base}/{backup_cluster}/{cluster}/{instance}"
os.makedirs(local_dir, exist_ok=True)

# Endpoint & credentials config
pv_os = {"endpoint":"http://virginia-ozone-s3:9878", "access":"virginia_key", "secret":"virginia_secret"}
pt_os = {"endpoint":"http://texas-ozone-s3:9878",     "access":"texas_key",    "secret":"texas_secret"}

cfg = pv_os if backup_cluster == "PVOS" else pt_os if backup_cluster == "PTOS" else None
if cfg is None:
    print("Error: backup cluster must be PVOS or PTOS"); sys.exit(1)

s3 = boto3.client(
    "s3",
    endpoint_url=cfg["endpoint"],
    aws_access_key_id=cfg["access"],
    aws_secret_access_key=cfg["secret"]
)

def download_key(key, dest, recursive=False):
    try:
        if recursive:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket="pgbackup", Prefix=key):
                for obj in page.get("Contents", []):
                    rel = os.path.relpath(obj["Key"], key)
                    tgt = os.path.join(dest, rel)
                    os.makedirs(os.path.dirname(tgt), exist_ok=True)
                    print(f"Downloading {obj['Key']} → {tgt}")
                    s3.download_file("pgbackup", obj["Key"], tgt)
        else:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            print(f"Downloading {key} → {dest}")
            s3.download_file("pgbackup", key, dest)
    except Exception as e:
        print(f"Skipping {key}: {e}")

if backup_type == "Filesystem":
    # Filesystem backup
    download_key(f"{backup_cluster}/{cluster}/file_systembackups/{instance}/{date_str}.tar.gz",
                 f"{local_dir}/file_systembackups/{date_str}.tar.gz")
elif backup_type == "SQL":
    # SQL backups (filter by date)
    prefix = f"{backup_cluster}/{cluster}/sql_backups/{instance}/"
    date_compact = date_str.replace("-", "")  # convert YYYY-MM-DD → YYYYMMDD
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="pgbackup", Prefix=prefix):
        for obj in page.get("Contents", []):
            if date_compact in os.path.basename(obj["Key"]):
                tgt = os.path.join(local_dir, "sql_backups", os.path.basename(obj["Key"]))
                os.makedirs(os.path.dirname(tgt), exist_ok=True)
                print(f"Downloading {obj['Key']} → {tgt}")
                s3.download_file("pgbackup", obj["Key"], tgt)
elif backup_type == "WAL":
    # WAL backups
    download_key(f"{backup_cluster}/{cluster}/wal_backups/{instance}/{date_str}/",
                 f"{local_dir}/wal_backups", recursive=True)
else:
    print(f"Error: Invalid backup type '{backup_type}'. Must be one of: SQL, Filesystem, WAL.")
    sys.exit(1)

print("Download complete.")
EOF

echo "Done. Files saved under: /app/admin/postgresozonedownload/${BACKUP_CLUSTER}/${CLUSTER}/${INSTANCE}/"
