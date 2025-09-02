#!/bin/bash

# Prompt the user
read -p "Enter Cluster Name (e.g. UTENTR/DTENTR): " CLUSTER
read -p "Enter Instance (e.g. psqld1, psqld2, psqld3): " INSTANCE
read -p "Enter Date (YYYY-MM-DD, leave empty for today): " DATE

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

cluster = "$CLUSTER"
instance = "$INSTANCE"
date_str = "$DATE"
local_base = "/app/admin/postgresozonedownload"
local_dir = f"{local_base}/{cluster}/{instance}"
os.makedirs(local_dir, exist_ok=True)

# Endpoint & credentials config
virginia = {"endpoint":"http://virginia-ozone-s3:9878", "access":"virginia_key", "secret":"virginia_secret"}
texas   = {"endpoint":"http://texas-ozone-s3:9878",     "access":"texas_key",    "secret":"texas_secret"}

cfg = virginia if "ATB" in cluster else texas if "AVB" in cluster else None
if cfg is None:
    print("Error: cluster must contain ATB or AVB"); sys.exit(1)

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

# Filesystem backup
download_key(f"{cluster}/file_systembackups/{instance}/{date_str}.tar.gz",
             f"{local_dir}/file_systembackups/{date_str}.tar.gz")

# SQL backups (filter by date)
prefix = f"{cluster}/sql_backups/{instance}/"
for page in s3.get_paginator("list_objects_v2").paginate(Bucket="pgbackup", Prefix=prefix):
    for obj in page.get("Contents", []):
        if date_str in obj["Key"]:
            tgt = os.path.join(local_dir, "sql_backups", os.path.basename(obj["Key"]))
            os.makedirs(os.path.dirname(tgt), exist_ok=True)
            print(f"Downloading {obj['Key']} → {tgt}")
            s3.download_file("pgbackup", obj["Key"], tgt)

# WAL backups
download_key(f"{cluster}/wal_backups/{instance}/{date_str}/",
             f"{local_dir}/wal_backups", recursive=True)

print("Download complete.")
EOF

echo "Done. Files saved under: /app/admin/postgresozonedownload/${CLUSTER}/${INSTANCE}/"
