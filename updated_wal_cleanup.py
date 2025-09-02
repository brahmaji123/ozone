#!/usr/bin/env python3
import boto3
import logging
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, EndpointConnectionError

# === Configuration ===
OZONE_ENDPOINT = "http://your-ozone-endpoint:9878"
OZONE_ACCESS_KEY = "your_ozone_access_key"
OZONE_SECRET_KEY = "your_ozone_secret_key"
OZONE_BUCKET = "your_ozone_bucket_name"

CLUSTER_NAME = "clusterA"   # <<< CHANGE ME
PSQLD_NODE = "psqld3"       # <<< CHANGE ME
OZONE_BASE_FOLDER = "wal_backups"
RETENTION_DAYS = 15

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"/var/log/postgresql/{CLUSTER_NAME}_{PSQLD_NODE}_wal_cleanup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def initialize_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=OZONE_ENDPOINT,
        aws_access_key_id=OZONE_ACCESS_KEY,
        aws_secret_access_key=OZONE_SECRET_KEY,
        config=boto3.session.Config(
            connect_timeout=10,
            read_timeout=30,
            retries={'max_attempts': 3}
        )
    )

def delete_old_wal_folders(cluster_name, psqld_node):
    s3_client = initialize_s3_client()
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)

    prefix = f"{cluster_name}/{OZONE_BASE_FOLDER}/{psqld_node}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=OZONE_BUCKET, Prefix=prefix)

    folders = {}

    # Collect all date-based folders
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            # Expect cluster/wal_backups/psqld3/YYYY-MM-DD/file
            if len(parts) >= 4:
                date_str = parts[3]
                try:
                    folder_date = datetime.strptime(date_str, "%Y-%m-%d")
                    folders[date_str] = folder_date
                except ValueError:
                    continue

    # Delete older than cutoff
    for folder, folder_date in folders.items():
        if folder_date < cutoff_date:
            logger.info(f"Deleting WAL folder {folder} under {cluster_name}/{psqld_node} (older than {RETENTION_DAYS} days)")
            folder_prefix = f"{cluster_name}/{OZONE_BASE_FOLDER}/{psqld_node}/{folder}/"

            del_page_iterator = paginator.paginate(Bucket=OZONE_BUCKET, Prefix=folder_prefix)
            for del_page in del_page_iterator:
                objects_to_delete = [{"Key": obj["Key"]} for obj in del_page.get("Contents", [])]
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    s3_client.delete_objects(Bucket=OZONE_BUCKET, Delete={"Objects": batch})
                    logger.info(f"Deleted {len(batch)} objects from {folder}")

if __name__ == "__main__":
    try:
        delete_old_wal_folders(CLUSTER_NAME, PSQLD_NODE)
    except (NoCredentialsError, EndpointConnectionError) as e:
        logger.error(f"Ozone connection failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
