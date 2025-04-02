#!/usr/bin/env python3
import os
import sys
import logging
import boto3
import shutil
from botocore.exceptions import NoCredentialsError, EndpointConnectionError
from datetime import datetime

# Configuration
OZONE_ENDPOINT = 'http://your-ozone-endpoint:9878'
OZONE_ACCESS_KEY = 'your_ozone_access_key'
OZONE_SECRET_KEY = 'your_ozone_secret_key'
OZONE_BUCKET = 'your_ozone_bucket_name'
OZONE_BASE_FOLDER = 'wal_backups'
LOCAL_FALLBACK_DIR = '/var/lib/postgresql/wal_archive_fallback'  # Local fallback directory
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/postgresql/wal_archiver.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def ensure_local_fallback_dir():
    """Ensure local fallback directory exists"""
    if not os.path.exists(LOCAL_FALLBACK_DIR):
        os.makedirs(LOCAL_FALLBACK_DIR, mode=0o700)
        os.chown(LOCAL_FALLBACK_DIR, os.getuid(), os.getgid())
        logger.info(f"Created local fallback directory: {LOCAL_FALLBACK_DIR}")

def initialize_s3_client():
    """Initialize and return the S3 client for Ozone"""
    return boto3.client(
        's3',
        endpoint_url=OZONE_ENDPOINT,
        aws_access_key_id=OZONE_ACCESS_KEY,
        aws_secret_access_key=OZONE_SECRET_KEY,
        config=boto3.session.Config(
            connect_timeout=10,
            read_timeout=30,
            retries={'max_attempts': 2}
        )
    )

def upload_to_ozone(file_path, file_name):
    """Upload WAL file to Ozone with retries"""
    s3_client = initialize_s3_client()
    date_folder = datetime.now().strftime('%Y-%m-%d')
    ozone_key = f"{OZONE_BASE_FOLDER}/{date_folder}/{file_name}"
    
    for attempt in range(MAX_RETRIES):
        try:
            s3_client.upload_file(file_path, OZONE_BUCKET, ozone_key)
            logger.info(f"Successfully uploaded {file_name} to Ozone")
            return True
        except (NoCredentialsError, EndpointConnectionError) as e:
            logger.warning(f"Ozone connection failed (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            continue
        except Exception as e:
            logger.error(f"Upload failed for {file_name}: {e}")
            return False
    return False

def save_to_local_fallback(file_path, file_name):
    """Save WAL file to local fallback directory"""
    try:
        dest_path = os.path.join(LOCAL_FALLBACK_DIR, file_name)
        shutil.copy2(file_path, dest_path)
        logger.info(f"Saved {file_name} to local fallback directory")
        return True
    except Exception as e:
        logger.error(f"Failed to save {file_name} locally: {e}")
        return False

def retry_fallback_uploads():
    """Retry uploading files from local fallback directory"""
    if not os.path.exists(LOCAL_FALLBACK_DIR):
        return
    
    s3_client = initialize_s3_client()
    
    for file_name in os.listdir(LOCAL_FALLBACK_DIR):
        file_path = os.path.join(LOCAL_FALLBACK_DIR, file_name)
        date_folder = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d')
        ozone_key = f"{OZONE_BASE_FOLDER}/{date_folder}/{file_name}"
        
        try:
            s3_client.upload_file(file_path, OZONE_BUCKET, ozone_key)
            logger.info(f"Successfully uploaded fallback file {file_name}")
            os.remove(file_path)
        except Exception as e:
            logger.warning(f"Failed to upload fallback file {file_name}: {e}")
            break  # Stop trying if we can't connect

def main():
    if len(sys.argv) != 3:
        logger.error("Usage: wal_archiver.py <path> <filename>")
        sys.exit(1)
    
    wal_path = sys.argv[1]
    wal_file = sys.argv[2]
    
    if not os.path.exists(wal_path):
        logger.error(f"WAL file not found: {wal_path}")
        sys.exit(1)
    
    ensure_local_fallback_dir()
    
    # First try uploading to Ozone
    if upload_to_ozone(wal_path, wal_file):
        # On success, check for any fallback files to upload
        retry_fallback_uploads()
        sys.exit(0)
    
    # If Ozone upload failed, save to local fallback
    if save_to_local_fallback(wal_path, wal_file):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
