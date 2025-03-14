import os
import time
import logging
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import NoCredentialsError, EndpointConnectionError

# Configuration
PG_WAL_DIRECTORY = '/var/lib/postgresql/data/pg_wal'  # PostgreSQL WAL directory
LOCAL_BACKUP_DIR = '/path/to/local/wal_backup'  # Local backup directory for WAL files

OZONE_ENDPOINT = 'http://your-ozone-endpoint:9878'  # Ozone endpoint
OZONE_ACCESS_KEY = 'your_ozone_access_key'  # Ozone access key
OZONE_SECRET_KEY = 'your_ozone_secret_key'  # Ozone secret key
OZONE_BUCKET = 'your_ozone_bucket_name'  # Ozone bucket name
OZONE_BASE_FOLDER = 'wal_backups'  # Base folder in the Ozone bucket

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='wal_upload.log'
)
logger = logging.getLogger()

def ensure_local_backup_dir():
    """Ensure the local backup directory exists."""
    if not os.path.exists(LOCAL_BACKUP_DIR):
        os.makedirs(LOCAL_BACKUP_DIR)
        logger.info(f"Created local backup directory: {LOCAL_BACKUP_DIR}")

def get_wal_files():
    """Fetch the list of WAL files in the pg_wal directory."""
    return [f for f in os.listdir(PG_WAL_DIRECTORY) if f.startswith('0000') and f.endswith('.partial')]

def get_date_from_wal_file(file_name):
    """Extract the date from the WAL file's modification time."""
    file_path = os.path.join(PG_WAL_DIRECTORY, file_name)
    modification_time = os.path.getmtime(file_path)
    return datetime.fromtimestamp(modification_time).strftime('%Y-%m-%d')

def upload_wal_file_to_ozone(file_name):
    """Upload a WAL file to the Ozone bucket, organized by date under the base folder."""
    file_path = os.path.join(PG_WAL_DIRECTORY, file_name)
    date_folder = get_date_from_wal_file(file_name)
    # Include the base folder in the Ozone key
    ozone_key = f"{OZONE_BASE_FOLDER}/{date_folder}/{file_name}"

    try:
        s3_client.upload_file(file_path, OZONE_BUCKET, ozone_key)
        logger.info(f"Uploaded {file_name} to Ozone bucket {OZONE_BUCKET} under {ozone_key}")
        return True
    except (NoCredentialsError, EndpointConnectionError) as e:
        logger.error(f"Ozone cluster is down or inaccessible: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to upload {file_name}: {e}")
        return False

def move_to_local_backup(file_name):
    """Move a WAL file to the local backup directory."""
    src_path = os.path.join(PG_WAL_DIRECTORY, file_name)
    dest_path = os.path.join(LOCAL_BACKUP_DIR, file_name)
    try:
        os.rename(src_path, dest_path)
        logger.info(f"Moved {file_name} to local backup directory: {LOCAL_BACKUP_DIR}")
    except Exception as e:
        logger.error(f"Failed to move {file_name} to local backup: {e}")

def retry_local_backup():
    """Retry uploading WAL files from the local backup directory."""
    if not os.path.exists(LOCAL_BACKUP_DIR):
        return

    for file_name in os.listdir(LOCAL_BACKUP_DIR):
        file_path = os.path.join(LOCAL_BACKUP_DIR, file_name)
        date_folder = get_date_from_wal_file(file_name)
        # Include the base folder in the Ozone key
        ozone_key = f"{OZONE_BASE_FOLDER}/{date_folder}/{file_name}"

        try:
            s3_client.upload_file(file_path, OZONE_BUCKET, ozone_key)
            logger.info(f"Uploaded {file_name} from local backup to Ozone bucket {OZONE_BUCKET} under {ozone_key}")
            os.remove(file_path)  # Remove the file after successful upload
        except (NoCredentialsError, EndpointConnectionError) as e:
            logger.error(f"Ozone cluster is still down: {e}")
            break
        except Exception as e:
            logger.error(f"Failed to upload {file_name} from local backup: {e}")

def delete_old_folders():
    """Delete date folders older than 7 days from the Ozone bucket."""
    try:
        # List all objects in the bucket under the base folder
        response = s3_client.list_objects_v2(Bucket=OZONE_BUCKET, Prefix=OZONE_BASE_FOLDER + '/', Delimiter='/')
        if 'CommonPrefixes' not in response:
            return

        # Get current date and calculate the cutoff date (7 days ago)
        cutoff_date = datetime.now() - timedelta(days=7)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')

        # Iterate through date folders and delete old ones
        for folder in response['CommonPrefixes']:
            folder_name = folder['Prefix'].rstrip('/')
            folder_date_str = folder_name.split('/')[-1]  # Extract date from folder name
            folder_date = datetime.strptime(folder_date_str, '%Y-%m-%d')

            if folder_date.strftime('%Y-%m-%d') < cutoff_date_str:
                # Delete all files in the folder
                objects_to_delete = s3_client.list_objects_v2(Bucket=OZONE_BUCKET, Prefix=folder_name)
                if 'Contents' in objects_to_delete:
                    delete_keys = [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]
                    s3_client.delete_objects(Bucket=OZONE_BUCKET, Delete={'Objects': delete_keys})
                    logger.info(f"Deleted folder {folder_name} and its contents from Ozone bucket {OZONE_BUCKET}")
    except Exception as e:
        logger.error(f"Failed to delete old folders: {e}")

def delete_old_local_files():
    """Delete WAL files older than 7 days from the local backup directory."""
    cutoff_date = datetime.now() - timedelta(days=7)
    for file_name in os.listdir(LOCAL_BACKUP_DIR):
        file_path = os.path.join(LOCAL_BACKUP_DIR, file_name)
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if file_mod_time < cutoff_date:
            try:
                os.remove(file_path)
                logger.info(f"Deleted old WAL file from local backup: {file_name}")
            except Exception as e:
                logger.error(f"Failed to delete old WAL file {file_name}: {e}")

def main():
    ensure_local_backup_dir()
    ozone_down = False  # Track if Ozone cluster is down

    while True:
        # Retry uploading any files in the local backup directory
        retry_local_backup()

        # Process new WAL files
        wal_files = get_wal_files()
        for wal_file in wal_files:
            if not upload_wal_file_to_ozone(wal_file):
                # If upload fails, move the file to the local backup directory
                move_to_local_backup(wal_file)
                ozone_down = True
            else:
                ozone_down = False

        # Delete date folders older than 7 days from Ozone
        delete_old_folders()

        # Delete old WAL files from local backup directory if Ozone is up
        if not ozone_down:
            delete_old_local_files()

        # Wait for 10 minutes before the next iteration
        time.sleep(600)

if __name__ == "__main__":
    main()
