import os
from datetime import datetime, timedelta

# Configuration
PG_WAL_DIRECTORY = '/var/lib/postgresql/data/pg_wal'  # PostgreSQL WAL directory

def get_wal_files_for_today():
    """List WAL files modified today in the PostgreSQL WAL directory."""
    # Get today's date
    today = datetime.now().date()

    # Initialize a list to store WAL files modified today
    wal_files_today = []

    # Iterate through all files in the WAL directory
    for file_name in os.listdir(PG_WAL_DIRECTORY):
        # Check if the file is a WAL file (starts with '0000' and ends with '.partial')
        if file_name.startswith('0000') and file_name.endswith('.partial'):
            file_path = os.path.join(PG_WAL_DIRECTORY, file_name)
            # Get the modification time of the file
            modification_time = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
            # Check if the file was modified today
            if modification_time == today:
                wal_files_today.append(file_name)

    return wal_files_today

def main():
    # Get WAL files modified today
    wal_files_today = get_wal_files_for_today()

    # Print the list of WAL files for today
    if wal_files_today:
        print("WAL files modified today:")
        for file_name in wal_files_today:
            print(file_name)
    else:
        print("No WAL files were modified today.")

if __name__ == "__main__":
    main()
