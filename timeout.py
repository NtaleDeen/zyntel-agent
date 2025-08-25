# ====================================================================
# Automated Z: Drive Data Fetching & TimeOut.csv Update Pipeline
#
# This version of the script standardizes the date format for
# the CreationTime field and filters out invalid date entries.
# It now handles multiple input date formats and outputs a
# single, consistent format required by downstream files.
# This version also includes comprehensive logging and R2 upload.
# ====================================================================

import os
import csv
import datetime
from pathlib import Path
from dotenv import load_dotenv
import sys
import logging
import boto3

# Load environment variables from the .env file
load_dotenv()

# --- Environment and Path Configuration ---
def get_application_base_dir():
    """Determines the application base directory."""
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return base_dir

APPLICATION_BASE_DIR = get_application_base_dir()
PUBLIC_DIR = Path(APPLICATION_BASE_DIR) / 'public'
LOGS_DIR = Path(APPLICATION_BASE_DIR) / 'debug'

# ==== CONFIGURATION - Now loaded from .env ===
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", "Z:/"))
OUTPUT_TIMEOUT_CSV_PATH = PUBLIC_DIR / os.getenv("OUTPUT_TIMEOUT_CSV_NAME", "TimeOut.csv")
LAST_RUN_TIMESTAMP_PATH = PUBLIC_DIR / os.getenv("LAST_RUN_TIMESTAMP_NAME", "last_run.txt")
DEFAULT_START_TIME = datetime.datetime(2025, 5, 1, 0, 0, 0) # May 1st, 2025, 12:00 AM

# Ensure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOGS_DIR, 'timeout_debug.log'))
    ]
)
logger = logging.getLogger('timeout.py')

def get_last_run_timestamp():
    """Reads the last run timestamp from a file or returns the default."""
    if LAST_RUN_TIMESTAMP_PATH.exists():
        try:
            with open(LAST_RUN_TIMESTAMP_PATH, 'r') as f:
                timestamp_str = f.read().strip()
                # Correctly parse the timestamp as an ISO format string
                return datetime.datetime.fromisoformat(timestamp_str)
        except Exception as e:
            logger.warning(f"Warning: Could not read last run timestamp. Using default. Error: {e}")
            return DEFAULT_START_TIME
    return DEFAULT_START_TIME

def save_last_run_timestamp(timestamp):
    """Saves the current run timestamp to a file."""
    try:
        LAST_RUN_TIMESTAMP_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LAST_RUN_TIMESTAMP_PATH, 'w') as f:
            f.write(timestamp.isoformat())
    except Exception as e:
        logger.error(f"Error: Failed to save last run timestamp. Error: {e}")

def format_creation_time(time_string):
    """
    Attempts to parse a variety of date formats and return a standardized string.
    Returns None if the string cannot be parsed as a date.
    """
    date_formats = [
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y %I:%M %p',
        '%#m/%#d/%Y %I:%M %p',
        '%#m/%#d/%Y %H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
    ]
    
    dt_object = None
    for fmt in date_formats:
        try:
            dt_object = datetime.datetime.strptime(time_string, fmt)
            break
        except ValueError:
            continue
    
    if dt_object:
        return dt_object.strftime('%m/%d/%Y %I:%M %p')
    return None

def upload_logs_to_r2():
    """Uploads timeout log files to Cloudflare R2 or S3-compatible storage."""
    r2_endpoint_url = os.getenv('R2_ENDPOINT_URL')
    r2_access_key_id = os.getenv('R2_ACCESS_KEY_ID')
    r2_secret_access_key = os.getenv('R2_SECRET_ACCESS_KEY')
    r2_log_bucket_name = os.getenv('R2_LOG_BUCKET_NAME')
    r2_client_folder = os.getenv('R2_CLIENT_FOLDER')

    if not all([r2_endpoint_url, r2_access_key_id, r2_secret_access_key, r2_log_bucket_name, r2_client_folder]):
        logger.error("R2 credentials not fully configured, including R2_CLIENT_FOLDER. Skipping log upload.")
        return

    session = boto3.session.Session()
    s3_client = session.client(
        's3',
        endpoint_url=r2_endpoint_url,
        aws_access_key_id=r2_access_key_id,
        aws_secret_access_key=r2_secret_access_key
    )

    log_file_path = os.path.join(LOGS_DIR, 'timeout_debug.log')
    if os.path.exists(log_file_path):
        try:
            # Construct the object key with the client folder prefix
            object_key = f"{r2_client_folder}/{os.path.basename(log_file_path)}"
            s3_client.upload_file(log_file_path, r2_log_bucket_name, object_key)
            logger.info(f"Successfully uploaded {os.path.basename(log_file_path)} to R2 bucket '{r2_log_bucket_name}' at key '{object_key}'.")
        except Exception as e:
            logger.error(f"Failed to upload {os.path.basename(log_file_path)} to R2: {e}")
    else:
        logger.warning(f"Log file not found, skipping upload: {log_file_path}")
        
def run_timeout_update():
    """Main function to run the update process."""
    try:
        logger.info("====================================================================")
        logger.info(" Starting Z: Drive Scan for new files...")
        logger.info("====================================================================")

        # ==== 1. GET LAST RUN TIMESTAMP ====
        last_run_time = get_last_run_timestamp()
        current_run_time = datetime.datetime.now()
        logger.info(f" Last successful scan was on: {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # ==== 2. SCAN Z: DRIVE FOR NEW FILES ====
        new_records = []
        logger.info(f"\nScanning '{SOURCE_FOLDER}' for new files created after {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}...")

        merged_data = {}
        # A set for fast lookups of existing filenames
        existing_filenames = set()
        if OUTPUT_TIMEOUT_CSV_PATH.exists():
            logger.info(f"Reading existing data from '{OUTPUT_TIMEOUT_CSV_PATH}'...")
            try:
                with open(OUTPUT_TIMEOUT_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Add existing filenames to the set for fast lookup
                        if row.get('FileName'):
                            existing_filenames.add(row['FileName'])

                        formatted_time = format_creation_time(row.get('CreationTime', ''))
                        if formatted_time:
                            merged_data[row['FileName']] = {'FileName': row['FileName'], 'CreationTime': formatted_time}
                        else:
                            logger.warning(f"Skipping invalid existing creation time for file '{row.get('FileName', 'N/A')}': '{row.get('CreationTime', 'N/A')}'")
                logger.info(f"Successfully read {len(merged_data)} existing records.")
            except Exception as e:
                logger.warning(f" Warning: Could not read existing data. Starting with a fresh dataset. Error: {e}")

        new_files_found_count = 0
        
        if SOURCE_FOLDER.is_dir():
            logger.info("Starting file system walk to find new files...")
            for root, dirs, files in os.walk(SOURCE_FOLDER):
                for file_name in files:
                    base_name = os.path.splitext(os.path.basename(file_name))[0]
                    
                    # Skip files that have already been processed
                    if base_name in existing_filenames:
                        continue

                    file_path = Path(root) / file_name
                    try:
                        creation_time_timestamp = os.path.getctime(file_path)
                        creation_time = datetime.datetime.fromtimestamp(creation_time_timestamp)

                        # Check if the file is new since the last run
                        if creation_time > last_run_time:
                            
                            formatted_time = creation_time.strftime('%m/%d/%Y %I:%M %p')
                            
                            new_record = {'FileName': base_name, 'CreationTime': formatted_time}
                            
                            new_records.append(new_record)
                            new_files_found_count += 1
                            logger.info(f"Found new file: '{file_path}' created at {formatted_time}")
                    except Exception as e:
                        logger.warning(f"Could not get creation time for file '{file_path}': {e}")
            logger.info(f"Finished file system walk. Found a total of {new_files_found_count} new files.")
        else:
            logger.error(f"Source folder '{SOURCE_FOLDER}' does not exist or is not a directory.")

        if new_files_found_count == 0:
            logger.info(" No new files found since last scan.")

        logger.info(f"Merging {new_files_found_count} new records with existing data...")
        for record in new_records:
            merged_data[record['FileName']] = record
        
        # ==== 3. EXPORT MERGED DATA TO TimeOut.csv ====
        if merged_data:
            logger.info(f"Exporting {len(merged_data)} consolidated records to '{OUTPUT_TIMEOUT_CSV_PATH}'...")
            try:
                OUTPUT_TIMEOUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
                fieldnames = ['FileName', 'CreationTime']
                with open(OUTPUT_TIMEOUT_CSV_PATH, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(merged_data.values())
                logger.info(f"\n Successfully updated '{OUTPUT_TIMEOUT_CSV_PATH}'.\n")
                
                save_last_run_timestamp(current_run_time)
                logger.info(f" Saved new last run timestamp: {current_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
            except Exception as e:
                logger.error(f" Error: Failed to export merged data. Error: {e}")
        else:
            logger.info(f"\n No records to export to '{OUTPUT_TIMEOUT_CSV_PATH}'. File remains unchanged.\n")

        logger.info("====================================================================")
        logger.info(" Scan complete.")
    finally:
        upload_logs_to_r2()

if __name__ == "__main__":
    run_timeout_update()