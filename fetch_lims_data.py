import os
import sys
import re
import json
import logging
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path
import boto3

# --- Base Paths ---
def get_application_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

APPLICATION_BASE_DIR = get_application_base_dir()
PUBLIC_DIR = Path(APPLICATION_BASE_DIR) / 'public'
LOGS_DIR = Path(APPLICATION_BASE_DIR) / 'debug'
DATA_JSON_PATH = PUBLIC_DIR / 'data.json'

os.makedirs(PUBLIC_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Load ENV ---
load_dotenv(os.path.join(APPLICATION_BASE_DIR, '.env'))
LIMS_URL = os.getenv('LIMS_URL', 'http://192.168.10.84:8080')
LOGIN_URL = f"{LIMS_URL}/index.php?m=login"
HOME_URL = f"{LIMS_URL}/home.php"
SEARCH_URL = f"{LIMS_URL}/search.php"

LIMS_USER = os.getenv('LIMS_USERNAME')
LIMS_PASSWORD = os.getenv('LIMS_PASSWORD')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_LOG_BUCKET_NAME = os.getenv('R2_LOG_BUCKET_NAME')
R2_DATA_BUCKET_NAME = os.getenv('R2_DATA_BUCKET_NAME')
R2_CLIENT_FOLDER = os.getenv('R2_CLIENT_FOLDER')

# File Paths
DATA_FILE = os.path.join(APPLICATION_BASE_DIR, 'public', 'data.json')
LAST_RUN_FILE = os.path.join(APPLICATION_BASE_DIR, '.last_run')

# --- Logging ---
# Configure console to show INFO level and a file to store all DEBUG level logs
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

file_handler = logging.FileHandler(LOGS_DIR / 'lims_fetcher_debug.log', mode='w', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logging.basicConfig(
    level=logging.DEBUG,  # Set the root logger to DEBUG to capture everything
    handlers=[console_handler, file_handler]
)
logger = logging.getLogger('fetch_lims_data')

# --- Login ---
def lims_login(session: requests.Session) -> bool:
    logger.info("Attempting LIMS login...")
    if not LIMS_USER or not LIMS_PASSWORD:
        logger.error("LIMS credentials missing in .env")
        return False
    try:
        login_page_url = f"{LIMS_URL}/index.php?m="
        r1 = session.get(login_page_url)
        logger.debug(f"GET {login_page_url} Status: {r1.status_code}")
        
        pattern = r'<input\s+name=["\']rdm["\']\s+type=["\']hidden["\']\s+value=["\']([^"\']+)["\']\s*/?>'
        match = re.search(pattern, r1.text, re.IGNORECASE)
        if not match:
            logger.error("rdm token not found on login page")
            return False
        rdm_token = match.group(1)
        logger.debug(f"Found rdm token: {rdm_token}")

        login_post_url = f"{LIMS_URL}/auth.php"
        payload = {
            "username": LIMS_USER,
            "password": LIMS_PASSWORD,
            "action": "auth",
            "rdm": rdm_token,
        }
        headers = {
            "Referer": login_page_url,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        r2 = session.post(login_post_url, data=payload, headers=headers, allow_redirects=True)
        
        final_url = r2.url
        if final_url.endswith("home.php"):
            logger.info("LIMS login successful.")
            return True
        else:
            logger.error("Login failed: Did not reach home.php after login")
            return False

    except Exception:
        logger.exception("Login sequence failed.")
        return False

# --- Get Start Date ---
def get_start_date() -> datetime.date:
    logger.info("Determining start date for data fetch...")
    # 1. Check for last_run.txt timestamp
    if os.path.exists(LAST_RUN_FILE):
        try:
            with open(LAST_RUN_FILE, 'r') as f:
                last_run_timestamp = f.read().strip()
            last_run_date = datetime.strptime(last_run_timestamp, '%Y-%m-%d %H:%M:%S.%f').date()
            logger.info(f"Found last run timestamp: {last_run_timestamp}. Starting fetch from {last_run_date}.")
            return last_run_date
        except Exception as e:
            logger.warning(f"Failed reading {LAST_RUN_FILE}: {e}. Falling back to data.json.")

    # 2. Fallback to data.json
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                records = json.load(f)
            if records:
                latest_date = max(datetime.fromisoformat(r['EncounterDate']).date() for r in records)
                logger.info(f"Latest date in existing records: {latest_date}. Fetching new data from {latest_date}.")
                return latest_date
        except Exception as e:
            logger.warning(f"Failed reading {DATA_FILE}: {e}. Falling back to default start date.")
    
    # 3. Default start date
    default_start = datetime(2025, 4, 1).date()
    logger.info(f"No valid records found, using default start date: {default_start}")
    return default_start

# --- Fetch Patient Details ---
def fetch_patient_details(session, patient):
    """
    Fetch and parse test details for a given patient from hoverrequest_b.php.
    Returns a list of dicts with TestCode, TestName.
    Logs warnings for patients with no test table or unexpected HTML.
    """
    url = f"{LIMS_URL}/hoverrequest_b.php?iid={patient['InvoiceNo']}&encounterno={patient['LabNo']}"
    details = []
    try:
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            logger.warning(f"Failed to fetch details for patient {patient['LabNo']} ({patient['InvoiceNo']}): HTTP {r.status_code}")
            return details

        soup = BeautifulSoup(r.text, 'html.parser')
        table = soup.find('table', class_='table-bordered')
        if not table:
            logger.warning(f"No details table found for patient {patient['LabNo']} ({patient['InvoiceNo']}) on {patient['EncounterDate']}")
            return details

        rows = table.find_all('tr')
        if len(rows) <= 1:
            logger.warning(f"Details table empty for patient {patient['LabNo']} ({patient['InvoiceNo']}) on {patient['EncounterDate']}")
            return details

        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) < 3:
                logger.warning(f"Skipping malformed test row for patient {patient['LabNo']} ({patient['InvoiceNo']}) - {len(cells)} cells found")
                continue

            test_date_raw = cells[0].text.strip()
            # Try multiple date formats
            test_date = test_date_raw
            for fmt in ('%d-%m-%Y', '%Y-%m-%d'):
                try:
                    test_date = datetime.strptime(test_date_raw, fmt).strftime('%d-%m-%Y')
                    break
                except ValueError:
                    continue

            detail = {
                'TestName': cells[2].text.strip(),
            }
            details.append(detail)

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error fetching details for {patient['LabNo']} ({patient['InvoiceNo']}): {e}")
    except Exception as e:
        logger.exception(f"Unexpected error fetching details for patient {patient['LabNo']} ({patient['InvoiceNo']})")

    if not details:
        logger.info(f"No tests found for patient {patient['LabNo']} ({patient['InvoiceNo']}) on {patient['EncounterDate']}")

    return details

# --- Fetch Data ---
def fetch_lims_data(session, start_date):
    """
    Fetch LIMS patient data by date range, then fetch test details for each unique patient.
    Returns a flat list of test records.
    """
    end_date = datetime.now().date()
    all_patients_info = {} # Stores unique patients as {patient_key: patient_data}
    
    logger.info(f"Fetching LIMS data from {start_date.isoformat()} to {end_date.isoformat()}...")

    search_params = {
        'searchtype': 'daterange',
        'daterange': f"{start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}",
        'Get': 'Get'
    }

    try:
        r = session.get(SEARCH_URL, params=search_params, timeout=300)
        r.raise_for_status()
        
        soup = BeautifulSoup(r.text, 'html.parser')
        table = soup.find('table', id='list')

        if not table:
            logger.warning("No patient table found on search results page.")
            return []

        rows = table.find_all('tr')[1:]  # Skip header row
        logger.info(f"Found {len(rows)} patients in total.")

        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 8:
                logger.warning(f"Skipping malformed patient row with {len(cells)} cells.")
                continue
            
            try:
                encounter_date = datetime.strptime(cells[0].text.strip(), '%d-%m-%Y').date().isoformat()
            except ValueError:
                logger.warning(f"Skipping patient with bad date format: {cells[0].text.strip()}")
                continue
            
            patient = {
                "EncounterDate": encounter_date,
                "LabNo": cells[1].text.strip(),
                "InvoiceNo": cells[3].text.strip(),
                "PNo": cells[4].text.strip(),
                "Patient": cells[5].text.strip(),
                "Tel": cells[6].text.strip(),
                "Src": cells[7].text.strip()
            }
            
            patient_key = patient['LabNo']
            if patient_key not in all_patients_info:
                all_patients_info[patient_key] = patient

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during initial patient search: {e}")
        return []
    except Exception as e:
        logger.exception("Unexpected error during patient search.")
        return []

    logger.info(f"Processing details for {len(all_patients_info)} unique patients.")
    final_records = []
    
    for idx, patient_data in enumerate(all_patients_info.values(), 1):
        if idx % 100 == 0:
            logger.info(f"Processing details for patient {idx} of {len(all_patients_info)}...")
            
        test_details = fetch_patient_details(session, patient_data)
        
        for test in test_details:
            record = patient_data.copy()
            record.update(test)
            final_records.append(record)

    logger.info(f"Fetched a total of {len(final_records)} test records.")
    return final_records

# --- Save + Upload ---
def save_and_upload(new_records):
    """Saves new records by appending to data.json and uploads debug logs to R2."""
    if not new_records:
        logger.info("No new records to save or upload.")
        upload_to_r2(os.path.join(LOGS_DIR, 'lims_fetcher_debug.log'), R2_LOG_BUCKET_NAME)
        return

    existing_data = []
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                existing_data = json.load(f)
            logger.info(f"Loaded {len(existing_data)} existing records from {DATA_FILE}.")
        except (json.JSONDecodeError, FileNotFoundError):
            logger.warning(f"Existing {DATA_FILE} is empty or corrupted. Starting fresh.")

    # Convert records to a hashable format (tuple of sorted items) for a set
    existing_set = {tuple(sorted(rec.items())) for rec in existing_data}
    new_set = {tuple(sorted(rec.items())) for rec in new_records}

    # Find truly new records
    truly_new_records_set = new_set - existing_set
    truly_new_records = [dict(t) for t in truly_new_records_set]

    if truly_new_records:
        final_data = existing_data + truly_new_records
        with open(DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=4)
        logger.info(f"Saved {len(truly_new_records)} truly new records. Total records now: {len(final_data)}.")
    else:
        logger.info("No new unique records found to append.")
    
    # Upload the debug log to R2
    upload_to_r2(os.path.join(LOGS_DIR, 'lims_fetcher_debug.log'), R2_LOG_BUCKET_NAME)

def get_last_run_timestamp():
    """Retrieves the timestamp of the last successful run."""
    try:
        with open(LAST_RUN_FILE, 'r') as f:
            last_run = datetime.fromisoformat(f.read().strip())
        logger.info(f"Last successful run timestamp found: {last_run}")
        return last_run
    except FileNotFoundError:
        logger.warning(".last_run file not found. Starting fresh.")
        return None
    except (ValueError, IndexError):
        logger.error("Invalid format in .last_run file. Starting fresh.")
        return None

def save_last_run_timestamp(timestamp):
    """Saves the current timestamp to mark a successful run."""
    try:
        with open(LAST_RUN_FILE, 'w') as f:
            f.write(timestamp.isoformat())
        logger.info(f"Updated .last_run file with timestamp: {timestamp}")
    except Exception as e:
        logger.error(f"Failed to save last run timestamp: {e}")

def upload_to_r2(file_path, bucket):
    """Uploads a file to a specific R2 bucket and client folder."""
    logger.info(f"Attempting to upload {os.path.basename(file_path)} to R2 bucket {bucket}...")
    if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, bucket, R2_CLIENT_FOLDER]):
        logger.error("R2 credentials or client folder incomplete. Skipping upload.")
        return
    try:
        s3 = boto3.client('s3',
                          endpoint_url=R2_ENDPOINT_URL,
                          aws_access_key_id=R2_ACCESS_KEY_ID,
                          aws_secret_access_key=R2_SECRET_ACCESS_KEY)

        object_key = f"{R2_CLIENT_FOLDER}/{os.path.basename(file_path)}"
        
        s3.upload_file(file_path, bucket, object_key)
        logger.info(f"Uploaded {os.path.basename(file_path)} to R2 at key: {object_key}.")
    except Exception as e:
        logger.exception(f"Failed to upload {os.path.basename(file_path)} to R2: {e}")

# --- Main ---
def run():
    """Main execution function for the LIMS data fetcher."""
    logger.info("Starting LIMS data fetch...")
    
    s = requests.Session()
    
    if not lims_login(s):
        logger.error("Failed to login to LIMS. Exiting.")
        return
    
    current_run_timestamp = datetime.now()
    
    start_date_for_fetch = get_start_date()
    
    try:
        new_records = fetch_lims_data(s, start_date_for_fetch)
        
        if new_records:
            save_and_upload(new_records)
        else:
            logger.info("No new records found.")
    finally:
        save_last_run_timestamp(current_run_timestamp)
        logger.info("LIMS fetch complete.")

if __name__ == '__main__':
    run()