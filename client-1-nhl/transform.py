import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
import csv
import logging
from collections import defaultdict
import ijson
from typing import List, Dict, Any, Set

# --- Environment and Path Configuration ---

def get_application_base_dir():
    """Determines the application base directory for both PyInstaller and standalone runs."""
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return base_dir

APPLICATION_BASE_DIR = get_application_base_dir()
load_dotenv(os.path.join(APPLICATION_BASE_DIR, '.env'))

# --- Default Values ---
DEFAULT_DATE_STR = '1970-01-01'
DEFAULT_DATETIME_DT = datetime(1970, 1, 1, 0, 0)
DEFAULT_DATETIME_STR = '1970-01-01 00:00:00'
DEFAULT_STRING = 'N/A'
DEFAULT_NUMERIC = 0.0
DEFAULT_DELAY_STATUS = 'Not Uploaded'
DEFAULT_TIME_RANGE = 'Not Uploaded'
DEFAULT_URGENCY = 'Not Urgent'
CLIENT_IDENTIFIER = os.getenv('CLIENT_IDENTIFIER', 'DefaultClient')
if CLIENT_IDENTIFIER == 'DefaultClient':
    CLIENT_IDENTIFIER = 'Nakasero'

# --- File Locations ---
LOCAL_PUBLIC_DIR = os.path.join(APPLICATION_BASE_DIR, 'public')
LOGS_DIR = os.path.join(APPLICATION_BASE_DIR, 'debug')
DATA_JSON_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'data.json')
META_CSV_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'meta.csv')
TIMEOUT_CSV_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'TimeOut.csv')
TESTS_DATASET_JSON_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'tests_dataset.json')
PATIENTS_DATASET_JSON_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'patients_dataset.json')
PROCESSED_INVOICES_FILE = os.path.join(LOCAL_PUBLIC_DIR, 'processed_invoice_numbers.json')
INVALID_LABNOS_OUTPUT_PATH = os.path.join(LOGS_DIR, 'data_json_invalid_labnos.txt')
UNMATCHED_TEST_NAMES_OUTPUT_PATH = os.path.join(LOGS_DIR, 'data_json_unmatched_test_names.txt')

# Ensure directories exist
os.makedirs(LOCAL_PUBLIC_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOGS_DIR, 'tests_dataset_debug.log'), mode='w')
    ]
)
logger = logging.getLogger('transform.py')

# --- Helper Functions ---
def parse_labno_timestamp(labno):
    """
    Parses DDMMYYHHMM from a LabNo string.
    Returns a datetime object or DEFAULT_DATETIME_DT on failure.
    """
    if not isinstance(labno, str) or len(labno) < 10:
        return DEFAULT_DATETIME_DT
    
    timestamp_str = labno[:10]
    
    if not timestamp_str.isdigit():
        return DEFAULT_DATETIME_DT
        
    try:
        date_str = timestamp_str[4:6] + '-' + timestamp_str[2:4] + '-' + timestamp_str[0:2]
        time_str = timestamp_str[6:8] + ':' + timestamp_str[8:10]
        dt_obj = datetime.strptime(f'{date_str} {time_str}', '%y-%m-%d %H:%M')
        return dt_obj
    except ValueError:
        return DEFAULT_DATETIME_DT

def get_shift(time_in_dt):
    """Determines shift based on Time_In datetime object."""
    if not isinstance(time_in_dt, datetime) or time_in_dt.date() == DEFAULT_DATETIME_DT.date():
        return DEFAULT_STRING

    hour = time_in_dt.hour
    if 8 <= hour <= 19:
        return 'Day Shift'
    else:
        return 'Night Shift'

def calculate_daily_tat(tats_list):
    """
    Calculates Daily_TAT based on a tiered logic from a list of individual TATs.
    Returns 0.0 if no TATs are available.
    """
    if not tats_list:
        return 0.0
    
    max_tat = max(tats_list)
    
    short_tats = [t for t in tats_list if t < 720]
    if short_tats:
        return max(short_tats)
    
    medium_tats = [t for t in tats_list if t < 1440]
    if medium_tats:
        return max(medium_tats)
        
    three_day_tats = [t for t in tats_list if t < 4320]
    if three_day_tats:
        return max(three_day_tats)
        
    five_day_tats = [t for t in tats_list if t < 7200]
    if five_day_tats:
        return max(five_day_tats)

    ten_day_tats = [t for t in tats_list if t < 14400]
    if ten_day_tats:
        return max(ten_day_tats)
    
    return max_tat

def calculate_delay_status_and_range(time_in, time_out, expected_time):
    """
    Calculates delay status and range based on three datetime objects.
    This function is used by ingest.py and must be defined here.
    """
    if time_out == DEFAULT_DATETIME_DT:
        return 'Not Uploaded', 'Not Uploaded'
    
    if not isinstance(time_in, datetime) or not isinstance(time_out, datetime) or not isinstance(expected_time, datetime):
        return 'Not Uploaded', 'Not Uploaded'

    delay_delta = time_out - expected_time
    delay_minutes = delay_delta.total_seconds() / 60
    
    delay_hours = int(abs(delay_minutes) // 60)
    delay_min_remainder = int(abs(delay_minutes) % 60)
    time_range_str = f"{delay_hours} hrs {delay_min_remainder} mins"

    if delay_minutes >= 15:
        status = 'Over Delayed'
    elif 0 < delay_minutes < 15:
        status = 'Delayed for less than 15 minutes'
    elif -30 <= delay_minutes <= 0:
        status = 'On Time'
    else: # delay_minutes < -30
        status = 'Swift'
        
    return status, time_range_str

# --- Data Loading Functions ---
def load_meta_data():
    """Loads meta.csv into a dictionary with error handling."""
    meta_data = {}
    try:
        with open(META_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                test_name = row.get('TestName', DEFAULT_STRING).upper().strip()
                try:
                    tat = float(row.get('TAT', DEFAULT_NUMERIC))
                    price = float(row.get('Price', DEFAULT_NUMERIC))
                except (ValueError, TypeError):
                    logger.warning(f"Failed to parse numeric values for TestName '{test_name}'. Using defaults.")
                    tat = DEFAULT_NUMERIC
                    price = DEFAULT_NUMERIC
                
                meta_data[test_name] = {
                    'TAT': tat,
                    'LabSection': row.get('LabSection', DEFAULT_STRING),
                    'Price': price
                }
        logger.info(f"Successfully loaded {len(meta_data)} test metadata entries.")
    except Exception as e:
        logger.error(f"Failed to load meta.csv: {e}")
    return meta_data

def load_timeout_data():
    """Loads TimeOut.csv into a dictionary, keeping the latest CreationTime for duplicate FileNames."""
    timeout_data = {}
    date_formats = ['%m/%d/%Y %I:%M %p', '%#m/%#d/%Y %I:%M %p']
    try:
        with open(TIMEOUT_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                invoice_no = row.get('FileName')
                creation_time_str = row.get('CreationTime')
                if not invoice_no or not creation_time_str:
                    continue
                
                creation_time_dt = None
                for fmt in date_formats:
                    try:
                        creation_time_dt = datetime.strptime(creation_time_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if creation_time_dt:
                    if invoice_no not in timeout_data or creation_time_dt > timeout_data[invoice_no]['CreationTime']:
                        timeout_data[invoice_no] = {'CreationTime': creation_time_dt}
                    
                else:
                    logger.warning(f"Failed to parse CreationTime '{creation_time_str}' for invoice '{invoice_no}'. No matching date format found.")

    except Exception as e:
        logger.error(f"Failed to load TimeOut.csv: {e}")
    return timeout_data

def load_processed_invoices():
    """Loads the set of already processed invoices from file."""
    if not os.path.exists(PROCESSED_INVOICES_FILE):
        return set()
    try:
        with open(PROCESSED_INVOICES_FILE, 'r') as f:
            return set(json.load(f))
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load processed invoices: {e}. Starting with an empty set.")
        return set()

def save_processed_invoices(processed_invoices):
    """Saves the set of processed invoices to file."""
    try:
        with open(PROCESSED_INVOICES_FILE, 'w') as f:
            json.dump(list(processed_invoices), f)
        logger.info(f"Saved {len(processed_invoices)} processed invoices.")
    except IOError as e:
        logger.error(f"Failed to save processed invoices: {e}")

# --- Core Transformation Logic ---
def run_data_generation():
    """Orchestrates the entire data generation pipeline."""
    logger.info("Starting data generation pipeline...")

    meta_data = load_meta_data()
    timeout_data = load_timeout_data()
    processed_invoices = load_processed_invoices()
    
    newly_processed_invoices = set()
    unmatched_test_names = set()
    invalid_labnos = defaultdict(int)
    
    tests_dataset = []
    patients_data_map = defaultdict(lambda: {
        'Tats': [],
        'InvoiceNos': set(),
        'Details': {}
    })
    
    try:
        with open(DATA_JSON_PATH, 'rb') as f, open(INVALID_LABNOS_OUTPUT_PATH, 'w') as invalid_labnos_log:
            logger.info("Starting to process raw data from data.json...")
            parser = ijson.items(f, 'item')
            for record in parser:
                invoice_no = record.get('InvoiceNo')
                lab_no = record.get('LabNo')
                test_name_raw = record.get('TestName')

                if invoice_no in processed_invoices:
                    continue
                    
                time_in_dt = parse_labno_timestamp(lab_no)
                if time_in_dt == DEFAULT_DATETIME_DT:
                    invalid_labnos[lab_no] += 1
                    continue
                    
                normalized_test_name = test_name_raw.upper().strip() if test_name_raw else None
                if normalized_test_name not in meta_data:
                    if normalized_test_name:
                        unmatched_test_names.add(normalized_test_name)
                    continue

                meta_info = meta_data[normalized_test_name]

                # --- Individual Test-Level Data (`tests_dataset.json`) ---
                individual_test_record = {
                    'ID': str(uuid.uuid4()),
                    'Lab_Number': lab_no,
                    'Test_Name': test_name_raw,
                    'Lab_Section': meta_info['LabSection'],
                    'TAT': meta_info['TAT'],
                    'Price': meta_info['Price'],
                    'Time_Received': DEFAULT_DATETIME_STR,
                    'Test_Time_Expected': (time_in_dt + timedelta(minutes=meta_info['TAT'])).strftime('%Y-%m-%d %H:%M:%S'),
                    'Urgency': DEFAULT_URGENCY,
                    'Test_Time_Out': DEFAULT_DATETIME_STR
                }
                tests_dataset.append(individual_test_record)

                # --- Aggregate Patient-Level Data (`patients_dataset.json`) ---
                patients_data_map[lab_no]['Tats'].append(meta_info['TAT'])
                patients_data_map[lab_no]['InvoiceNos'].add(invoice_no)
                
                if not patients_data_map[lab_no]['Details'] or not patients_data_map[lab_no]['Details'].get('LabNo'):
                    patients_data_map[lab_no]['Details'] = {
                        'LabNo': lab_no,
                        'Client': CLIENT_IDENTIFIER,
                        'Date': record.get('EncounterDate', DEFAULT_DATE_STR),
                        'Time_In': time_in_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'Unit': record.get('Src', DEFAULT_STRING)
                    }
                
                newly_processed_invoices.add(invoice_no)

            for labno, count in invalid_labnos.items():
                invalid_labnos_log.write(f"LabNo: {labno}, Occurrences: {count}\n")
            logger.info(f"Logged invalid LabNos to {INVALID_LABNOS_OUTPUT_PATH}")

    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        return

    # --- Finalize patient-level data after processing all records ---
    patients_dataset = []
    for lab_no, data in patients_data_map.items():
        time_in_dt = parse_labno_timestamp(lab_no)
        daily_tat = calculate_daily_tat(data['Tats'])
        
        request_time_out_dt = DEFAULT_DATETIME_DT
        latest_timeout = DEFAULT_DATETIME_DT
        
        for invoice_no in data['InvoiceNos']:
            if invoice_no in timeout_data:
                current_timeout = timeout_data[invoice_no]['CreationTime']
                if current_timeout > latest_timeout:
                    latest_timeout = current_timeout

        request_time_out_dt = latest_timeout
        
        request_time_expected_dt = time_in_dt + timedelta(minutes=daily_tat)
        
        delay_status, time_range = calculate_delay_status_and_range(time_in_dt, request_time_out_dt, request_time_expected_dt)

        patient_record = {
            'Lab_Number': lab_no,
            'Client': CLIENT_IDENTIFIER,
            'Date': data['Details'].get('Date', DEFAULT_DATE_STR),
            'Shift': get_shift(time_in_dt),
            'Unit': data['Details'].get('Unit', DEFAULT_STRING),
            'Time_In': time_in_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'Daily_TAT': daily_tat,
            'Request_Time_Expected': request_time_expected_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'Request_Time_Out': request_time_out_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'Request_Delay_Status': delay_status,
            'Request_Time_Range': time_range,
        }
        patients_dataset.append(patient_record)

    # --- Save Outputs ---
    try:
        with open(TESTS_DATASET_JSON_PATH, 'w') as f:
            json.dump(tests_dataset, f, indent=4)
        logger.info(f"Successfully generated {len(tests_dataset)} test records and saved to {TESTS_DATASET_JSON_PATH}")

        with open(PATIENTS_DATASET_JSON_PATH, 'w') as f:
            json.dump(patients_dataset, f, indent=4)
        logger.info(f"Successfully generated {len(patients_dataset)} patient records and saved to {PATIENTS_DATASET_JSON_PATH}")
        
        with open(UNMATCHED_TEST_NAMES_OUTPUT_PATH, 'w') as f:
            for name in unmatched_test_names:
                f.write(f"{name}\n")
        logger.info(f"Logged {len(unmatched_test_names)} unmatched test names to {UNMATCHED_TEST_NAMES_OUTPUT_PATH}")

        processed_invoices.update(newly_processed_invoices)
        save_processed_invoices(processed_invoices)
        
    except Exception as e:
        logger.error(f"Failed to save output files: {e}")

    logger.info("Data generation pipeline finished.")

if __name__ == '__main__':
    run_data_generation()