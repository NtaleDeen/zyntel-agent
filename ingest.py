import os
import sys
import json
import logging
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import boto3
import csv
from typing import List, Dict, Any
import ijson
import re

# Assuming `transform.py` exists in the same directory or is importable
# It defines calculate_delay_status_and_range
from transform import DEFAULT_DATETIME_DT, DEFAULT_DATETIME_STR, calculate_delay_status_and_range

# --- Environment and Path Configuration ---
def get_application_base_dir():
    """Determines the application base directory."""
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return base_dir

APPLICATION_BASE_DIR = get_application_base_dir()
load_dotenv(os.path.join(APPLICATION_BASE_DIR, '.env'))

# --- File Locations ---
LOCAL_PUBLIC_DIR = os.path.join(APPLICATION_BASE_DIR, 'public')
LOGS_DIR = os.path.join(APPLICATION_BASE_DIR, 'debug')
TESTS_DATASET_JSON_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'tests_dataset.json')
PATIENTS_DATASET_JSON_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'patients_dataset.json')
TIMEOUT_CSV_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'TimeOut.csv')
DATA_JSON_PATH = os.path.join(LOCAL_PUBLIC_DIR, 'data.json')

# Ensure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOGS_DIR, 'ingest_debug.log'))
    ]
)
logger = logging.getLogger('ingest.py')


# --- Database Schema Definitions ---
PATIENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS patients (
    lab_number TEXT PRIMARY KEY,
    client TEXT,
    date DATE,
    shift TEXT,
    unit TEXT,
    time_in TIMESTAMP,
    daily_tat NUMERIC,
    request_time_expected TIMESTAMP,
    request_time_out TIMESTAMP,
    request_delay_status TEXT,
    request_time_range TEXT
);
"""

TESTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS tests (
    id TEXT PRIMARY KEY,
    lab_number TEXT,
    test_name TEXT,
    lab_section TEXT,
    tat NUMERIC,
    price NUMERIC,
    time_received TIMESTAMP,
    test_time_expected TIMESTAMP,
    urgency TEXT,
    test_time_out TIMESTAMP
);
"""

# --- Helper Functions ---
def parse_datetime_field(dt_str: str | None) -> datetime | None:
    """
    Parses a datetime string and returns a naive datetime object (no timezone).
    Returns None if string is a known invalid/default timestamp (e.g., Unix epoch).
    """
    if not dt_str:
        return None

    if dt_str in ['N/A', DEFAULT_DATETIME_STR]:
        return None

    try:
        naive_dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        if naive_dt == datetime(1970, 1, 1, 0, 0):
            return None
        return naive_dt

    except ValueError:
        try:
            naive_dt = datetime.strptime(dt_str, '%m/%d/%Y %H:%M')
            if naive_dt == datetime(1970, 1, 1, 0, 0):
                return None
            return naive_dt
        except Exception as inner_e:
            logger.warning(f"Unable to parse datetime string '{dt_str}': {inner_e}")
            return None

def load_timeout_data():
    """Loads TimeOut.csv into a dictionary for efficient lookup."""
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
        logger.error(f"Failed to load TimeOut.csv for post-ingestion updates: {e}")
    return timeout_data

def upload_logs_to_r2():
    """Uploads specified log files to Cloudflare R2 or S3-compatible storage."""
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

    log_files_to_upload = [
        os.path.join(LOGS_DIR, 'ingest_debug.log'),
        os.path.join(LOGS_DIR, 'data_json_invalid_labnos.txt'),
        os.path.join(LOGS_DIR, 'data_json_unmatched_test_names.txt'),
        os.path.join(LOGS_DIR, 'tests_dataset_debug.log')
    ]

    for file_path in log_files_to_upload:
        if os.path.exists(file_path):
            try:
                # Construct the object key with the client folder prefix
                object_key = f"{r2_client_folder}/{os.path.basename(file_path)}"
                s3_client.upload_file(file_path, r2_log_bucket_name, object_key)
                logger.info(f"Successfully uploaded {os.path.basename(file_path)} to R2 bucket '{r2_log_bucket_name}' at key '{object_key}'.")
            except Exception as e:
                logger.error(f"Failed to upload {os.path.basename(file_path)} to R2: {e}")
        else:
            logger.warning(f"Log file not found, skipping upload: {file_path}")

# --- Core Ingestion Logic ---
def ingest_data(cursor, table_name, data_list, primary_key, columns, BATCH_SIZE=1000):
    """
    Ingests data into a table in batches, handling idempotency.
    """
    if not data_list:
        logger.info(f"No new records to ingest for table '{table_name}'.")
        return
    
    placeholders = sql.SQL(', ').join(sql.Placeholder() * len(columns))
    insert_columns = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
    update_set_clause = sql.SQL(', ').join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
        for col in columns if col != primary_key
    )

    insert_query = sql.SQL("""
        INSERT INTO {} ({}) VALUES ({})
        ON CONFLICT ({}) DO UPDATE SET {}
    """).format(
        sql.Identifier(table_name),
        insert_columns,
        placeholders,
        sql.Identifier(primary_key),
        update_set_clause
    )
    
    try:
        execute_batch(cursor, insert_query, data_list, page_size=BATCH_SIZE)
        logger.info(f"Successfully ingested {len(data_list)} records into '{table_name}'.")
    except Exception as e:
        logger.error(f"Failed to ingest data into '{table_name}': {e}")
        raise

def ensure_tables_exist(cursor):
    """Creates tables if they don't already exist."""
    logger.info("Ensuring database tables exist...")
    cursor.execute(PATIENTS_SCHEMA)
    cursor.execute(TESTS_SCHEMA)
    logger.info("Tables 'patients' and 'tests' are ready.")

def get_existing_ids(cursor, table_name, primary_key):
    """Queries the database to get a set of existing primary keys."""
    try:
        cursor.execute(sql.SQL("SELECT {} FROM {}").format(sql.Identifier(primary_key), sql.Identifier(table_name)))
        return {row[0] for row in cursor.fetchall()}
    except psycopg2.Error as e:
        logger.error(f"Error querying existing IDs from '{table_name}': {e}. Proceeding with empty set.")
        return set()

def update_incomplete_records(conn, cursor, timeout_data):
    """
    Queries for records with default/null Request_Time_Out and updates them
    using the data from TimeOut.csv.
    """
    logger.info("Starting post-ingestion update for incomplete records...")
    
    # Query for records with the default placeholder or NULL
    query_incomplete = sql.SQL("""
        SELECT lab_number, time_in, request_time_expected
        FROM patients
        WHERE request_time_out = %s OR request_time_out IS NULL;
    """)
    
    cursor.execute(query_incomplete, (DEFAULT_DATETIME_DT,))
    incomplete_records = cursor.fetchall()
    
    if not incomplete_records:
        logger.info("No incomplete 'patients' records found to update.")
        return
    
    # Re-parse data.json to create a mapping from lab_number to all associated invoice numbers
    labno_to_invoices = {}
    try:
        with open(DATA_JSON_PATH, 'rb') as f:
            parser = ijson.items(f, 'item')
            for record in parser:
                labno = record.get('LabNo')
                invoiceno = record.get('InvoiceNo')
                if labno and invoiceno:
                    if labno not in labno_to_invoices:
                        labno_to_invoices[labno] = set()
                    labno_to_invoices[labno].add(invoiceno)
    except FileNotFoundError:
        logger.error("data.json not found, cannot update incomplete records.")
        return
        
    records_to_update = []
    for lab_number, time_in_str, request_time_expected_str in incomplete_records:
        latest_timeout_dt = None
        
        invoices = labno_to_invoices.get(lab_number, [])
        for invoice_no in invoices:
            timeout_info = timeout_data.get(invoice_no)
            if timeout_info and (not latest_timeout_dt or timeout_info['CreationTime'] > latest_timeout_dt):
                latest_timeout_dt = timeout_info['CreationTime']
        
        if latest_timeout_dt:
            time_in_dt = parse_datetime_field(str(time_in_str))
            request_time_expected_dt = parse_datetime_field(str(request_time_expected_str))

            if time_in_dt and request_time_expected_dt:
                delay_status, time_range = calculate_delay_status_and_range(time_in_dt, latest_timeout_dt, request_time_expected_dt)
                records_to_update.append({
                    'lab_number': lab_number,
                    'request_time_out': latest_timeout_dt,
                    'request_delay_status': delay_status,
                    'request_time_range': time_range
                })
            else:
                logger.warning(f"Could not parse datetime for record {lab_number}. Skipping update.")
                
    if not records_to_update:
        logger.info("No incomplete records found with available timeout data to update.")
        return

    update_query = """
        UPDATE patients
        SET 
            request_time_out = %(request_time_out)s,
            request_delay_status = %(request_delay_status)s,
            request_time_range = %(request_time_range)s
        WHERE lab_number = %(lab_number)s;
    """

    try:
        psycopg2.extras.execute_batch(
            cursor,
            update_query,
            records_to_update
        )
        conn.commit()
        logger.info(f"Successfully updated {len(records_to_update)} incomplete records in 'patients' table.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update incomplete records: {e}")


def run_data_ingestion():
    """Orchestrates the entire data ingestion pipeline."""
    conn = None
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable is not set.")
        
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False # Use transactions for atomicity
        cursor = conn.cursor()
        
        ensure_tables_exist(cursor)
        
        # Load and filter test-level data
        logger.info("Loading `tests_dataset.json`...")
        with open(TESTS_DATASET_JSON_PATH, 'r') as f:
            tests_data = json.load(f)
        
        existing_test_ids = get_existing_ids(cursor, 'tests', 'id')
        new_tests_data = [
            (
                rec['ID'], rec['Lab_Number'], rec['Test_Name'], rec['Lab_Section'], rec['TAT'], rec['Price'],
                parse_datetime_field(rec['Time_Received']), parse_datetime_field(rec['Test_Time_Expected']),
                rec['Urgency'], parse_datetime_field(rec['Test_Time_Out'])
            )
            for rec in tests_data if rec['ID'] not in existing_test_ids
        ]
        
        if new_tests_data:
            test_columns = ['id', 'lab_number', 'test_name', 'lab_section', 'tat', 'price',
                            'time_received', 'test_time_expected', 'urgency', 'test_time_out']
            ingest_data(cursor, 'tests', new_tests_data, 'id', test_columns)
        
        # Load and filter patient-level data
        logger.info("Loading `patients_dataset.json`...")
        with open(PATIENTS_DATASET_JSON_PATH, 'r') as f:
            patients_data = json.load(f)
            
        existing_patient_labnos = get_existing_ids(cursor, 'patients', 'lab_number')
        new_patients_data = [
            (
                rec['Lab_Number'], rec['Client'], rec['Date'], rec['Shift'], rec['Unit'],
                parse_datetime_field(rec['Time_In']), rec['Daily_TAT'],
                parse_datetime_field(rec['Request_Time_Expected']),
                parse_datetime_field(rec['Request_Time_Out']),
                rec['Request_Delay_Status'],
                rec['Request_Time_Range']
            )
            for rec in patients_data if rec['Lab_Number'] not in existing_patient_labnos
        ]

        if new_patients_data:
            patient_columns = ['lab_number', 'client', 'date', 'shift', 'unit',
                               'time_in', 'daily_tat', 'request_time_expected',
                               'request_time_out', 'request_delay_status',
                               'request_time_range']
            ingest_data(cursor, 'patients', new_patients_data, 'lab_number', patient_columns)
            
        # Post-ingestion logic
        timeout_data = load_timeout_data()
        update_incomplete_records(conn, cursor, timeout_data)
            
        conn.commit()
        
        cursor.execute("ANALYZE;")
        logger.info("Database statistics analyzed.")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred during ingestion: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed.")
        upload_logs_to_r2()
    
    logger.info("Data ingestion pipeline finished.")

if __name__ == '__main__':
    run_data_ingestion()