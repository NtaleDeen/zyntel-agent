import os
import sys
import logging
from dotenv import load_dotenv

import timeout
import ingest
import psycopg2

# --- Environment and Path Configuration ---
def get_application_base_dir():
    """Determines the application base directory."""
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return base_dir

APPLICATION_BASE_DIR = get_application_base_dir()
LOGS_DIR = os.path.join(APPLICATION_BASE_DIR, 'debug')
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOGS_DIR, 'slow_agent_debug.log'), mode='w')
    ]
)
logger = logging.getLogger('slow_agent.py')

def main():
    logger.info("Starting Zyntel Slow Agent orchestration...")
    conn = None
    try:
        # Step 1: Run the timeout script.
        logger.info("Step 1: Running timeout.py to update TimeOut.csv...")
        timeout.run_timeout_update()
        logger.info("Timeout file scanning and update completed successfully.")

        # Step 2: Connect to the database and run post-ingestion updates.
        logger.info("Step 2: Connecting to database to update incomplete records...")
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable is not set.")
        
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        timeout_data = ingest.load_timeout_data()
        ingest.update_incomplete_records(conn, cursor, timeout_data)
        
        conn.commit()
        
        logger.info("Zyntel Slow Agent orchestration finished successfully.")

    except Exception as e:
        logger.error(f"An error occurred during orchestration: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed.")
        ingest.upload_logs_to_r2()
        
if __name__ == '__main__':
    main()