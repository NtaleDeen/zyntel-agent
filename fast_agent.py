import os
import sys
import logging
from dotenv import load_dotenv

import fetch_lims_data
import transform
import ingest

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
        logging.FileHandler(os.path.join(LOGS_DIR, 'fast_agent_debug.log'), mode='w')
    ]
)
logger = logging.getLogger('fast_agent.py')

def main():
    logger.info("Starting Zyntel Fast Agent orchestration...")
    try:
        # Step 1: Run the LIMS data fetcher.
        logger.info("Step 1: Running LIMS data fetcher (fetch_lims_data.py)...")
        fetch_lims_data.run()
        logger.info("LIMS data fetch completed successfully.")

        # Step 2: Run the data transformation.
        logger.info("Step 2: Running data transformation (transform.py)...")
        transform.run_data_generation()
        logger.info("Data transformation completed successfully.")

        # Step 3: Run the data ingestion.
        logger.info("Step 3: Running data ingestion (ingest.py)...")
        ingest.run_data_ingestion()
        logger.info("Data ingestion completed successfully.")

        logger.info("Zyntel Fast Agent orchestration finished successfully.")

    except Exception as e:
        logger.error(f"An error occurred during orchestration: {e}")

if __name__ == '__main__':
    main()