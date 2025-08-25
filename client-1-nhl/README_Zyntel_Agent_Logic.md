**Zyntel Data Agent**

**Overview**
* This document outlines the logic and setup for the Zyntel Data Agent.
* It processes hospital data, merges it, and populates a PostgreSQL database.
* It also handles data storage and logging in Cloudflare R2 (S3-compatible storage).It’s designed for multiple clients and includes built-in defaults, validation, patient/test granularity, and an admin-ready meta management interface.

**Main Components:**
* `transform.py` → Raw data → cleaned and structured patients/tests datasets.
* `ingest.py` → Inserts the datasets into the PostgreSQL DB.
* `main_agent.py` → Orchestrates the full pipeline.
* `Admin Interface` → Manages metadata and user tables.

**Data Generation Logic**
**`transform.py`**
* This script orchestrates the creation of two distinct data outputs to serve different reporting needs:
1.  **Individual Test-Level Data [`tests_dataset.json` (1 row per test)]:**
    * Reads raw hospital data from `public/data.json`.
    * Checks `data.json` and filters out already processed records based on `public/processed_invoice_numbers.json`.
    * Filters for `Invalid Date/Time Components` by checking LabNos for timestamps (DDMMYYHHMM). Invalid LabNos are logged to `debug/data_json_invalid_labnos.txt`.
    * Reads metadata for tests from `public/meta.csv` (containing`TestName`, `TAT`, `LabSection`, `Price`) by matching `TestName`.
    * **DataSchema for `tests_dataset.json`:**
    * Generates a unique `ID` for each individual test record, ensuring each row is distinct.
    * Populates fields for each test as below:
        `ID`, `Lab_Number`, `Test_Name`, `Lab_Section`, `TAT` (individual test TAT from `public/meta.csv`), `Price`, `Time_Received`, `Test_Time_Expected`, `Urgency`, `Test_Time_Out`.
    * Skips individual test records if `TestName` is not found in `meta.csv`, logging these to `debug/data_json_unmatched_test_names.txt`.
    * This output (`tests_dataset.json`) is designed as the source for the detailed, test-level table in the database.
    * ***Ingest `Request_Time_Out` from TimeOut.csv, then calculates `Request_Delay_Status`, and `Request_Time_Range`:***
2.  **Patient-Level Aggregated Data [`patients_dataset.json` (1 row per patient visit)]:**
    * **Uses `Lab_Number` as the unique identifier for each record**, consolidating all tests for a single patient visit into one row.
    * Schema (For each unique `LabNo`, the aggregated record includes):
        `Lab_Number`, `Client`, `Date`, `Shift`, `Unit`, `Time_In`, `Daily_TAT`, `Request_Time_Expected`, `Request_Time_Out`, `Request_Delay_Status`, `Request_Time_Range`, `Test_Names`(A list of all unique test names performed under this `LabNo`), `Lab_Sections`(A list of all unique lab sections involved for this `LabNo`).
    * **Sample Input Data Files:**
    * ```Example `data.json` snippet:```
        [
        {
        `EncounterDate`: `2025-02-23`,
        `LabNo`: `230225015230`,
        `InvoiceNo`: `275122`,
        `PNo`: `NHL-PID/50004836`,
        `Patient`: `Mrs ********** ********`,
        `Tel`: `#########`,
        `Src`: `A&E`,
        `TestResultDate`: `23-02-2025`,
        `TestCode`: `_micro474`,
        `TestName`: `URINALYSIS COMPLETE`
        },
        {
        `EncounterDate`: `2025-02-23`,
        `LabNo`: `230225015303`,
        `InvoiceNo`: `275135`,
        `PNo`: `NHL-PID/50087694`,
        `Patient`: `Ms ********** ********`,
        `Tel`: `#########`,
        `Src`: `A&E`,
        `TestResultDate`: `23-02-2025`,
        `TestCode`: `_micro496`,
        `TestName`: `H.Pylori Antigen- Stool`
        }
        ]
        * `meta.csv`: Contains metadata for tests, including `TestName`, `TAT` (Standard TAT in minutes), `LabSection`, and `Price`. 

        ```Example `meta.csv` snippet:```
        TestName,TAT,LabSection,Price
        17-Hydroxy Progesterone,17280,REFERRAL,130000
        17-OH PROGESTERONE,17280,REFERRAL,125460
        24 hrs Urine Protein,1440,CHEMISTRY,543660
        24Hr Urine Cortisol,1440,CHEMISTRY,65000
        50g OGTT (Pregnancy),240,CHEMISTRY,89760
        75g OGTT,240,CHEMISTRY,89760
        ACE,17280,REFERRAL,200000
        ACTH,90,CHEMISTRY,120000
        ADA,17280,REFERRAL,93738
        
        * `TimeOut.csv`: Contains file metadata, specifically `FileName` (which corresponds to `InvoiceNo` in `data.json`) and `CreationTime`.

        ```Example `TimeOut.csv` snippet:```
        FileName,CreationTime,LastModified,FileHash
        323975,4/12/2025 9:47,4/10/2025 5:06,D3B18F2C2D2D8E97D7F3E525A7C0523F4DE749977E09B68EFACB678234F0DCBF
        324140,4/12/2025 9:47,4/10/2025 1:46,E5A00B4A3D5EBB6880C7D7DC39721BDC6A195E1726304A9845243B631D204BC7
        323878,4/12/2025 9:47,4/11/2025 11:38,9F4F40B1A59E55F686405C22FA6C005A10FCC0ADE5744C250A48028547382C8D
        324174,4/12/2025 9:47,4/10/2025 5:08,575AE2891BD984A49D4D3954739A3DD0ED69406AEB26E6C5CC6326C418AC197A
        323970,4/9/2025 20:01,4/9/2025 20:01,D6766FD74663A4A2E7155057434410F20F4E2B75C866DFA564F2B7FD2E67ACE7
        324173,4/12/2025 9:47,4/10/2025 5:11,228F0ECCF7316A19A01A215B89EF71F6F813402909C9B9C5B5DD2F95DAA27C40

**`ingest.py`**
* This script orchestrates the ingestion of data from both `tests_dataset.json` and `patients_dataset.json` into the PostgreSQL database.
* **Data Source for Tables:**
    * **From `patients_dataset.json` (LabNo-unique aggregated data):**
        * `patients` table (using `Lab_Number` as its primary key).
    * **From `tests_dataset.json` (ID-unique individual test data):**
        * `tests` table (using `ID` as its primary key).
* Connects to a PostgreSQL database using a provided `DATABASE_URL`.
* Ensures `tests` and `patients` tables exist (to prevent overriding previous records), creating them if they don't.
* Reads timeout information from `public/TimeOut.csv` (containing `Request_Time_Out` as `CreationTime` by matching `InvoiceNo` with `FileName`).
* Reads the merged data from local file and ingests it into the respective PostgreSQL tables.
* Handles ON CONFLICT (id) DO UPDATE SET logic for idempotent inserts, allowing re-runs to update existing records.
* ***Avoid Re-ingesting Already Processed Data:***
* The script has logic to check the database for existing records before starting the ingestion process.
* It queries the `tests` and `patients` tables to get sets of all existing `IDs` and `Lab_Numbers`, respectively.
* After loading the JSON files, the `tests-Level` and `patients-Level` lists are filtered using these sets.
* This ensures that the ingestion loops only process records that are not already in the database.
* The script includes a post-ingestion process specifically for updating incomplete records.
* It reads `TimeOut.csv` and creates a dictionary for efficient lookups.
* It specifically queries the `patients` table for records where `Request_Time_Out` is still the default placeholder value `1970-01-01 00:00:00` or `NULL` because in the database, default values are removed and converted back to null.
* For each of these incomplete records, it finds the corresponding `InvoiceNo` from `data.json` and uses the `TimeOut.csv` map to get the correct `Request_Time_Out`. 
* It simply links `LabNo` to `InvoiceNo` and then check the latest `TimeOut.csv` data to perform the final calculations.
* This eliminates that gap where, if transform.py already processed a given invoice, but the requesttimeout was not present in timeout.csv because the tests are still running, that invoice will be will be skipped next time, because it assumes they were already processed and can't reprocess them again to get the new timeout.
* It then calculates `Request_Delay_Status` and `Request_Time_Range` and performs an UPDATE operation on only those new timeouts.
* This ensures that complete records are never overwritten.
* ***How the Script is Now Resilient to Unstable Internet***
* The script's resilience is achieved through a combination of the changes mentioned above, making it safe to re-run at any time, even after a crash or with new data.
***Prevention of Duplicate Ingestion:***
* The core of the resilience comes from the initial filtering step.
* As shown in your `ingest.py` output, the script crashed after ingesting 47,000 records.
* When `ingest.py` is  re-ran, it will query the database and find all previously ingested records.
* The subsequent filtering logic will exclude these previously ingested records, allowing the ingestion to seamlessly continue from where it stopped without attempting to re-insert old data.
* This prevents ON CONFLICT errors and saves processing time.

* ***Updating Incomplete Records:***
* The post-ingestion update logic handles incomplete records gracefully.
* If your internet connection was unstable during the initial run and `TimeOut.csv` data was not fully available, those patients records would have been ingested with the default '1970' placeholder values.
* When the script is ran again, even if the main ingestion is complete, it will identify those specific placeholder records and update them with the new `TimeOut.csv` information.
* This means the script not only resumes from where it left off but also *fixes* any incomplete records from the previous, interrupted run.
* ***Idempotent Operation:***
* The entire process is designed to be idempotent.
* Running the script multiple times will not cause duplication or errors because it always checks for existing data before inserting new records and only updates records with a specific placeholder value.
* If no new `TimeOut.csv` data is available, the update logic simply finds no records to change and logs a message to that effect, leaving the database untouched.
* The script also uploads its own log `debug/ingest_debug.log`, along with `debug/labno_parse_errors.log`, and `debug/data_json_unmatched_test_names.txt` to Cloudflare R2.

**`main_agent.py`**
* This script orchestrates the execution of `tests_dataset.run_data_generation()` followed by `ingest.run_data_ingestion()`.
* Configures its own logging to `debug/orchestrator_debug.log`.

**Default Values:**
* The following default values are used for fields that are missing, invalid, or unparseable from source data:
* `DEFAULT_DATETIME_STR`: `01/01/1970 00:00` (MM/DD/YYYY HH:MM)
* `DEFAULT_DATE_STR`: `1970-01-01` (YYYY-MM-DD)
* `DEFAULT_DATETIME_DT`: datetime(1970, 1, 1, 0, 0, 0) (corresponding datetime object). This is used as a programmatic placeholder for uninitialized or missing datetime fields, allowing datetime arithmetic to proceed without errors.
* `DEFAULT_STRING`: `N/A`
* `DEFAULT_NUMERIC`: `0.0`
* `DEFAULT_DELAY_STATUS`: `Not Uploaded`
* `DEFAULT_TIME_RANGE`: `Not Uploaded`
* `DEFAULT_URGENCY`: `Not Urgent`
* `CLIENT_IDENTIFIER`: This value is read from the `.env` file. If not found, it defaults to `DefaultClient`. For this agent, it is expected to be `Nakasero`.

**File Locations and Configuration:**
* `APPLICATION_BASE_DIR`: Dynamically determined based on whether the script is run as a PyInstaller bundle or a standalone Python script.
* `.env` file: Expected in `[APPLICATION_BASE_DIR]/.env` for database credentials and R2/S3 configuration.
* LOCAL_PUBLIC_DIR: `[APPLICATION_BASE_DIR]/public`
* DATA_JSON_PATH: `[LOCAL_PUBLIC_DIR]/data.json`
* META_CSV_PATH: `[LOCAL_PUBLIC_DIR]/meta.csv`
* TIMEOUT_CSV_PATH: `[LOCAL_PUBLIC_DIR]/TimeOut.csv`
* MERGED_HOSPITAL_DATA_JSON_PATH: `[LOCAL_PUBLIC_DIR]/tests_dataset.json` (output)
* INVALID_LABNOS_OUTPUT_PATH: `[LOGS_DIR]/data_json_invalid_labnos.txt` (output for logging invalid LabNos)
* UNMATCHED_TEST_NAMES_OUTPUT_PATH: `[LOGS_DIR]/data_json_unmatched_test_names.txt` (output for logging unmatched test names that caused records to be skipped)
* PROCESSED_INVOICES_FILE: `[LOCAL_PUBLIC_DIR]/processed_invoice_numbers.json` (state file for incremental processing)
* LOGS_DIR: `[APPLICATION_BASE_DIR]/debug`
* TESTS_DATASET_LOG: `[LOGS_DIR]/tests_dataset_debug.log`
* INGEST_LOG: `[LOGS_DIR]/ingest_debug.log`
* LABNO_PARSE_ERRORS_LOG: `[LOGS_DIR]/labno_parse_errors.log`
* ORCHESTRATOR_LOG: `[LOGS_DIR]/orchestrator_debug.log`

**Database Connection:**
* Retrieves `DATABASE_URL` from environment variables.
* Establishes a connection to the PostgreSQL database.
* Includes a basic test query to ensure connectivity.

**R2/S3 Log Upload:**
* Utilizes boto3 to upload debug logs (`tests_dataset_debug.log`, `ingest_debug.log`) and the (`data_json_invalid_labnos.txt`, `data_json_unmatched_test_names.txt`) file to an R2/S3 compatible storage bucket.
* R2 credentials (`R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_LOG_BUCKET_NAME`) are loaded from environment variables.
* Upload is attempted in a finally block to ensure logs are uploaded even if database ingestion fails.

**Data Processing Flow:**
* **Incremental Data Loading:**
* The script first reads processed_invoice_numbers.json to get a set of InvoiceNo values that have already been processed in previous runs.
* `data.json` is read using ijson for memory efficiency.
* Only records with an InvoiceNo not found in the `processed_invoice_numbers` set are loaded for processing.
* After successful processing and merging, the `InvoiceNo` values of newly processed records are added to the `processed_invoice_numbers` set, and this updated set is saved back to `processed_invoice_numbers.json`.
* **Invalid LabNos Filtering:**
* The script also reads `data.json` to get `LabNo`s then checks them for time stamps in the first 10 figures (DDMMYYHHMM) as the sole criterion for invalid `LabNo`s.
* Those that don't have valid timestamps (e.g., 2004203499...) or are too short (less than 10 characters) are logged to `debug/data_json_invalid_labnos.txt` with their count of occurrence.
* Records with invalid timestamps found in the `data_json_invalid_labnos` set are not loaded for processing.

* **meta.csv and TimeOut.csv Loading:**
* `meta.csv` is loaded into a dictionary (`meta_data`) using `TestName` (normalized to uppercase and stripped) as the key.
* It provides `TAT`, `LabSection`, and `Price` information. While the `meta.csv` is assumed to be perfect and up-to-date with no empty values, the loading function includes basic error handling for numeric conversions to ensure robustness.
* `TimeOut.csv` is loaded into a dictionary (`timeout_data`) using `FileName` (which corresponds to `InvoiceNo`) as the key.
* If multiple entries for the same `FileName` exist, the one with the latest `CreationTime` is retained.
* `CreationTime` from `TimeOut.csv` is parsed into a datetime object.
* If parsing fails, `DEFAULT_DATETIME_DT` is used.

* **Dual Data Outputs for Granularity & Aggregation:** The processing pipeline will produce two distinct JSON output files:
    * `tests_dataset.json`: Contains individual test records, each with its own unique `ID`, suitable for detailed test-level analysis and other tables.
    * `patients_dataset.json`: Contains aggregated records, where `Lab_Number` is the unique identifier, specifically designed for patient-level patients and progress tracking tables.
* **Daily TAT Calculation Logic (LabNo-based for Patient-Level Data):** For the `patients_dataset.json` output, the `Daily_TAT` is calculated as the *maximum* TAT among all individual tests associated with a given `LabNo`, applying the specified tiered categorization. This effectively provides the longest waiting period for a particular patient based on their their soonest period category (i.e., the period in which one expects the first report).
* The `Request_Time_Out` for `LabNo`s with more than one `InvoiceNo` is the `TestCompletionTime` of the `InvoiceNo` with *latest* completion time among all `InvoiceNo`s for that `LabNo`.
* **`Request` and `Test` Prefixes:** Fields with the `Request` prefix (`Request_Progress`, `Request_Delay_Status`, `Request_Time_Range`, `Request_Time_Expected`, `Time_In`, `Request_Time_Out`) are for Patient-Level Fields and are exclusively calculated and present in the `patients_dataset.json` to reflect the aggregated, patient-level metrics. Corresponding `Test` prefixed fields are omitted from this aggregated output, emphasizing its patient-centric nature.
* **Skipping Unmatched TestNames:** If a TestName from `data.json` does not have a corresponding entry in `meta.csv`, the individual test record is SKIPPED from processing. These skipped `TestName` values are logged to `debug/data_json_unmatched_test_names.txt`. This is because `meta.csv` is always perfect and up-to-date.
* **`CLIENT_IDENTIFIER` Source:** `CLIENT_IDENTIFIER` is read from the `.env` file, with `DefaultClient` as a fallback. The expected value for this agent is `Nakasero`.
* **Urgency Default Value:** The default value for the `Urgency` field has been set to `Not Urgent`.
* **`UNMATCHED_TEST_NAMES_OUTPUT_PATH` Location:** The output file for unmatched test names (`data_json_unmatched_test_names.txt`) has been moved to the debug directory (`LOGS_DIR`) for consistency with other log files.
* **PyInstaller Path Handling:** Has `import sys` and logic to `APPLICATION_BASE_DIR` determination to correctly handle paths when the script is bundled with PyInstaller.
* **Robustness in `load_meta_data`:** While `meta.csv` is expected to be perfect, try-except blocks are added for TAT and Price conversion in `load_meta_data` as a safeguard against potential non-numeric values, logging errors and assigning `DEFAULT_NUMERIC` if issues occur.

**Field Mapping and Deriving:**
* `Unit`: A field in the merged data is explicitly mapped to the `Src` field from the raw `data.json` record.
* `Test_Names`: A list of all unique test names performed under this `LabNo`.
* `Lab_Sections`: A list of all unique lab sections involved for this `LabNo`.
* `Request_Time_Out`: This field is still looked up from `TimeOut.csv` using `InvoiceNo`.
* The *latest* `TestCompletionTime` among all `InvoiceNo`s for a given `LabNo`.
* If `InvoiceNo` is found, `CreationTimeStr` (MM/DD/YYYY HH:MM) from `TimeOut.csv` is used.
* If `InvoiceNo` is not found, it defaults to `DEFAULT_DATETIME_STR` (`01/01/1970 00:00`).
* `Daily_TAT`: Calculated as the *maximum* of individual TATs for all tests under a given `LabNo`, applying a tiered logic (max TAT < 12hrs, then < 24hrs, < 3 days, < 5 days, < 10 days, otherwise max of all). This is crucial for tracking patient-level TAT.
* It first attempts to find the maximum TAT among tests for that InvoiceNo that are less than 12 hours (720 minutes).
* If no such test exists, it then checks for tests less than 24 hours (1440 minutes).
* Then less than 3 days (4320 minutes).
* Then less than 5 days (7200 minutes).
* And finally less than 10 days (14400 minutes).
* If all tests associated with an InvoiceNo have TATs greater than or equal to 10 days, or if no TATs are found, the `Daily_TAT` will be the maximum of all available TATs.
* Defaults to 0.0 if no TATs are available for calculation.
* `Request_Time_Expected`: Calculated based on `Time_In` and the new `Daily_TAT` for the `LabNo`.
* `Time_In`: This field is now assigned the exact same value as `Time_In` (i.e., merged_record['Time_In'] = merged_record['Time_In']).
* No parsing from `TestResultDate` is performed.
* `Date`: Derived from `EncounterDate` in `data.json` (format YYYY-MM-DD).
* If parsing fails, `DEFAULT_DATE_STR` (`1970-01-01`) is used.
* `Shift`: Determined from Time_In (derived from LabNo):
* *Day Shift*: `Time_In` between 08:00 (inclusive) and 19:59 (inclusive).
* *Night Shift*: `Time_In` between 20:00 (inclusive) and 07:59 (inclusive).
* `Lab_Section`, `TAT`, `Price`:
* Looked up from `meta_data` using `Test_Name` (normalized).
* If `Test_Name` is not found in `meta.csv`, the record is SKIPPED from further processing, and the unmatched `Test_Name` is logged to `debug/data_json_unmatched_test_names.txt`.
* No default values are assigned for these fields in skipped records.
* `Request_Time_Expected`: Is calculated as `Time_In` + `Daily_TAT`.
* `Test_Time_Expected` is calculated as `Time_In` + `TAT`.
* `Time_Received`: This field is explicitly initialized to `DEFAULT_DATETIME_STR` (`01/01/1970 00:00`) and is not derived from `TimeOut.csv`.
* It is expected to be populated by user action (e.g., clicking a `Receive` button).
* `Test_Time_Out`: This field is initialized to `DEFAULT_DATETIME_STR` (`01/01/1970 00:00`) and is expected to be populated by user action (e.g., clicking a `Result` button).
* `Request_Time_Out`: 
* `Request_Progress`: Calculated as a percentage = ((Current_Time - Time_In) / (Request_Time_Expected - Time_In)) * 100.
* `Test_Progress`: Calculated as a percentage = ((Current_Time - Time_In) / (Test_Time_Expected - Time_In)) * 100.
* Values are capped between 0% and 100%.
* `Client`: Static `CLIENT_IDENTIFIER` constant defined in the script (e.g., `Nakasero`).
* A unique identifier for the hospital/clinic/lab from which the data originates.
* This value is read from the `.env` file.
* `Unit`: This field is derived directly from the `Src` field in the `data.json` record. If `Src` is missing, it defaults to `DEFAULT_STRING`.
* `Urgency`: This field is initialized to `DEFAULT_URGENCY` (`Not Urgent`).
* `Time Range Logic`: Time_Range is formatted as `X hrs Y mins`.
* *`Request_Delay_Status` and `Test_Delay_Status`:*
* If `Test_Time_Out` or `Request_Time_Out` is not available (i.e., `blank` / `null` or `N/A` or `01/01/1970 00:00` or `Not Uploaded`): `Not Uploaded`.
* If the delay is 15 minutes or more (test result delivered 15 minutes or more late): `Over Delayed`.
* If the delay is greater than 0 but less than 15 minutes (test result delivered late, but by less than 15 minutes): `Delayed for less than 15 minutes`.
* If the delay is 0 or negative (test delivered on time or early), and the absolute value of the delay is 30 minutes or less (not more than 30 minutes early): `On Time`.
* If the delay is less than -30 minutes (test result delivered more than 30 minutes early): `Swift`.
* `Lab_Number`: Is derived by from the `LabNo` in `data.json`.
* `Request_Time_Out`: Is derived by parsing the `LabNo`.
* Accepts LabNo with >= 10 values which correspond to formats like DDMMYYHHMM, DDMMYYHHMMS, DDMMYYHHMMSS, DDMMYYHHMMSSS, etc.
* Crucially, it extracts and parses the first 10 digits (DDMMYYHHMM).
* No invalid `LabNo`s (invalid date/time components or too short, less than 10) are expected here because they were filtered out earlier and therefore `DEFAULT_DATETIME_DT` (datetime(1970, 1, 1, 0, 0, 0)) is never used for `Time_In` nor `Time_In` in a successfully processed record.

**Output Generation:**
* All processed records are collected into a list.
* This list is then written to `tests_dataset.json` as a JSON array.
* The `processed_invoice_numbers.json` file is updated.
* A `data_json_unmatched_test_names.txt` file is generated, listing all `TestName` values from `data.json` that did not have a corresponding entry in `meta.csv` and thus caused the record to be skipped.
* A `data_json_invalid_labnos.txt` file is generated, listing those LabNos from `data.json` that didn't have a valid timestamp, or were less than 10 in length.

**Logging:**
* Debug messages are printed to the console and appended to respective `.log` files within the debug directory.
* Critical errors during record processing will still be logged but will not cause the record to be skipped entirely; instead, default values will be used (unless the record is explicitly skipped due to invalid `LabNo` or unmatched `TestName`).
* Robust error handling is in place to log issues during database operations and R2/S3 uploads.

**Database Table Creation:**
* Ensures database update without previous data loss.
* Tables are created with specific schemas designed to store the merged hospital data.
* The PostgreSQL, by default, converts unquoted column names to lowercase, so `CREATE TABLE` statements have to be ware of that.
* **patients:** `Lab_Number`, `Client`, `Date`, `Shift`, `Unit`, `Time_In`, `Daily_TAT`, `Request_Time_Expected`, `Request_Time_Out`, `Request_Delay_Status`, `Request_Time_Range`.
* **tests:** `ID`, `Lab_Number`, `Test_Name`, `Lab_Section`, `TAT` (individual test TAT from `public/meta.csv`), `Price`, `Time_Received`, `Test_Time_Expected`, `Urgency`, `Test_Time_Out`.
* `patients` table (using `Lab_Number` as its primary key).
* `tests` table (using `ID` as its primary key).

**Data Ingestion:**
* Reads the `tests_dataset.json` and `patients_dataset.json` files.
* `patients_dataset.json` (LabNo-unique aggregated data) generates `patients` and `progress` tables (using `Lab_Number` as its primary key).
* `tests_dataset.json` generates `Overview`, `tests`, `Reception` and `tests` tables (using `ID` as its primary key).
* Iterates through the merged data and inserts/updates records into each of the six tables in batches (BATCH_SIZE = 500).
* Datetime strings are parsed into Python datetime objects before insertion into the database.
* If parsing fails, or if the string is DEFAULT_DATETIME_STR, None is inserted into the database field (corresponding to NULL).
* The `calculate_delay_status_and_range` function is imported from `transform.py` and used for populating delay status fields in `patients`, `tests`, and `Overview` tables.
* This function now correctly interprets DEFAULT_DATETIME_STR for `Request_Delay_Status` and `Test_Delay_Status` as `Not Uploaded`.
* After all data is processed, an ANALYZE command is run on the database to update statistics, which can help with query patients.
* The `tests_dataset.json` file, created by transform.py, does not contain a field named `Time_In`.
* It contains a field named Time_Received which should not be confused with `Time_In`.
* The `Time_In` field is part of the *patient-level data*, while `Time_Received` is for the individual test records.
* It checks if the `request_time_out` parameter is the default value (`DEFAULT_DATETIME_DT`). If it is, it will return `Not Uploaded` for the status and range, as this indicates that the data for that field is missing.
* It also has a check to ensure that a valid `time_in` or `time_received` value is present before performing any calculations.
