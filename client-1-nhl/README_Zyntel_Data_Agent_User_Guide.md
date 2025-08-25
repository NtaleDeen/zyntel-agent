Zyntel Data Agent: Quick Start Guide
This guide will walk you through the process of deploying and running the Zyntel Data Agent, a portable executable that automates data fetching and ingestion for your LIMS and other data sources.

📦 Step 1: Agent Setup and File Placement
The Zyntel Data Agent is a portable application that runs without installation. To use it, you must keep all of its files in a single, dedicated directory.

Create a Folder: On the target computer or server, create a new folder, for example, ZyntelAgent.

Copy the Files: Copy the following files and folders into your new ZyntelAgent directory:

main_agent.exe (the executable file)

The public folder, which should contain TimeOut.csv, last_run.txt, and lims_data.json.

The .env file (which contains your configuration settings).

Your final folder structure should look like this:

/ZyntelAgent
├── main_agent.exe
├── .env
└── /public
    ├── TimeOut.csv
    ├── last_run.txt
    └── lims_data.json

📝 Step 2: Configure the .env File
The .env file is where you configure the agent's settings without modifying the code. It is critical that you open this file in a text editor and ensure the values are correct for your environment.

Variable Name

Description

SOURCE_FOLDER

The path to the network shared drive where the new files are created. This is typically the Z: drive.

OUTPUT_TIMEOUT_CSV_NAME

The name of the CSV file that tracks all scanned files. Do not change this unless you intend to use a different filename.

LAST_RUN_TIMESTAMP_NAME

The name of the text file that stores the timestamp of the last successful scan. Do not change this unless you intend to use a different filename.

LIMS_USERNAME

Your LIMS login username.

LIMS_PASSWORD

Your LIMS login password.

Example .env file:

SOURCE_FOLDER="Z:/"
OUTPUT_TIMEOUT_CSV_NAME="TimeOut.csv"
LAST_RUN_TIMESTAMP_NAME="last_run.txt"
LIMS_USERNAME="johndoe"
LIMS_PASSWORD="secure-password-123"

🚀 Step 3: Run the Agent
Once your files are in place and the .env file is configured, you can run the agent.

Double-click main_agent.exe to run it.

A console window will appear, showing the agent's progress as it performs the following tasks:

Fetches patient data from the LIMS web interface.

Scans the Z: drive for new files.

Updates the TimeOut.csv file.

Ingests the data into your PostgreSQL database.

The console will provide logging information, confirming the status of each step.

⏰ Step 4: Automate the Process
To ensure the agent runs automatically at a regular interval (e.g., every 15 minutes), you need to schedule the task.

On Windows: Use the Task Scheduler application to create a new task that runs main_agent.exe on your desired schedule.

On macOS/Linux: Use a cron job to execute the main_agent.exe file at the specified interval.

Note: For the agent to run, the computer must be powered on. If the internet connection is interrupted during a run, the agent is designed with a retry mechanism to handle temporary network issues.