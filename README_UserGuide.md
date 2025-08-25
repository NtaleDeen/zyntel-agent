LIMS Data Extract Agent Deployment Guide
Overview
This guide provides instructions for deploying and configuring the LIMS Data Extract Agent, which is responsible for extracting, transforming, and loading LIMS data into the central database.

Prerequisites
Windows Operating System (where the agent will run).

Network access to your R2 bucket for data file downloads (if configured to download from R2).

Network access to your PostgreSQL database (Neon DB).

Environment variables configured for database and R2 access (usually via a .env file placed next to the executable or set at the system level).

Installation Steps
Create Agent Directory:

Create a new folder on your C: drive (or desired location) named LIMSAgent.

Example: C:\LIMSAgent\

Copy Executable:

Copy the LIMSDataAgent.exe file (found in the dist\LIMSDataAgent folder after running PyInstaller) into the C:\LIMSAgent\ directory.

Place Data Files (Initial Setup / Manual Mode):

For the first run and if not downloading from R2, ensure your initial meta.csv, TimeOut.csv, and data.json files are placed within a sub-folder named public inside C:\LIMSAgent\.

Example: C:\LIMSAgent\public\data.json, C:\LIMSAgent\public\meta.csv, C:\LIMSAgent\public\TimeOut.csv

Note: The agent is configured to download these files from R2, so in a typical deployment, you might only need to place the .env file here.

Configure Environment Variables (.env file):

Create a file named .env in the C:\LIMSAgent\ directory (next to LIMSDataAgent.exe).

Populate it with your database and R2 credentials. Replace placeholders with your actual values:

DATABASE_URL="postgresql://your_neon_user:your_neon_password@your_neon_host/your_neon_database?sslmode=require&channel_binding=require"
R2_ACCESS_KEY_ID="your_r2_access_key_id"
R2_SECRET_ACCESS_KEY="your_r2_secret_access_key"
R2_ACCOUNT_ID="your_r2_account_id"
R2_BUCKET_NAME="zyntel-data"
R2_ENDPOINT_URL="https://your_r2_account_id.r2.cloudflarestorage.com"
R2_LOG_BUCKET_NAME="zyntel-debug-logs"

Important: Ensure these environment variables are correctly set, as the agent relies on them to connect to the database and R2.

Running the Agent
Manual Run (for testing)
Open Command Prompt or PowerShell, navigate to C:\LIMSAgent\, and run:

.\LIMSDataAgent.exe

This will execute the data ingestion process once. Check the debug folder for logs.

Scheduling with Windows Task Scheduler (Recommended for Automation)
To automate the data extraction and loading process, you can schedule the LIMSDataAgent.exe to run periodically using Windows Task Scheduler.

Open Task Scheduler: Search for "Task Scheduler" in the Windows Start menu.

Create Basic Task (Quick Setup):

Click Create Basic Task... on the right-hand pane.

Name: LIMS Data Ingestion Agent (or similar)

Trigger: Choose Daily or Weekly for less frequent runs, or When a specific event is logged if you want it to trigger on file arrival (more advanced). For continuous monitoring, Daily with repetition is simpler.

Daily/Weekly/Monthly: Configure the recurrence.

Action: Start a program

Program/script: Browse to C:\LIMSAAgent\LIMSDataAgent.exe

Click Finish. You can then right-click the task and go to Properties for more advanced settings (like repeating every 5 minutes).

For "Create Task..." (Recommended for more control):

Click Create Task... on the right-hand pane.

General Tab:

Name: LIMS Data Agent

Description: Extracts and loads LIMS data to the database.

Select Run whether user is logged on or not (and provide credentials if prompted).

Check Run with highest privileges.

Configure for: Windows 10 (or your appropriate OS).

Triggers Tab:

Click New...

Begin the task: On a schedule

Settings: Daily (or One time and configure repetition below)

Start: Set your desired start date and time (e.g., today's date, 08:00:00).

Check Repeat task every: 5 minutes (or your desired interval, e.g., 1 hour)

for a duration of: Indefinitely

Click OK.

Actions Tab:

Click New...

Action: Start a program

Program/script: C:\LIMSAAgent\LIMSDataAgent.exe

Click OK.

Conditions, Settings Tabs: Review and adjust as needed (e.g., Start the task only if the computer is on AC power can be unchecked for servers).

Click OK to save the task. You will be prompted for credentials if you selected Run whether user is logged on or not.

Support
For any issues or questions, please refer to the documentation or contact the development team.