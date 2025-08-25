import os
import sys
import boto3
from dotenv import load_dotenv

def get_application_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

APPLICATION_BASE_DIR = get_application_base_dir()
load_dotenv(os.path.join(APPLICATION_BASE_DIR, '.env'))

# --- R2 Configuration ---
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_CLIENT_FOLDER = os.getenv('R2_CLIENT_FOLDER')

# List of agent executables to upload
AGENT_EXECUTABLES = [
    'Zyntel_Fast_Agent_Nakasero.exe',
    'Zyntel_Slow_Agent_Nakasero.exe'
]

def upload_agent_to_r2(file_path, bucket_name, client_folder):
    if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, bucket_name, client_folder]):
        print("R2 credentials or client folder name are incomplete. Skipping upload.")
        return

    try:
        s3 = boto3.client('s3',
                          endpoint_url=R2_ENDPOINT_URL,
                          aws_access_key_id=R2_ACCESS_KEY_ID,
                          aws_secret_access_key=R2_SECRET_ACCESS_KEY)

        # Construct the key with the client folder prefix
        object_key = f"{client_folder}/{os.path.basename(file_path)}"
        
        print(f"Uploading {file_path} to R2 bucket '{bucket_name}' under key '{object_key}'...")
        s3.upload_file(str(file_path), bucket_name, object_key)
        print(f"Successfully uploaded {file_path} to R2.")
    except Exception as e:
        print(f"Failed to upload {file_path} to R2: {e}")

def main():
    dist_folder = os.path.join(APPLICATION_BASE_DIR, 'dist')
    
    if not os.path.exists(dist_folder):
        print("Error: 'dist' folder not found. Please compile the agents first.")
        return

    for agent_name in AGENT_EXECUTABLES:
        agent_path = os.path.join(dist_folder, agent_name)
        if os.path.exists(agent_path):
            upload_agent_to_r2(agent_path, R2_BUCKET_NAME, R2_CLIENT_FOLDER)
        else:
            print(f"Warning: {agent_name} not found in the 'dist' folder.")

if __name__ == '__main__':
    main()