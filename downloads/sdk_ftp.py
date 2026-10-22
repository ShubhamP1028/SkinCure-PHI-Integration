import os
from databricks.sdk import WorkspaceClient

# Replace these with your actual Databricks credentials
DATABRICKS_HOST = "https://adb-2156656001216832.12.azuredatabricks.net/" # Your Workspace URL
DATABRICKS_TOKEN = "dapie5de1f11bf708028d1def686e493cfe0-3"          # Your Personal Access Token

# Initialize the Client explicitly passing the credentials
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Define source and destination
source = r"C:\Users\ShubhamPandey1\OneDrive - ProcDNA Analytics Pvt. Ltd\Documents\SC_NMSC\downloads"
volume_base_path = "/Volumes/us_scdb_phi_dev/comm_bronze/sdk_ftp"

# Loop through and upload
for filename in os.listdir(source):
    if filename.endswith('.csv'):        
        local_file_path = os.path.join(source, filename)
        volume_dest_path = os.path.join(volume_base_path, filename).replace("\\", "/")

        print(f"Uploading {filename} from your local folder...")

        try:
            # FIX: Removed the incorrect 'path=' and 'local_path=' keywords
            w.files.upload_from(
                volume_dest_path, 
                local_file_path, 
                overwrite=True
            )
            print(f"✅ Success! File pushed to Databricks at: {volume_dest_path}\n")
        except Exception as e:
            print(f"❌ Upload failed for {filename}: {e}\n")
