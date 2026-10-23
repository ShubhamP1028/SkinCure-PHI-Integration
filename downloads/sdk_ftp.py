import os
import re
import pandas as pd
from databricks.sdk import WorkspaceClient

# Replace these with your actual Databricks credentials
DATABRICKS_HOST = "https://adb-2156656001216832.12.azuredatabricks.net/" # Your Workspace URL
DATABRICKS_TOKEN = "dapie5de1f11bf708028d1def686e493cfe0-3"          # Your Personal Access Token

# Initialize the Client explicitly passing the credentials
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Define source and destination
source = r"C:\Users\ShubhamPandey1\OneDrive - ProcDNA Analytics Pvt. Ltd\Documents\SC_NMSC\downloads"
volume_base_path = "/Volumes/us_scdb_phi_dev/comm_bronze/sdk_ftp"
catalog = "main"
schema = "default"
warehouse_id = "040e9eabb51b1045"  

# Regex pattern to isolate the specific user name from the file schema
# Matches 'user_a0' out of 'Northfield_EMR_user_a0_20261022_160745.csv'
user_pattern = re.compile(r"Northfield_EMR_(user_[a-zA-Z0-9]+)_\d{8}_\d{6}\.csv")

# 2. Step One: Catalog all local matching files by User ID
user_file_groups = {}

for filename in os.listdir(source):
    match = user_pattern.match(filename)
    if match:
        user_name = match.group(1)  # Extracts: 'user_a0'
        local_path = os.path.join(source, filename)
        
        if user_name not in user_file_groups:
            user_file_groups[user_name] = []
        user_file_groups[user_name].append(local_path)

# 3. Step Two: Combine data and push directly as a Databricks Table
for user_name, file_paths in user_file_groups.items():
    print(f"--- Processing User: {user_name} (Found {len(file_paths)} files) ---")
    
    # Read and UNION all local CSV files for this specific user
    try:
        df_list = [pd.read_csv(fp) for fp in file_paths]
        combined_df = pd.concat(df_list, ignore_index=True)
        print(f"   ↳ Merged local data rows: {len(combined_df)}")
    except Exception as e:
        print(f"❌ Failed to combine local data for {user_name}: {e}")
        continue
    
    # Define Target Unified Catalog table name
    full_table_path = f"{catalog}.{schema}.{user_name}"
    
    # 4. Stream data directly to Databricks using statement execution parameters
    try:
        print(f"   ⏳ Pushing inline dataset and creating table '{full_table_path}'...")
        
        # Convert the Pandas DataFrame into record batches for the API client
        records = combined_df.to_dict(orient="records")
        
        # Execute parameterized generation query
        w.statement_execution.execute_statement(
            statement=f"CREATE OR REPLACE TABLE {full_table_path} AS SELECT * FROM VALUES (:data)",
            warehouse_id=warehouse_id,
            parameters=[
                {
                    "name": "data",
                    "value": str(records),
                    "type": "STRING" # Handled sequentially inside Databricks runtime execution engine
                }
            ]
        )
        print(f"✅ Success! Generated Databricks Table: {full_table_path}\n")
        
    except Exception as e:
        # Fallback if the raw payload size exceeds API limits: upload temporary stream object
        print(f"⚠️ Direct execution limit reached. Utilizing internal stream protocol for {user_name}...")
        try:
            # Drop the combined dataframe as a temporary stream structure to create the table
            temp_vol_path = f"/Volumes/{catalog}/{schema}/my-volume/temp_{user_name}.csv"
            
            # Convert merged dataframe into clean CSV bytes
            csv_buffer = combined_df.to_csv(index=False).encode('utf-8')
            
            # Send file object buffer using upload stream method
            w.files.upload(temp_vol_path, csv_buffer, overwrite=True)
            
            # Generate table from the structural path stream setup
            w.statement_execution.execute_statement(
                statement=f"""
                CREATE OR REPLACE TABLE {full_table_path} AS 
                SELECT * FROM read_files('{temp_vol_path}', format => 'csv', header => 'true', inferSchema => 'true')
                """,
                warehouse_id=warehouse_id
            )
            
            # Clean up the single temporary cloud stream marker afterwards
            w.files.delete(temp_vol_path)
            print(f"✅ Success via fallback tracking mechanism! Table created at: {full_table_path}\n")
        except Exception as fallback_err:
            print(f"❌ Comprehensive tracking failed for {user_name}: {fallback_err}\n")