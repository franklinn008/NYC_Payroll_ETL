import pandas as pd
from google.cloud import storage
import os
import glob

def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(data)

def load_csv_files(base_path, file_pattern):
    # Identify all files matching the pattern
    file_paths = glob.glob(os.path.join(base_path, file_pattern))
    dfs = [pd.read_csv(file) for file in file_paths]
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def extract_and_upload_data(service_account_key, base_path):
    bucket_name = "your-bucket-name"  # Replace with your GCS bucket name
    
    # Define paths for raw data storage
    raw_data_dir = os.path.join(base_path, 'rawdata')
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)
    print(f"Raw data directory: {raw_data_dir}")
    
    # Step 1: Load Data
    emp_df = load_csv_files(base_path, "EmpMaster*.csv")
    payroll_df = load_csv_files(base_path, "nycpayroll_*.csv")
    agency_df = pd.read_csv(os.path.join(base_path, 'AgencyMaster.csv'))
    title_df = pd.read_csv(os.path.join(base_path, 'TitleMaster.csv'))
    
    # Step 2: Save raw data to CSV files
    emp_df.to_csv(os.path.join(raw_data_dir, "raw_EmpMaster.csv"), index=False)
    payroll_df.to_csv(os.path.join(raw_data_dir, "raw_Payroll.csv"), index=False)
    agency_df.to_csv(os.path.join(raw_data_dir, "raw_AgencyMaster.csv"), index=False)
    title_df.to_csv(os.path.join(raw_data_dir, "raw_TitleMaster.csv"), index=False)
    
    # Step 3: Upload the raw CSV files to GCS
    upload_to_gcs(bucket_name, "rawfiles/raw_EmpMaster.csv", os.path.join(raw_data_dir, "raw_EmpMaster.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_Payroll.csv", os.path.join(raw_data_dir, "raw_Payroll.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_AgencyMaster.csv", os.path.join(raw_data_dir, "raw_AgencyMaster.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_TitleMaster.csv", os.path.join(raw_data_dir, "raw_TitleMaster.csv"), service_account_key)

# Example of how to call the function
# extract_and_upload_data("path/to/your/service_account_key.json", base_path="/mnt/data")
