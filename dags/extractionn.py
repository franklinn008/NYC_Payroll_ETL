import pandas as pd
from google.cloud import storage
import os
import glob

def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    try:
        print(f"Uploading {data} to bucket {bucket_name} as {blob_name}")
        
        # Log file size or a sample of the file content
        print(f"Preparing to upload {data}. File size: {os.path.getsize(data)} bytes")
        with open(data, 'r') as file:
            print(f"Sample content:\n{file.read(200)}\n")
        
        client = storage.Client.from_service_account_json(service_account_key)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(data)
        
        print(f"Upload complete for {blob_name}")
    except Exception as e:
        print(f"Failed to upload {blob_name}: {e}")

def load_csv_files(base_path, file_pattern):
    file_paths = glob.glob(os.path.join(base_path, file_pattern))
    print(f"Found files: {file_paths}")
    if not file_paths:
        print(f"No files found with pattern: {file_pattern}")
    dfs = [pd.read_csv(file) for file in file_paths]
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def extract_and_upload_data(service_account_key, base_path):
    bucket_name = "nycpayroll-bucket"  
    base_path = "C:/Users/User/OneDrive/Desktop/Capstone/rawdata"
    
    raw_data_dir = os.path.join(base_path, 'rawdata')
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)
    print(f"Raw data directory: {raw_data_dir}")
    
    # Step 1: Loading Data
    emp_df = load_csv_files(base_path, "EmpMaster*.csv")
    payroll_df = load_csv_files(base_path, "nycpayroll_*.csv")
    agency_df = load_csv_files(base_path, 'AgencyMaster.csv')
    title_df = load_csv_files(base_path, 'TitleMaster.csv')

    # Print column names for debugging
    print(f"emp_df columns: {emp_df.columns.tolist()}")
    print(f"agency_df columns: {agency_df.columns.tolist()}")

    # Save the raw data to CSV files without any merging or further transformations
    emp_df.to_csv(os.path.join(raw_data_dir, "raw_EmpMaster.csv"), index=False)
    payroll_df.to_csv(os.path.join(raw_data_dir, "raw_Payroll.csv"), index=False)
    agency_df.to_csv(os.path.join(raw_data_dir, "raw_AgencyMaster.csv"), index=False)
    title_df.to_csv(os.path.join(raw_data_dir, "raw_TitleMaster.csv"), index=False)
    
    # Step 2: Uploading the raw CSV files to GCS
    print("Starting upload process...")
    upload_to_gcs(bucket_name, "rawfiles/raw_EmpMaster.csv", os.path.join(raw_data_dir, "raw_EmpMaster.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_Payroll.csv", os.path.join(raw_data_dir, "raw_Payroll.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_AgencyMaster.csv", os.path.join(raw_data_dir, "raw_AgencyMaster.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_TitleMaster.csv", os.path.join(raw_data_dir, "raw_TitleMaster.csv"), service_account_key)

    print("Raw Dataset uploaded to GCS bucket successfully")

# Example of how to call the function
# extract_and_upload_data(r"C:/Users/User/OneDrive/Desktop/Capstone/dags/etl/service_account_key.json", base_path="C:/Users/User/OneDrive/Desktop/Capstone/rawdata")