import pandas as pd
from google.cloud import storage
import io
import os
import glob

def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    try:
        print(f"Uploading {data} to bucket {bucket_name} as {blob_name}")
        
        client = storage.Client.from_service_account_json(service_account_key)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(data)
        
        print(f"Upload complete for {blob_name}")
    except Exception as e:
        print(f"Failed to upload {blob_name}: {e}")

def read_csv_from_gcs(bucket_name, blob_pattern, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=blob_pattern))
    
    if not blobs:
        print(f"No files found with pattern: {blob_pattern}")
        return pd.DataFrame()  # Return an empty DataFrame
    
    dfs = []
    for blob in blobs:
        print(f"Reading file from GCS: {blob.name}")
        csv_data = blob.download_as_text()
        dfs.append(pd.read_csv(io.StringIO(csv_data)))
    
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def process_and_upload_data(service_account_key, base_path):
    bucket_name = "nycpayroll-bucket"  
    raw_data_dir = os.path.join(base_path, 'rawdata')
    
    # Step 1: Loading Data from GCS
    emp_df = read_csv_from_gcs(bucket_name, "raw_EmpMaster", service_account_key)
    payroll_df = read_csv_from_gcs(bucket_name, "raw_Payroll", service_account_key)
    agency_df = read_csv_from_gcs(bucket_name, 'raw_AgencyMaster', service_account_key)
    title_df = read_csv_from_gcs(bucket_name, 'raw_TitleMaster', service_account_key)

    # Print column names for debugging
    print(f"emp_df columns: {emp_df.columns.tolist()}")
    print(f"agency_df columns: {agency_df.columns.tolist()}")

    # Optional: Save data locally if needed
    raw_data_dir = os.path.join(base_path, 'rawdata2')
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)


    # Save the raw data to CSV files without any merging or further transformations
    emp_df.to_csv(os.path.join(raw_data_dir, "raw_EmpMaster.csv"), index=False)
    payroll_df.to_csv(os.path.join(raw_data_dir, "raw_Payroll.csv"), index=False)
    agency_df.to_csv(os.path.join(raw_data_dir, "raw_AgencyMaster.csv"), index=False)
    title_df.to_csv(os.path.join(raw_data_dir, "raw_TitleMaster.csv"), index=False)
    
     # Upload results to GCS if needed
    print("Starting upload process...")
    upload_to_gcs(bucket_name, "processed_files/raw_EmpMaster.csv", os.path.join(raw_data_dir, "raw_EmpMaster.csv"), service_account_key)
    upload_to_gcs(bucket_name, "processed_files/raw_Payroll.csv", os.path.join(raw_data_dir, "raw_Payroll.csv"), service_account_key)
    upload_to_gcs(bucket_name, "processed_files/raw_AgencyMaster.csv", os.path.join(raw_data_dir, "raw_AgencyMaster.csv"), service_account_key)
    upload_to_gcs(bucket_name, "processed_files/raw_TitleMaster.csv", os.path.join(raw_data_dir, "raw_TitleMaster.csv"), service_account_key)

    print("Raw Dataset uploaded to GCS bucket successfully")
