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
    
    # Step 2: Extract required fields from payroll for DimEmployee
    payroll_emp_fields = payroll_df[['EmployeeID', 'AgencyStartDate', 'WorkLocationBorough']]
    
    # Get the earliest AgencyStartDate for each EmployeeID
    payroll_emp_fields = payroll_emp_fields.groupby('EmployeeID').agg({
        'AgencyStartDate': 'min',
        'WorkLocationBorough': 'first'
    }).reset_index()

    # Merge employee data with agency, title data, and additional fields from payroll
    emp_df = emp_df.merge(payroll_emp_fields, on='EmployeeID', how='left')
    emp_df = emp_df.merge(agency_df, on='AgencyID', how='left')
    emp_df = emp_df.merge(title_df, on='TitleCode', how='left')
    
    # Step 3: Add Fiscal Year to Payroll data
    payroll_df['FiscalYear'] = pd.to_datetime(payroll_df['PayPeriod']).dt.year
    
    # Merge payroll data with employee data
    payroll_fact_df = payroll_df.merge(emp_df, on='EmployeeID', how='left')
    
    # Step 4: Save transformed data to CSV files
    emp_df.to_csv(os.path.join(raw_data_dir, "DimEmployee.csv"), index=False)
    payroll_fact_df.to_csv(os.path.join(raw_data_dir, "FactPayroll_Table.csv"), index=False)
    agency_df.to_csv(os.path.join(raw_data_dir, "DimAgency.csv"), index=False)
    title_df.to_csv(os.path.join(raw_data_dir, "DimTitle.csv"), index=False)
    
    # Step 5: Upload the transformed CSV files to GCS
    upload_to_gcs(bucket_name, "DimEmployee.csv", os.path.join(raw_data_dir, "DimEmployee.csv"), service_account_key)
    upload_to_gcs(bucket_name, "FactPayroll_Table.csv", os.path.join(raw_data_dir, "FactPayroll_Table.csv"), service_account_key)
    upload_to_gcs(bucket_name, "DimAgency.csv", os.path.join(raw_data_dir, "DimAgency.csv"), service_account_key)
    upload_to_gcs(bucket_name, "DimTitle.csv", os.path.join(raw_data_dir, "DimTitle.csv"), service_account_key)

# Example of how to call the function
# extract_and_upload_data("path/to/your/service_account_key.json", base_path="/mnt/data")
