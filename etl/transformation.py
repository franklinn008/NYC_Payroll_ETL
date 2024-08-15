import pandas as pd
from google.cloud import storage
import os
import tempfile

def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = storage.Client.from_service_account_json(service_account_key).get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(data)

def download_from_gcs(bucket_name, blob_name, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    blob.download_to_filename(temp_file.name)
    return temp_file.name

def transform_data(service_account_key):
    bucket_name = "your-bucket-name"  # Replace with your GCS bucket name
    
    wd = os.getcwd()
    print(f"Current working directory: {wd}") 

    transform_path = os.path.join(wd, 'cleandata')
    if not os.path.exists(transform_path):
        os.makedirs(transform_path)
    print(f"Transform data directory: {transform_path}")

    # Download raw data from GCS
    temp_files = {
        "EmpMaster.csv": download_from_gcs(bucket_name, "rawfiles/raw_EmpMaster.csv", service_account_key),
        "Payroll.csv": download_from_gcs(bucket_name, "rawfiles/raw_Payroll.csv", service_account_key),
        "AgencyMaster.csv": download_from_gcs(bucket_name, "rawfiles/raw_AgencyMaster.csv", service_account_key),
        "TitleMaster.csv": download_from_gcs(bucket_name, "rawfiles/raw_TitleMaster.csv", service_account_key)
    }
    
    # Step 1: Load the data into dataframes
    emp_df = pd.read_csv(temp_files["EmpMaster.csv"])
    payroll_df = pd.read_csv(temp_files["Payroll.csv"])
    agency_df = pd.read_csv(temp_files["AgencyMaster.csv"])
    title_df = pd.read_csv(temp_files["TitleMaster.csv"])

    # Step 2: Data Transformations

    # Convert PayPeriod to datetime and extract FiscalYear
    payroll_df['PayPeriod'] = pd.to_datetime(payroll_df['PayPeriod'])
    payroll_df['FiscalYear'] = payroll_df['PayPeriod'].dt.year

    # Extract required fields from payroll for DimEmployee and handle duplicates
    payroll_emp_fields = payroll_df[['EmployeeID', 'AgencyStartDate', 'WorkLocationBorough']].drop_duplicates()
    
    # Get the earliest AgencyStartDate for each EmployeeID
    payroll_emp_fields = payroll_emp_fields.groupby('EmployeeID').agg({
        'AgencyStartDate': 'min',
        'WorkLocationBorough': 'first'
    }).reset_index()

    # Merge employee data with agency, title data, and additional fields from payroll
    emp_df = emp_df.merge(payroll_emp_fields, on='EmployeeID', how='left')
    emp_df = emp_df.merge(agency_df[['AgencyID', 'AgencyName']], on='AgencyID', how='left')
    emp_df = emp_df.merge(title_df[['TitleCode', 'TitleDescription']], on='TitleCode', how='left')
    
    # Merge payroll data with employee data to create the Fact table
    payroll_fact_df = payroll_df.merge(emp_df, on='EmployeeID', how='left')

    # Step 3: Save transformed data to CSV files
    emp_df.to_csv(os.path.join(transform_path, "DimEmployee.csv"), index=False)
    payroll_fact_df.to_csv(os.path.join(transform_path, "FactPayroll_Table.csv"), index=False)
    agency_df.to_csv(os.path.join(transform_path, "DimAgency.csv"), index=False)
    title_df.to_csv(os.path.join(transform_path, "DimTitle.csv"), index=False)
    
    # Step 4: Upload the transformed CSV files to GCS
    upload_to_gcs(bucket_name, "transformedfiles/DimEmployee.csv", os.path.join(transform_path, "DimEmployee.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/FactPayroll_Table.csv", os.path.join(transform_path, "FactPayroll_Table.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/DimAgency.csv", os.path.join(transform_path, "DimAgency.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/DimTitle.csv", os.path.join(transform_path, "DimTitle.csv"), service_account_key)

    # Clean up the temporary files after uploads are complete
    for temp_file in temp_files.values():
        try:
            os.remove(temp_file)
            print(f"Successfully removed temporary file {temp_file}")
        except PermissionError as e:
            print(f"Could not remove temporary file {temp_file}: {e}")

# Example usage
# transform_data("path/to/your/service_account_key.json")
