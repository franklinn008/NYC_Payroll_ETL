This README provides an overview of the ETL pipeline for processing NYC Payroll data. The pipeline is divided into three main stages: Extraction, Transformation, and Loading, each represented by a Python script (extraction.py, transformation.py, loading.py). Below is a detailed description of each stage and what was implemented.

1. extraction.py
Purpose:
The extraction.py script is responsible for extracting raw data from local CSV files, performing initial preprocessing, and uploading the processed files to a Google Cloud Storage (GCS) bucket.

Key Operations:
Loading CSV Files:

Uses the load_csv_files function to load all CSV files that match the specified file pattern (e.g., EmpMaster*.csv, nycpayroll_*.csv).
Concatenates multiple CSV files into a single DataFrame for easier processing.
Initial Preprocessing:

Extracts key fields (EmployeeID, AgencyStartDate, WorkLocationBorough) from the payroll data.
Determines the earliest AgencyStartDate for each EmployeeID and associates the corresponding WorkLocationBorough.
Merges the employee data with agency and title information to enrich the employee data.
Uploading to GCS:

The processed DataFrames (DimEmployee, FactPayroll_Table, DimAgency, DimTitle) are saved as CSV files.
These CSV files are then uploaded to a specified GCS bucket.
Usage:
python
Copy code
# Example of how to call the function
extract_and_upload_data("path/to/your/service_account_key.json", base_path="/mnt/data")
2. transformation.py
Purpose:
The transformation.py script is responsible for performing further data transformation on the files uploaded to GCS during the extraction phase. It ensures the data is clean, properly formatted, and ready for loading into BigQuery.

Key Operations:
Downloading from GCS:

Downloads the raw CSV files from GCS that were uploaded during the extraction phase.
Data Transformation:

Datetime Conversion: Converts date fields like AgencyStartDate and PayPeriod to proper datetime formats.
Dropping Duplicates: Ensures no duplicate records exist in key tables.
Additional Transformations: Other transformations specific to payroll data, such as extracting fiscal years or handling missing values, are performed.
Uploading Transformed Data to GCS:

After transformation, the cleaned DataFrames are uploaded back to GCS in a transformedfiles/ directory for subsequent loading into BigQuery.
Usage:
python
Copy code
# Example of how to call the function
transform_data("path/to/your/service_account_key.json")
3. loading.py
Purpose:
The loading.py script handles loading the transformed CSV files from GCS into Google BigQuery. This is the final step in the ETL pipeline.

Key Operations:
Loading to BigQuery:

Uses the load_csv_to_bigquery function to load each transformed CSV file into a corresponding BigQuery table.
The autodetect=True option in LoadJobConfig allows BigQuery to automatically infer the schema from the CSV files.
Configuration:

The script is set up to load multiple files into different tables within the same BigQuery dataset.
Usage:
python
Copy code
# Example of how to call the function
load_data_to_bigquery("path/to/your/service_account_key.json")
Airflow DAG (nyc_payroll_etl_dag)
An Airflow DAG is used to orchestrate the ETL process. The DAG consists of three tasks:

Extract Data: Calls the extract_and_upload_data function from extraction.py.
Transform Data: Calls the transform_data function from transformation.py.
Load Data: Calls the load_data_to_bigquery function from loading.py.
The tasks are executed in sequence, ensuring that data flows smoothly from extraction to transformation and finally to loading.

python
Copy code
# DAG Task Dependencies
extract_task >> transform_task >> load_task
Summary
This ETL pipeline automates the process of extracting, transforming, and loading NYC Payroll data into Google BigQuery. Each stage of the process is modularized into distinct Python scripts for easy maintenance and scalability. The Airflow DAG orchestrates the entire workflow, ensuring that data is processed in the correct sequence and is consistently available for analysis in BigQuery.