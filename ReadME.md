NYC Payroll Data ETL Project
Project Description
This project is designed to extract, transform, and load (ETL) NYC payroll data into a data warehouse for analysis. The data pipeline processes raw CSV files stored in Google Cloud Storage (GCS), performs necessary transformations, and loads the cleaned and structured data into a data warehouse. The processed data is structured into dimensional tables for easy querying and reporting, supporting key business questions about payroll expenditures across different agencies and titles.

Tools Used
Python: The primary programming language used for data processing.
Pandas: A Python library used for data manipulation and analysis.
Google Cloud Storage (GCS): Used for storing the raw and transformed CSV files.
Google Cloud SDK: To interact with GCS and other Google Cloud services.
SQLAlchemy: Used to interact with the SQL databases for loading the data.
PostgreSQL / Snowflake / BigQuery: The data warehouse solutions used for storing the transformed data.
Stored Procedures: For moving data from the STG schema to the EDW schema.


Warehouse Schema
The data warehouse schema is designed as a star schema, consisting of the following tables:

Fact Table

FactPayroll_Table
EmployeeID (Foreign Key from DimEmployee)
AgencyID (Foreign Key from DimAgency)
TitleCode (Foreign Key from DimTitle)
FiscalYear
PayrollNumber
BaseSalary
RegularHours
RegularGrossPaid
OTHours
TotalOTPaid
TotalOtherPay

Dimension Tables
DimEmployee
EmployeeID (Primary Key)
LastName
FirstName
AgencyStartDate
WorkLocationBorough

DimAgency
AgencyID (Primary Key)
AgencyName

DimTitle
TitleCode (Primary Key)
TitleDescription

Aggregate Table
PayrollAggregate
AgencyID
TitleCode
TotalBaseSalary
TotalOTHours
TotalOTPaid
TotalOtherPay

ETL Process Overview

1. Data Extraction
Raw CSV files (EmpMaster.csv, Payroll.csv, AgencyMaster.csv, TitleMaster.csv) are downloaded from a GCS bucket.

2. Data Transformation
Dimensional Tables: Extract relevant data from the Payroll.csv to create DimEmployee, DimAgency, and DimTitle.
Fact Table: Extract and format the payroll data to create the FactPayroll_Table, ensuring that the necessary foreign keys and metrics are included.
Aggregate Table: Aggregate payroll data by AgencyID and TitleCode to generate total payroll statistics like TotalBaseSalary, TotalOTHours, TotalOTPaid, and TotalOtherPay.

3. Data Loading
The transformed data is saved to new CSV files in a local directory (cleandata).
These CSV files are uploaded back to GCS in the transformedfiles/ directory.
The data is then loaded into the respective tables in the STG schema of the data warehouse (e.g., BigQuery, Snowflake, PostgreSQL).

4. Data Migration to EDW
Once data is loaded into the STG schema, a stored procedure is executed to transfer and aggregate data into the EDW schema.
This process ensures that the data warehouse is optimized for querying and reporting.

5. Clean-Up
Temporary files generated during the process are deleted to free up resources.

6. Execution
The entire ETL process can be executed by running the transform.py script, which is modular and handles both transformation and loading.

Step-by-Step Execution Guide
Setup:
Install Dependencies:
Ensure you have Python installed along with the required libraries (pandas, google-cloud-storage, sqlalchemy).

Authenticate Google Cloud SDK:
Authenticate your Google Cloud SDK with the service account key JSON file.
Configure:

Update GCS Bucket:
Update the bucket_name in the transform_data() function to point to your GCS bucket.

Provide Service Account Key:
Provide the path to your Google Cloud service account key in the script.

Run the Script:
Execute the ETL Process:
Run the transform.py script using a Python environment. This will process the raw data, perform the transformations, save the outputs, and upload them to GCS.

Load Data to Data Warehouse:
Run loading.py:
This script will load the transformed CSV files from GCS to the STG schema of the respective data warehouses (BigQuery, Snowflake, PostgreSQL).
Execute Stored Procedure:

Automated Execution:
The loading.py script automatically calls a stored procedure (STG.InsertIntoEDW) to migrate data from the STG schema to the EDW schema.
Query and Analyze:

Start Querying:
Once the data is loaded into the EDW schema, you can start querying the data using SQL to answer key business questions regarding payroll distribution across agencies and titles.

Conclusion
This project automates the ETL process for NYC payroll data, making it easy to maintain and analyze payroll information in a structured manner. With the modularity of the Python scripts, you can easily adapt the process for other datasets or extend its capabilities by adding more transformations and aggregations.

This README file provides sufficient guidance and documentation for anyone using or extending the ETL pipeline for the NYC payroll data project
