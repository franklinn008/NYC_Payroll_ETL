from google.cloud import bigquery

def load_csv_to_bigquery(dataset_id, table_id, csv_file_uri, service_account_key):
    client = bigquery.Client.from_service_account_json(service_account_key)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row in the CSV file
        autodetect=True       # Automatically detect the schema
    )
    
    table_ref = client.dataset(dataset_id).table(table_id)
    load_job = client.load_table_from_uri(
        csv_file_uri,   # The URI for the CSV file in GCS
        table_ref,      # The reference to the BigQuery table
        job_config=job_config  # Configuration for the loading job
    )
    
    load_job.result()  # Wait for the job to complete
    
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_id} table.")

def load_data_to_bigquery(service_account_key):
    # List of files and their corresponding BigQuery table names
    files = [
        {"filepath": "your-bucket-name/transformedfiles/DimEmployee.csv", "table_id": "DimEmployee"},
        {"filepath": "your-bucket-name/transformedfiles/FactPayroll_Table.csv", "table_id": "FactPayroll_Table"},
        {"filepath": "your-bucket-name/transformedfiles/DimAgency.csv", "table_id": "DimAgency"},
        {"filepath": "your-bucket-name/transformedfiles/DimTitle.csv", "table_id": "DimTitle"}
    ]

    # BigQuery dataset ID where the tables will be loaded
    dataset_id = "your_dataset_id"  # Replace with your BigQuery dataset ID

    # Iterate through the files and load each one into its corresponding BigQuery table
    for file in files:
        load_csv_to_bigquery(dataset_id, file['table_id'], f"gs://{file['filepath']}", service_account_key)

# Example usage:
# load_data_to_bigquery('path/to/your/service_account_key.json')
