from google.cloud import bigquery, storage
import pandas as pd
from sqlalchemy import create_engine, text
import snowflake.connector
import os
import tempfile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def download_from_gcs(bucket_name, blob_name, service_account_key):
    """Download a file from GCS to a temporary location."""
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Ensure the blob exists
    if not blob.exists():
        raise FileNotFoundError(f"The file {blob_name} does not exist in the bucket {bucket_name}.")
    
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    blob.download_to_filename(temp_file.name)
    return temp_file.name

def load_csv_to_bigquery(dataset_id, table_id, csv_file, service_account_key):
    client = bigquery.Client.from_service_account_json(service_account_key)
    uri = f"gs://{csv_file}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # This will overwrite the existing table
    )
    
    table_ref = client.dataset(dataset_id).table(table_id)
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )
    
    try:
        load_job.result()
        table = client.get_table(table_ref)
        print(f"Loaded {table.num_rows} rows into {table_id} in BigQuery")
    except Exception as e:
        print(f"Error loading data into BigQuery: {e}")

def get_postgres_engine():
    """Create a PostgreSQL engine using environment variables."""
    user = os.getenv('pg_user', 'default_user')
    password = os.getenv('pg_password', 'default_password')
    host = os.getenv('pg_host', 'localhost')
    port = os.getenv('pg_port', '5432')
    database = os.getenv('pg_database', 'default_database')

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(db_url)
    return engine

def load_csv_to_postgres(csv_file, table_name, service_account_key):
    """Download CSV from GCS and load it into PostgreSQL."""
    temp_file = None
    try:
        # Download file from GCS to a temporary location
        temp_file = download_from_gcs('nycpayroll-bucket', csv_file, service_account_key)
        
        engine = get_postgres_engine()
        df = pd.read_csv(temp_file)
        df.to_sql(name=table_name, con=engine, schema='stg', if_exists='append', index=False)
        num_rows = df.shape[0] 
        print(f"Loaded {num_rows} rows into {table_name} in PostgreSQL")
        print(f"Loaded data into STG.{table_name} in PostgreSQL")
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
    finally:
        # Clean up temporary file
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)




def get_snowflake_connection():
    """Create a Snowflake connection using environment variables."""
    conn = snowflake.connector.connect(
        user=os.getenv('sn_user'),
        password=os.getenv('sn_password'),
        account=os.getenv('sn_account_identifier'),
        warehouse=os.getenv('sn_warehouse'),
        database=os.getenv('sn_database'),
        schema=os.getenv('sn_schema')
    )
    return conn

def load_csv_to_snowflake(csv_file, table_name, service_account_key):
    """Download CSV from GCS and load it into Snowflake."""
    temp_file = None
    try:
        # Download file from GCS to a temporary location
        temp_file = download_from_gcs('nycpayroll-bucket', csv_file, service_account_key)

        # Read the CSV file to get the number of rows
        df = pd.read_csv(temp_file)
        num_rows = df.shape[0]  
        
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Upload file to Snowflake stage
        cur.execute(f"PUT file://{temp_file} @~/staging_area AUTO_COMPRESS=TRUE")
        
        # Copy data into Snowflake table
        cur.execute(f"""
            COPY INTO STG.{table_name}
            FROM @~/staging_area/{os.path.basename(temp_file)}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1);
        """)
        
        cur.close()
        conn.close()
        print(f"Loaded {num_rows} rows into {table_name} in Snowflake")
        print(f"Loaded data into STG.{table_name} in Snowflake")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Snowflake SQL compilation error: {e}")
    except Exception as e:
        print(f"Error loading data into Snowflake: {e}")
    finally:
        # Clean up temporary file
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)



def call_stored_procedure(database_type):
    try:
        if database_type == 'postgres':
            engine = get_postgres_engine()
            with engine.connect() as conn:
                # Check if the procedure exists
                result = conn.execute(text("SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'insertintoedw')"))
                procedure_exists = result.fetchone()[0]
                
                if procedure_exists:
                    # Execute stored procedure
                    conn.execute(text("CALL stg.insertintoedw();"))
                    print("Stored procedure stg.insertintoedw executed successfully in PostgreSQL")
                else:
                    print("Stored procedure stg.insertintoedw does not exist in PostgreSQL")

        elif database_type == 'snowflake':
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute("CALL STG.InsertIntoEDW();")
            cur.close()
            conn.close()
            print("Stored procedure STG.InsertIntoEDW executed successfully in Snowflake")
    except Exception as e:
        print(f"Error executing stored procedure in {database_type}: {e}")



def load_data_to_databases(service_account_key):
    """Load data from GCS to BigQuery, PostgreSQL, and Snowflake."""
    files = [
        {"filepath": "transformedfiles/DimEmployee.csv", "table_id": "DimEmployee"},
        {"filepath": "transformedfiles/FactPayroll_Table.csv", "table_id": "FactPayroll_Table"},
        {"filepath": "transformedfiles/DimAgency.csv", "table_id": "DimAgency"},
        {"filepath": "transformedfiles/DimTitle.csv", "table_id": "DimTitle"},
        {"filepath": "transformedfiles/PayrollAggregate.csv", "table_id": "PayrollAggregate"}
    ]

    # Load to BigQuery
    for file in files:
        load_csv_to_bigquery("nycpayroll_table", file['table_id'], f"nycpayroll-bucket/{file['filepath']}", service_account_key)

    # Load to PostgreSQL
    for file in files:
        load_csv_to_postgres(file['filepath'], file['table_id'], service_account_key)

    # Load to Snowflake
    for file in files:
        load_csv_to_snowflake(file['filepath'], file['table_id'], service_account_key)

    # Call stored procedures
    call_stored_procedure('postgres')
    call_stored_procedure('snowflake')