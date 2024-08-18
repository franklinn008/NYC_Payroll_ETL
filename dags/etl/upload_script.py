# Assume this is saved as upload_script.py

from dags.extractionn import extract_and_upload_data

# Adjust the paths as necessary
service_account_key = r"C:/Users/User/OneDrive/Desktop/Capstone/dags/etl/service_account_key.json"
base_path = r"C:/Users/User/OneDrive/Desktop/Capstone/rawdata"

extract_and_upload_data(service_account_key, base_path)
#agency_df = load_csv_files(os.path.join(base_path, 'AgencyMaster.csv'))
#title_df = load_csv_files(os.path.join(base_path, 'TitleMaster.csv'))
