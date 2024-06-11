from opensearchpy import OpenSearch, helpers
import pandas as pd
import numpy as np
import os
import math
import warnings
from datetime import datetime
import wget
import zipfile
import shutil
import getpass
#                          ------------ FUNCTIONS -----------------

# Function to download files from url into file_path
def download_file(url, file_path):
    try:
        wget.download(url, out=file_path)
        print(f"Downloaded: {url}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def transform_id(id_csv): #Function to swap the ID_NOTICE_CAN field so it aligns with the one used in the xml format
    year_part = str(id_csv)[:4]
    id_xml = str(id_csv)[4:].zfill(6) + '-' + year_part
    return id_xml

# Extracts a .zip compressed file and deletes the compressed file
def extract_file(file_path):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(file_path[:-4])

        # Remove the downloaded archive file
        os.remove(file_path)
        # Moving csvs inside extracted folder to the parent folder
        for root, dirs, files in os.walk(file_path[:-4]):
            for file in files:
                if file.endswith('.csv'):
                    source_file_path = os.path.join(root, file)
                    destination_folder = os.path.dirname(root)
                    shutil.move(source_file_path, destination_folder)
                    print(f"Extracted: {file}")
        os.rmdir(file_path[:-4])
    except Exception as e:
        print(f"Failed to extract {file_path}: {e}")


def download_csv(save_folder):
    start_year = 2018
    end_year = 2023
    base_url = 'https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-'
    for year in range(start_year, end_year + 1):
        download_url = f"{base_url}{year}.zip"
        save_path = f"{save_folder}TED_AWARD_{year}.zip"
        download_file(download_url, save_path)
        extract_file(save_path)


def flatten_csv(folder_path):
    dfs = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv') and file_name.startswith('export_CAN'):
            file_path = os.path.join(folder_path, file_name)
            current_df = pd.read_csv(file_path)
            dfs.append(current_df)
    df = pd.concat(dfs, ignore_index=True)

    columns_can_level = ['ID_NOTICE_CAN', 'TED_NOTICE_URL', 'YEAR', 'ID_TYPE', 'DT_DISPATCH', 'XSD_VERSION',
                         'CANCELLED',
                         'CORRECTIONS', 'B_MULTIPLE_CAE', 'CAE_NAME', 'CAE_NATIONALID', 'CAE_ADDRESS', 'CAE_TOWN',
                         'CAE_POSTAL_CODE', 'CAE_GPA_ANNEX', 'ISO_COUNTRY_CODE', 'ISO_COUNTRY_CODE_GPA',
                         'B_MULTIPLE_COUNTRY',
                         'ISO_COUNTRY_CODE_ALL', 'CAE_TYPE', 'EU_INST_CODE', 'MAIN_ACTIVITY',
                         'B_ON_BEHALF', 'B_INVOLVES_JOINT_PROCUREMENT', 'B_AWARDED_BY_CENTRAL_BODY', 'TYPE_OF_CONTRACT',
                         'B_FRA_AGREEMENT', 'FRA_ESTIMATED', 'B_DYN_PURCH_SYST', 'CPV', 'MAIN_CPV_CODE_GPA',
                         'B_GPA', 'GPA_COVERAGE', 'LOTS_NUMBER', 'VALUE_EURO', 'VALUE_EURO_FIN_1', 'VALUE_EURO_FIN_2',
                         'TOP_TYPE',
                         'B_ACCELERATED', 'OUT_OF_DIRECTIVES', 'B_ELECTRONIC_AUCTION', 'NUMBER_AWARDS']
    df_flat = df[columns_can_level]
    df_flat = df_flat.groupby('ID_NOTICE_CAN').first()
    df_flat.index = df_flat.index.to_series().apply(transform_id)
    return df_flat
def parse_date(date_str):
    formats = ['%d-%b-%y', '%d-%m-%y', '%d/%m/%y']  # Add more formats if needed
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError("Date format not recognized: {}".format(date_str))  # Print the problematic value


def format_fields(df_chunk):
    float_cols = ["VALUE_EURO", "VALUE_EURO_FIN_1", "VALUE_EURO_FIN_2"]
    for col in float_cols:
        df_chunk.loc[:, col] = pd.to_numeric(df_chunk[col], errors='coerce')
        df_chunk.loc[:, col].fillna(-1, inplace=True)

    # Apply the parse_date function to the 'DT_DISPATCH' column
    df_chunk.loc[:, 'DT_DISPATCH'] = df_chunk['DT_DISPATCH'].apply(parse_date)

    bool_cols = ["CANCELLED", "B_MULTIPLE_CAE", "B_MULTIPLE_COUNTRY", "B_INVOLVES_JOINT_PROCUREMENT",
                 "B_AWARDED_BY_CENTRAL_BODY", "B_FRA_AGREEMENT", "B_DYN_PURCH_SYST", "B_ELECTRONIC_AUCTION",
                 "B_ON_BEHALF", "B_GPA", "B_ACCELERATED", "OUT_OF_DIRECTIVES"]
    for col in bool_cols:
        df_chunk.loc[:, col].replace({0: False, 1: True, 'N': False, 'Y': True}, inplace=True)
        df_chunk.loc[:, col].fillna(False, inplace=True)

    null_cols = ["ISO_COUNTRY_CODE_ALL", "GPA_COVERAGE", "XSD_VERSION", "TOP_TYPE", "TED_NOTICE_URL", "CAE_GPA_ANNEX",
                 "CAE_POSTAL_CODE", "CAE_NATIONALID", "CAE_ADDRESS", "CAE_TOWN", "MAIN_ACTIVITY", "EU_INST_CODE",
                 "TYPE_OF_CONTRACT", "ISO_COUNTRY_CODE_GPA", "GPA_COVERAGE"]
    for col in null_cols:
        df_chunk.loc[:, col].fillna("None", inplace=True)

    df_chunk.loc[:, 'MAIN_CPV_CODE_GPA'].fillna("0", inplace=True)
    df_chunk.loc[:, "FRA_ESTIMATED"].fillna("No", inplace=True)
    df_chunk.loc[:, "LOTS_NUMBER"].fillna(0, inplace=True)

    return df_chunk

#                               ------------ CONSTANTS -----------------

host = 'localhost'
port = 9200
index='ted-csv'
username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")
auth = (username, password)  # For testing only. Don't store credentials in code.
ca_certs_path = '/full/path/to/root-ca.pem'  # Provide a CA bundle if you use intermediate CAs with your root CA.

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

folder = "./temp/csv/"  #"/home/procure/data/ted/"


#                               ------------ CODE -----------------

download_csv(folder)
if  not os.path.exists(folder):
    os.makedirs(folder)
df = flatten_csv(folder)
lines = df.shape[0]
columns = df.columns


logs = []
print("Lines to upload " + str(lines))
iters = math.ceil(lines / 100000)
for i in range(0, iters):

    start_line = i * 100000
    end_line = min(((i + 1) * 100000 - 1), lines)

    print('Uploading lines ' + str(start_line) + ' to ' + str(end_line))
    df_chunk = df.iloc[start_line:end_line + 1]
    df_chunk = format_fields(df_chunk)
    # Prepare a list of actions for the bulk API
    actions = [
        {
            "_op_type": "index",
            "_index": index,
            "_id": id_doc,
            **{f"{col_name}": doc[col_name] for col_name in columns}
        }
        for id_doc, doc in df_chunk.iterrows()
    ]
    # Use the bulk API to index the documents
    try:
        success, failed = helpers.bulk(client, actions, index=index, raise_on_error=True, refresh=True)
        #Logging
        successful_ids = {action['_id'] for action in actions}
        failed_ids = {failure['index']['_id'] for failure in failed}
        successful_ids -= failed_ids
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for action in actions:
            if action['_id'] in successful_ids:
                logs.append(pd.DataFrame({
                    '_id': action['_id'],
                    '_index': action['_index'],
                    'status': 'success',
                    'error': None,
                    'date': current_date
                }, ignore_index=True))

        for failure in failed:
            action = failure['index'] if 'index' in failure else failure['create']
            error = failure['index']['error'] if 'index' in failure else failure['create']['error']
            document_id = action['_id']
            index = action['_index']
            reason = error['reason']

            logs.append(pd.DataFrame({
                '_id': document_id,
                '_index': index,
                'status': 'failed',
                'error': reason,
                'date': current_date
            }, ignore_index=True))
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
shutil.rmtree(folder)
logs_df = pd.concat(logs, ignore_index=True)
logs_df.to_csv("./logs/csv-ingestion.csv", index=False)