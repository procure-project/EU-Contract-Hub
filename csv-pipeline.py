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
from tqdm import tqdm
#                               ------------ CONSTANTS -----------------
FOLDER = "./temp/csv/"
HOST = 'localhost'
PORT = 9200
INDEX = 'ted-csv'
username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")
auth = (username, password)

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': HOST, 'port': PORT}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

#                          ------------ FUNCTIONS -----------------

# Function to download files from url into file_path
def download_file(url, file_path):
    try:
        wget.download(url, out=file_path)
        print(f"Downloaded: {url}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

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

#Converter functions for reading csv
def transform_id(id_csv): #Function to swap the ID_NOTICE_CAN field so it aligns with the one used in the xml format
    year_part = str(id_csv)[:4]
    id_xml = str(id_csv)[4:].zfill(6) + '-' + year_part
    return id_xml
def bool_converter(value):
    if pd.isnull(value) or value =='':
        return False
    elif value == 'Y' or value == '1':
        return True
    elif value == 'N' or value == '0':
        return False
    else:
        raise ValueError(f"Unexpected value '{value}' found in the column.")
def int_converter(value):
    if pd.isnull(value) or value =='':
        return -1
    else:
        return int(value)
def lots_converter(value):
    if pd.isnull(value) or value == '' or pd.isna(value):
        return 0
    elif value.isdigit():
        return int(value)
    else:
        raise ValueError(f"Unexpected value '{value}' found in the column.")
def date_converter(value):
    return datetime.strptime(value, '%d/%m/%y')

def read_csvs(folder_path): #Reads all yearly csv and concats them. Groups by CAN ID.
    columns_can_level = ['ID_NOTICE_CAN', 'TED_NOTICE_URL', 'YEAR', 'ID_TYPE', 'DT_DISPATCH', 'XSD_VERSION', 'CANCELLED', 'CORRECTIONS', 'B_MULTIPLE_CAE', 'CAE_NAME', 'CAE_NATIONALID', 'CAE_ADDRESS', 'CAE_TOWN', 'CAE_POSTAL_CODE', 'CAE_GPA_ANNEX', 'ISO_COUNTRY_CODE', 'ISO_COUNTRY_CODE_GPA', 'B_MULTIPLE_COUNTRY', 'ISO_COUNTRY_CODE_ALL', 'CAE_TYPE', 'EU_INST_CODE', 'MAIN_ACTIVITY', 'B_ON_BEHALF', 'B_INVOLVES_JOINT_PROCUREMENT', 'B_AWARDED_BY_CENTRAL_BODY', 'TYPE_OF_CONTRACT', 'B_FRA_AGREEMENT', 'FRA_ESTIMATED', 'B_DYN_PURCH_SYST', 'CPV', 'MAIN_CPV_CODE_GPA', 'B_GPA', 'GPA_COVERAGE', 'LOTS_NUMBER', 'VALUE_EURO', 'VALUE_EURO_FIN_1', 'VALUE_EURO_FIN_2', 'TOP_TYPE', 'B_ACCELERATED', 'OUT_OF_DIRECTIVES', 'B_ELECTRONIC_AUCTION', 'NUMBER_AWARDS']
    dtypes = {
        'ID_NOTICE_CAN': 'str',
        'TED_NOTICE_URL': 'str',
        'XSD_VERSION': 'str',
        'CANCELLED': 'bool',
        'CAE_NAME': 'str',
        'CAE_NATIONALID': 'str',
        'CAE_ADDRESS': 'str',
        'CAE_TOWN': 'str',
        'CAE_POSTAL_CODE': 'str',
        'CAE_GPA_ANNEX': 'str',
        'ISO_COUNTRY_CODE': 'str',
        'ISO_COUNTRY_CODE_GPA': 'str',
        'ISO_COUNTRY_CODE_ALL': 'str',
        'CAE_TYPE': 'str',
        'EU_INST_CODE': 'str',
        'MAIN_ACTIVITY': 'str',
        'TYPE_OF_CONTRACT': 'str',
        'FRA_ESTIMATED': 'str',
        'VALUE_EURO': 'float',
        'VALUE_EURO_FIN_1': 'float',
        'VALUE_EURO_FIN_2': 'float',
        'TOP_TYPE': 'str'
    }
    bool_cols = ['B_MULTIPLE_CAE', 'B_MULTIPLE_COUNTRY', "B_ON_BEHALF", "B_INVOLVES_JOINT_PROCUREMENT",
                 "B_AWARDED_BY_CENTRAL_BODY", "B_FRA_AGREEMENT", "B_DYN_PURCH_SYST", "B_GPA", "B_ACCELERATED",
                 "B_ELECTRONIC_AUCTION", "OUT_OF_DIRECTIVES"]
    int_cols = ['GPA_COVERAGE', 'CPV', 'CORRECTIONS', 'ID_TYPE', 'YEAR', 'MAIN_CPV_CODE_GPA']
    converters = {col: bool_converter for col in bool_cols}
    converters.update({col: int_converter for col in int_cols})
    converters['LOTS_NUMBER'] = lots_converter
    converters['DT_DISPATCH'] = date_converter
    dfs = []

    csv_files = [csv_file for csv_file in os.listdir(folder_path) if csv_file.startswith("export_CAN")]
    with tqdm(total=len(csv_files), desc=f"Loading CSVs", colour='white', unit='year',
              bar_format="{desc}: |{bar}| {n}/{total}") as pbar:
        for csv_file in csv_files:
            file_path = os.path.join(folder_path, csv_file)
            current_df = pd.read_csv(file_path, usecols=columns_can_level, dtype=dtypes, converters=converters)
            dfs.append(current_df)
            pbar.update(1)

    df = pd.concat(dfs, ignore_index=True)

    df_flat = df.groupby('ID_NOTICE_CAN').first()
    df_flat.index = df_flat.index.to_series().apply(transform_id)
    df_flat.fillna({'VALUE_EURO': -1.,
                   'VALUE_EURO_FIN_1': -1.,
                   'VALUE_EURO_FIN_2': -1., }, inplace=True) #JSON Parser does not accept na. We set them at -1.
    return df_flat

#                               ------------ CODE -----------------

download_csv(FOLDER)
if not os.path.exists(FOLDER):
    os.makedirs(FOLDER)
df = read_csvs(FOLDER)
lines = df.shape[0]
columns = df.columns


logs = []
print("Lines to upload " + str(lines))
iters = math.ceil(lines / 100000)
for i in tqdm(range(iters), desc="Indexing", unit='00000 lines'):

    start_line = i * 100000
    end_line = min(((i + 1) * 100000 - 1), lines)

    print('Uploading lines ' + str(start_line) + ' to ' + str(end_line))
    df_chunk = df.iloc[start_line:end_line + 1]
    # Prepare a list of actions for the bulk API
    actions = [
        {
            "_op_type": "index",
            "_index": INDEX,
            "_id": id_doc,
            **{f"{col_name}": doc[col_name] for col_name in columns}
        }
        for id_doc, doc in df_chunk.iterrows()
    ]
    # Use the bulk API to index the documents
    try:
        success, failed = helpers.bulk(client, actions, index=INDEX, raise_on_error=True, refresh=True)
        #Logging
        successful_ids = {action['_id'] for action in actions}
        failed_ids = {failure['index']['_id'] for failure in failed}
        successful_ids -= failed_ids
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for action in actions:
            if action['_id'] in successful_ids:
                log_entry = pd.DataFrame({
                    '_id': action['_id'],
                    '_index': action['_index'],
                    'status': 'success',
                    'error': None,
                    'date': current_date
                })
                logs.append(log_entry)

        for failure in failed:
            action = failure['index'] if 'index' in failure else failure['create']
            error = failure['index']['error'] if 'index' in failure else failure['create']['error']
            document_id = action['_id']
            index = action['_index']
            reason = error['reason']

            log_entry = pd.DataFrame({
                '_id': document_id,
                '_index': index,
                'status': 'failed',
                'error': reason,
                'date': current_date
            })
            logs.append(log_entry)
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
shutil.rmtree(FOLDER)
logs_df = pd.concat(logs, ignore_index=True)
logs_df.to_csv("./logs/csv-ingestion.csv", index=False)