from opensearchpy import OpenSearch
import pandas as pd
import shutil
import xmltodict
import json
import os
import wget
import tarfile
from urllib.error import HTTPError
import datetime
from tqdm import tqdm
import getpass

#                               ------------ CONSTANTS -----------------
BASE_URL = 'https://ted.europa.eu/packages/daily/'
BASE_FOLDER = "/home/procure/data/ted/xml/"
START_YEAR = 2023
END_YEAR = datetime.date.today().year
# Opensearch client
HOST = 'localhost'
PORT = 9200
username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")
auth = (username, password)
INDEX = 'ted-xml'
# Create the client with SSL/TLS enabled, but hostname verification disabled.
OS_CLIENT = OpenSearch(
    hosts=[{'host': HOST, 'port': PORT}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

#                          ------------ FUNCTIONS -----------------
# Extracts a .tar.gz compressed file and deletes the compressed file
def extract_file(file_path):
    try:
        with tarfile.open(file_path, 'r:gz') as tar:
            tar.extractall(path=file_path[:-7])

        # Remove the downloaded archive file
        os.remove(file_path)
        print(f"Extracted: {file_path}")

    except Exception as e:
        print(f"Failed to extract {file_path}: {e}")


def modify_p_fields(dictionary):
    for key, value in dictionary.items():
        if key == 'P':

            # print("before: "+json.dumps(dictionary))
            if isinstance(value, str):
                dictionary["P"] = value
            elif isinstance(value, list):
                flattened = []

                def flatten_recursive(sublist):
                    for item in sublist:
                        if isinstance(item, list):
                            flatten_recursive(item)
                        elif isinstance(item, str) and item.strip() != "":
                            flattened.append(item.strip())

                flatten_recursive(value)
                dictionary["P"] = '\n'.join(flattened)

            elif isinstance(value, dict):
                if "#text" in dictionary["P"]:
                    dictionary["P"] = dictionary["P"]["#text"]
                else:
                    dictionary["P"] = dictionary["P"]["FT"]["#text"]
                    # print("after: "+json.dumps(dictionary))
        elif key == "EU_PROGR_RELATED":
            if isinstance(value,dict):
                dictionary[key] = value["P"]
        elif isinstance(value, dict):
            modify_p_fields(value)
        elif isinstance(value, list):
            for e in range(len(value)):
                if isinstance(value[e], dict):
                    modify_p_fields(value[e])


# Preformats the xml, selecting only Contract Award Notices and preformatting P text fields so opensearch may index them as they had a variable structure.
def format_dict(xml_dict):
    id = xml_dict["TED_EXPORT"]["@DOC_ID"]
    try:
        xml_out = {
            "CODED_DATA_SECTION": xml_dict["TED_EXPORT"]["CODED_DATA_SECTION"],
            "CONTRACT_AWARD_NOTICE": xml_dict["TED_EXPORT"]["FORM_SECTION"]["F03_2014"]
        }
        modify_p_fields(xml_out)
    except KeyError as e:
        raise

    return id, xml_out


# Indexes a document with given id to an opensearch client
def index_doc_opensearch(doc_id, doc):
    try:
        response = OS_CLIENT.index(
            index=INDEX,
            body=doc,
            id=doc_id,
            refresh=True
        )
    except Exception as e:
        print(f"Error during indexing: {e}")


def ted_xml_upload(package, package_path):
    logs = []
    for root, _, files in os.walk(package_path):
        xml_files = [xml_file for xml_file in files if xml_file.endswith(".xml")]
        if xml_files:
            with tqdm(total=len(xml_files), desc=f"Processing {package}", colour='white', unit='file',
                      bar_format="{desc}: |{bar}| {n}/{total}") as pbar:

                for xml_file in xml_files:
                    xml_path = os.path.join(root, xml_file)
                    with open(xml_path, 'r', encoding='utf-8') as file:  # Reads and parses XML files to json
                        xml_data = file.read()
                        xml_dict = xmltodict.parse(xml_data)
                        try:
                            doc_id, xml_processed = format_dict(xml_dict)
                            index_doc_opensearch(doc_id, xml_processed)
                            log_entry = pd.DataFrame([{
                                'package': package,
                                '_id': doc_id,
                                '_index': INDEX,
                                'status': 'success',
                                'error': None,
                                'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }])
                            logs.append(log_entry)
                            pbar.update(1)
                        except KeyError as keyerror:
                            if keyerror.args[0] != 'F03_2014':
                                print(f"exception{keyerror}")
                                log_entry = pd.DataFrame([{
                                    'package': package,
                                    '_id': doc_id,
                                    '_index': INDEX,
                                    'status': 'failed',
                                    'error': 'Key Error:' + keyerror,
                                    'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }])
                                logs.append(log_entry)
                                break
                            else:
                                log_entry = pd.DataFrame([{
                                'package': package,
                                '_id': doc_id,
                                '_index': INDEX,
                                'status': 'discarded',
                                'error': 'Not CAN',
                                'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }])
                                logs.append(log_entry)
                                pbar.update(1)
                                pass
                        except Exception as e:
                            log_entry = pd.DataFrame([{
                                'package': package,
                                '_id': doc_id,
                                '_index': INDEX,
                                'status': 'failed',
                                'error': 'Error:' + e,
                                'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }])
                            logs.append(log_entry)
                            print(e)
                            print(json.dumps(xml_processed, indent=4))
                            break
    logs_df = pd.concat(logs, ignore_index=True)
    logs_df.to_csv("./logs/xml-ingestion.csv", index=False)


def ted_xml_ingestion(year):
    ojs = 1
    # os_ojs = Get OJS in OS for given year
    while True:
        download_url = f"{BASE_URL}{year}{str(ojs).zfill(5)}"
        save_path = f"{BASE_FOLDER}{year}/{str(ojs)}.tar.gz"
        package = f'{year}-{ojs}'
        try:
            # If ojs not on os_ojs:
            print(f"Downloading {download_url}")
            wget.download(download_url, out=save_path)
            print("")
            extract_file(save_path)
            ted_xml_upload(package, save_path[:-7])
            shutil.rmtree(save_path[:-7])  # Removes package folder after upload
        # else:
        # print (f"OJS: {year}{str(ojs).zfill(5) already in OpenSearch")
        except HTTPError as e:
            if e.code == 404:
                print(f"HTTP Error 404: {package} not Found - . Exiting loop.")
                break  # Exit the loop if HTTP Error 404 is encountered
            else:
                print(f"Failed to download {download_url}: HTTP Error {e.code}")
        except Exception as e:
            print(f"Failed to process {package}: {e}")
        ojs += 1


#                               ------------ CODE -----------------

for year in range(START_YEAR, END_YEAR + 1):
    year_folder = f"{BASE_FOLDER}{year}/"  # Temp yearly packages folder
    if not os.path.exists(year_folder):
        os.makedirs(year_folder)
    ted_xml_ingestion(year)
    shutil.rmtree(year_folder)