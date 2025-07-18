from opensearchpy import OpenSearch, helpers
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
import traceback
from datetime import datetime as dt
from pipelinepackage.auth import get_opensearch_auth
import urllib.request
from time import sleep
from opensearchpy import RequestsHttpConnection
from urllib3.util import Retry
#                               ------------ CONSTANTS -----------------
BASE_URL = 'https://ted.europa.eu/packages/daily/'
BASE_FOLDER = "./temp/xml/"
LOGS_PATH ="./logs/xml-ingestion.csv"
START_YEAR = 2019
END_YEAR = datetime.date.today().year
# Opensearch client
HOST = 'localhost'
PORT = 9200
auth = get_opensearch_auth()
INDEX_XML = 'ted-xml'
INDEX_EFORMS = 'ted-eforms'
from time import sleep

OS_CLIENT = OpenSearch(
    hosts=[{'host': HOST, 'port': PORT}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
    retry_on_timeout=True,
    max_retries=5,
    timeout=60,
    connection_class=RequestsHttpConnection
)

#                          ------------ FUNCTIONS -----------------

def url_exists(url):
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req) as response:
            return response.status == 200
    except urllib.error.HTTPError as e:
        return False
    except Exception as e:
        print(f"Error while checking URL: {e}")
        return False

def log_pipeline_status(client, year, ojs=None):
    timestamp = datetime.datetime.now()
    doc_id = f"xml-ingestion-{year}" if ojs is None else f"xml-ingestion-{year}-{ojs}"

    doc = {
        "pipeline": "xml-ingestion",
        "year": year,
        "timestamp": timestamp
    }

    if ojs is not None:
        doc["ojs"] = ojs
    client.index(index="pipeline_status", id=doc_id, body=doc)

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
    if isinstance(dictionary, dict):
        for text_field, text_dictionary in dictionary.items():
            if isinstance(text_dictionary, dict):
                for p_field, p_value in text_dictionary.items():
                    if p_field == 'P':
                        # print("before: "+json.dumps(dictionary[text_field]))
                        # if p_value is not None:
                        #     print('---------------------------------------------------------------------------------------------------------------------------------------------------')
                        #     print(text_field +": "+fetch_p_text(p_value))
                        dictionary[text_field] = fetch_p_text(p_value)
                        # print("after: "+json.dumps(dictionary[text_field]))
                    else:
                        modify_p_fields(text_dictionary)
            elif isinstance(text_dictionary, list):
                modify_p_fields(text_dictionary)
    elif isinstance(dictionary, list):
        for e in range(len(dictionary)):
            if isinstance(dictionary[e], dict):
                modify_p_fields(dictionary[e])


def fetch_p_text(p_dictionary):
    if p_dictionary is None:
        return ""
    elif isinstance(p_dictionary, str):
        return p_dictionary
    elif isinstance(p_dictionary, list):
        p_text = ""
        for item in p_dictionary:
            p_text = p_text + "\n" + fetch_p_text(item)
        return p_text[1:]
    elif isinstance(p_dictionary, dict):
        if "#text" in p_dictionary:
            return p_dictionary["#text"]
        else:
            p_text = ""
            for inner_field, inner_dictionary in p_dictionary.items():
                p_text = p_text + "; " + fetch_p_text(inner_dictionary)
            return p_text[2:]


def modify_txt_fields(dictionary):
    if isinstance(dictionary, dict):
        for k, v in dictionary.items():
            if isinstance(v, dict):
                if '#text' in v.keys():
                    #print("before: " + json.dumps(dictionary))
                    # if p_value is not None:
                    #     print('---------------------------------------------------------------------------------------------------------------------------------------------------')
                    dictionary[k] = dictionary[k]['#text']
                    #print("after: " + json.dumps(dictionary))
                else:
                    modify_txt_fields(v)
            elif isinstance(v, list):
                for e in range(len(v)):
                    if isinstance(v[e], dict):
                        if '#text' in v[e].keys():
                            dictionary[k] = v[e]['#text']
                        else:
                            modify_txt_fields(v[e])
    elif isinstance(dictionary, list):
        for e in range(len(dictionary)):
            modify_txt_fields(dictionary[e])


# Preformats the xml, selecting only Contract Award Notices and preformatting P text fields so opensearch may index them as they had a variable structure.
def format_dict(notice):
    is_eforms = True
    if "TED_EXPORT" in notice:
        notice = notice["TED_EXPORT"]
    if "CODED_DATA_SECTION" in notice:
        is_eforms = False
        try:
            doc_ojs = notice["CODED_DATA_SECTION"]["NOTICE_DATA"]["NO_DOC_OJS"]
            notice_id = doc_ojs.split("-")[-1].zfill(8) + "-" + doc_ojs[:4]
            notice_clean = {
                "CODED_DATA_SECTION": notice["CODED_DATA_SECTION"],
                "CONTRACT_AWARD_NOTICE": notice["FORM_SECTION"]["F03_2014"]
            }
            modify_p_fields(notice_clean)
        except KeyError as e:
            raise

    else:  # "ContractAwardNotice" in notice:
        try:
            notice_clean = notice['ContractAwardNotice']
            notice_id = \
            notice_clean['ext:UBLExtensions']['ext:UBLExtension']['ext:ExtensionContent']['efext:EformsExtension'][
                'efac:Publication']['efbc:NoticePublicationID']['#text']
            modify_txt_fields(notice_clean)
            notice_clean = {k: v for k, v in notice_clean.items() if not k.startswith('@')} #Clean namespace tags
        except KeyError as e:
            raise
    return is_eforms, notice_id, notice_clean


# Single-doc helper kept but refresh disabled (unused in bulk path)
def index_doc_opensearch(doc_id, doc, index):
    try:
        OS_CLIENT.index(
            index=index,
            body=doc,
            id=doc_id,
            refresh=False
        )
    except Exception as e:
        print(f"Error during indexing: {e}")

def generate_log(package,doc_id,index,status,error):
    return pd.DataFrame([{'package': package,
                        '_id': doc_id,
                        '_index': index,
                        'status': status,
                        'error': error,
                        'date': dt.now().strftime('%Y-%m-%d %H:%M:%S')
                    }])

def ted_xml_upload(package, package_path):
    logs = []
    BULK_CHUNK = 250  # contracts per bulk request
    actions = []  # accumulate bulk actions
    for root, _, files in os.walk(package_path):
        xml_files = [xml_file for xml_file in files if xml_file.endswith(".xml")]
        if xml_files:
            with tqdm(total=len(xml_files), desc=f"Processing {package}", colour='white', unit='file',
                      bar_format="{desc}: |{bar}| {n}/{total}") as pbar:

                for xml_file in xml_files:
                    xml_path = os.path.join(root, xml_file)
                    index = INDEX_XML
                    with open(xml_path, 'r', encoding='utf-8') as file:  # Reads and parses XML files to json
                        xml_data = file.read()
                        xml_dict = xmltodict.parse(xml_data)
                        try:
                            # print(xml_dict)
                            is_eforms, doc_id, xml_processed = format_dict(xml_dict)
                            if is_eforms:
                                index = INDEX_EFORMS
                            index_doc_opensearch(doc_id, xml_processed, index)
                            actions.append({
                                "_op_type": "index",
                                "_index": index,
                                "_id": doc_id,
                                "_source": xml_processed
                            })
                            # When chunk full, send bulk request
                            if len(actions) >= BULK_CHUNK:
                                for attempt in range(5):
                                    try:
                                        helpers.bulk(OS_CLIENT, actions, refresh=False, chunk_size=BULK_CHUNK)
                                        actions.clear()
                                        sleep(1)
                                        break
                                    except Exception as bulk_err:
                                        print(f"Error during bulk indexing (attempt {attempt+1}): {bulk_err}")
                                        sleep(2 * (2 ** attempt))  # exponential backoff
                            logs.append(generate_log(package, doc_id, index, 'success', None))
                            pbar.update(1)
                        except KeyError as keyerror:
                            if keyerror.args[0] not in ('F03_2014', 'ContractAwardNotice'):
                                print(f"exception{keyerror}")
                                logs.append(generate_log(package, xml_path, index, 'failed',
                                                         'Key Error:' + str(keyerror)))
                                break
                            else:
                                pbar.update(1)
                                logs.append(generate_log(package, xml_path, index, 'discarded', 'Not CAN'))
                                pass
                        except Exception as e:
                            logs.append(generate_log(package, xml_path, None, 'failed', 'Error:' + str(e)))
                            print(e)
                            traceback.print_exc()
                            print(json.dumps(xml_dict, indent=4))
                            break
    # flush remaining actions after package processed
    if actions:
        for attempt in range(5):
            try:
                helpers.bulk(OS_CLIENT, actions, refresh=False, chunk_size=BULK_CHUNK)
                sleep(1)
                break
            except Exception as bulk_err:
                print(f"Error during final bulk indexing (attempt {attempt+1}): {bulk_err}")
                sleep(2 * (2 ** attempt))  # exponential backoff
    logs_df = pd.concat(logs, ignore_index=True)
    file_exists = os.path.exists(LOGS_PATH)
    logs_df.to_csv(LOGS_PATH, mode='a', header=not file_exists, index=False)



def ted_xml_ingestion(year):
    doc_id_year = f"xml-ingestion-{year}"
    if OS_CLIENT.exists(index="pipeline_status", id=doc_id_year):
        print(f"Year {year} already marked as completed. Skipping.")
        return

    ojs = 1

    while True:
        doc_id_ojs = f"xml-ingestion-{year}-{ojs}"
        if OS_CLIENT.exists(index="pipeline_status", id=doc_id_ojs):
            print(f"OJS {year}-{ojs} already ingested. Skipping.")
            ojs += 1
            continue
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
            log_pipeline_status(OS_CLIENT, year, ojs)
        # else:
        # print (f"OJS: {year}{str(ojs).zfill(5) already in OpenSearch")
        except HTTPError as e:
            if e.code == 404:
                print(f"HTTP Error 404: {package} not Found - . Exiting loop.")
                # Check if next year's OJS 1 package exists online
                next_year = year + 1
                next_url = f"{BASE_URL}{next_year}00001"
                if url_exists(next_url):
                    print(f"\nNext year ({next_year}) OJS 1 exists. Marking {year} as complete.")
                    log_pipeline_status(OS_CLIENT, year)
                else:
                    print(f"Next year ({next_year}) OJS 1 not yet published. Not marking {year} as complete.")
                break
            else:
                print(f"Failed to download {download_url}: HTTP Error {e.code}")
        except Exception as e:
            print(f"Failed to process {package}: {e}")
        ojs += 1

#                               ------------ CODE -----------------

if not os.path.exists(BASE_FOLDER):
    os.makedirs(BASE_FOLDER)
for year in range(START_YEAR, END_YEAR + 1):
    year_folder = f"{BASE_FOLDER}{year}/"  # Temp yearly packages folder
    if not os.path.exists(year_folder):
        os.makedirs(year_folder)
    ted_xml_ingestion(year)
    shutil.rmtree(year_folder)
shutil.rmtree(BASE_FOLDER)