from opensearchpy import OpenSearch, helpers
import csv
import getpass
from pipelinepackage.extractormodule import get_client, translation_upload_bulk_actions, process_bulk_batches

# File location constant
CSV_FILE_PATH = '/home/procure/data/temp_translations_10S.csv'

client = get_client()
actions = translation_upload_bulk_actions(CSV_FILE_PATH, "procure")
process_bulk_batches(actions, client, batch_size=10000)
