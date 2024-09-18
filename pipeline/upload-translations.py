from opensearchpy import OpenSearch, helpers
import csv
import getpass

# File location constant
CSV_FILE_PATH = '../data/temp_translations_10S.csv'

# Opensearch client configuration
HOST = 'localhost'
PORT = 9200

username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")
auth = (username, password)

# Create the OpenSearch client with SSL/TLS enabled.
client = OpenSearch(
    hosts=[{'host': HOST, 'port': PORT}],
    http_compress=True,
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

# Generator to yield actions for bulk update
def csv_to_bulk_actions(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield {
                "_op_type": "update",  # Update operation
                "_index": "procure_v3_new",
                "_id": row['Document ID'],  # Document ID to update
                "doc": {
                    "Title (Translation)": row['Title (Translation)'],
                    "Description (Translation)": row['Description (Translation)']
                }
            }

# Perform the bulk update
actions = csv_to_bulk_actions(CSV_FILE_PATH)

# Execute bulk operation and handle errors
success, failed = helpers.bulk(
    client,
    actions,
    raise_on_error=False,  # Do not stop on error
    raise_on_exception=False  # Continue even if exceptions occur
)

print(f'Successful updates: {success}')
print(f'Failed updates: {len(failed)}')

# Log failed updates
for error in failed:
    if 'update' in error and error['update']['status'] == 404:
        print(f"Document not found: {error['update']['_id']} - skipping.")
    else:
        print(f"Failed to update document: {error}")
