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

def csv_to_bulk_actions(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        actions = []
        for row in reader:
            actions.append({
                "_op_type": "update",
                "_index": "procure_v3_new",
                "_id": row['Document ID'],
                "doc": {
                    "Title (Translation)": row['Title (Translation)'],
                    "Description (Translation)": row['Description (Translation)']
                }
            })
            # Process in batches of 10,000
            if len(actions) == 10000:
                yield actions
                actions = []
        # Yield any remaining actions
        if actions:
            yield actions

# Batch processing
batch_size = 10000
for batch in csv_to_bulk_actions(CSV_FILE_PATH):
    response = helpers.bulk(client, batch)
    print(f'Bulk update response: {response}')
