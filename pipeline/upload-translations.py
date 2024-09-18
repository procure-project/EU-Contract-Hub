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


# Perform the bulk update in batches of 10,000
def process_bulk_batches(actions, batch_size=10000):
    batch = []
    total_success_count = 0
    total_failed_count = 0

    for action in actions:
        batch.append(action)
        if len(batch) == batch_size:
            success, failed = helpers.bulk(
                client,
                batch,
                raise_on_error=False,  # Do not stop on error
                raise_on_exception=False  # Continue even if exceptions occur
            )
            total_success_count += success
            total_failed_count += len(failed)
            batch = []  # Reset batch

            # Log failed updates
            for error in failed:
                if 'update' in error and error['update']['status'] == 404:
                    print(f"Document not found: {error['update']['_id']} - skipping.")
                else:
                    print(f"Failed to update document: {error}")

    # Process any remaining actions in the batch
    if batch:
        success, failed = helpers.bulk(
            client,
            batch,
            raise_on_error=False,
            raise_on_exception=False
        )
        total_success_count += success
        total_failed_count += len(failed)

        # Log failed updates
        for error in failed:
            if 'update' in error and error['update']['status'] == 404:
                print(f"Document not found: {error['update']['_id']} - skipping.")
            else:
                print(f"Failed to update document: {error}")

    # Print total success and failed counts
    print(f'Total successful updates: {total_success_count}')
    print(f'Total failed updates: {total_failed_count}')


# Call the function to process updates in batches
actions = csv_to_bulk_actions(CSV_FILE_PATH)
process_bulk_batches(actions, batch_size=10000)
