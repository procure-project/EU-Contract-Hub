#This module will group functions to extract csv from opensearch database.
from opensearchpy import OpenSearch, helpers
from opensearchpy.helpers import bulk
import pandas as pd
from tqdm import tqdm
import getpass
import csv


def get_client():
    HOST = 'localhost'
    PORT = 9200

    username = input("Enter ProCureSpot username: ")
    password = getpass.getpass(prompt="Enter ProCureSpot password: ")
    auth = (username, password)

    # Create the client with SSL/TLS enabled, but hostname verification disabled.
    return OpenSearch(
        hosts=[{'host': HOST, 'port': PORT}],
        http_compress=True,  # enables gzip compression for request bodies
        http_auth=auth,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=True,
        ssl_show_warn=False,
    )



def query_os(index, query, client = None):
    if not client:
        client = get_client()
    # Execute the initial search query to get the first batch of results
    response = client.search(
        index=index,
        body=query,
        size=5000,  # Number of documents to retrieve per batch
        scroll="1m"  # Keep the scroll window open for 1 minute
    )
    scroll_id = response["_scroll_id"]
    total_hits = response["hits"]["total"]["value"]  # Total number of hits

    all_records = []  # List to store all documents
    with tqdm(total=total_hits, desc="Fetching documents", unit="doc") as pbar:
        while True:
            # Extract document IDs and corresponding field values from the current batch of results
            id_field_pairs = []
            records = []
            for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
                doc_id = hit["_id"]
                doc = hit["_source"]
                record = {"Document ID": doc_id, **doc}
                all_records.append(record)

            # Update tqdm progress
            pbar.update(len(response["hits"]["hits"]))

            # Stop scrolling if fewer than 10,000 hits in the current batch
            if len(response["hits"]["hits"]) < 10000:
                break

            # Next Scroll
            response = client.scroll(scroll_id=scroll_id, scroll="1m")
            scroll_id = response["_scroll_id"]  # Update scroll ID for next batch
    return pd.DataFrame(all_records)

def translation_upload_bulk_actions(file_path, index):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield {
                "_op_type": "update",  # Update operation
                "_index": index,
                "_id": row['Document ID'],  # Document ID to update
                "doc": {
                    "Title (Translation)": row['Title (Translation)'],
                    "Description (Translation)": row['Description (Translation)']
                }
            }


def process_bulk_batches(actions, client, batch_size=10000):
    batch = []
    total_success_count = 0
    total_failed_count = 0

    # Wrap actions in tqdm without the need for len()
    pbar = tqdm(actions, desc="Uploading actions", unit="actions")

    for action in pbar:
        batch.append(action)
        if len(batch) == batch_size:
            success, failed = bulk(
                client,
                batch,
                raise_on_error=False,  # Do not stop on error
                raise_on_exception=False  # Continue even if exceptions occur
            )
            total_success_count += success
            total_failed_count += len(failed)
            batch = []  # Reset batch

            # Update progress bar after each batch
            pbar.set_postfix(success=total_success_count, failed=total_failed_count)

            # Log failed updates
            for error in failed:
                if 'update' in error and error['update']['status'] == 404:
                    print(f"Document not found: {error['update']['_id']} - skipping.")
                else:
                    print(f"Failed to update document: {error}")

    # Process any remaining actions in the batch
    if batch:
        success, failed = bulk(
            client,
            batch,
            raise_on_error=False,
            raise_on_exception=False
        )
        total_success_count += success
        total_failed_count += len(failed)

        # Update progress bar for remaining actions
        pbar.set_postfix(success=total_success_count, failed=total_failed_count)

        # Log failed updates
        for error in failed:
            if 'update' in error and error['update']['status'] == 404:
                print(f"Document not found: {error['update']['_id']} - skipping.")
            else:
                print(f"Failed to update document: {error}")

    # Print total success and failed counts
    print(f'Total successful updates: {total_success_count}')
    print(f'Total failed updates: {total_failed_count}')