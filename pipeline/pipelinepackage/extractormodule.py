#This module will group functions to extract csv from opensearch database.
from opensearchpy import OpenSearch, helpers
import pandas as pd
from tqdm import tqdm
import json
import getpass

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



def query_os(index, query):
    client = get_client()
    # Execute the initial search query to get the first batch of results
    response = client.search(
        index=index,
        body=query,
        size=10000,  # Number of documents to retrieve per batch
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
    data = pd.DataFrame(all_records)
    data.to_csv("os_extraction.csv", index=False)
    print("Results saved on os_extraction.csv for query:")
    print(json.dumps(query, indent=4))