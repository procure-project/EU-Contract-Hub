
from opensearchpy import OpenSearch, helpers
import pandas as pd
import getpass
from deep_translator import GoogleTranslator
# Initialize the OpenSearch client

# Opensearch client
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

# Define your index and field
index_name = "procure"
# Define the query to retrieve all documents
query = {
    "query": {
        "term": {
            "Healthcare CPV": True
        }
    }
}

# Execute the initial search query to get the first batch of results
response = client.search(
    index=index_name,
    body=query,
    size=1000,  # Number of documents to retrieve per batch
    scroll="1m"  # Keep the scroll window open for 1 minute
)

scroll_id = response["_scroll_id"]
dfs=[]
scr = 1
while True:
    # Continue scrolling
    response = client.scroll(scroll_id=scroll_id, scroll="1m")
    id_field_pairs = []

    # Extract document IDs and corresponding field values from the current batch of results
    for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
            doc_id = hit["_id"]
            CA = hit["_source"]["Contracting Authority"]
            id_field_pairs.append((doc_id, CA))
    # Processing fields Scroll-level
    df = pd.DataFrame(id_field_pairs, columns=["Document ID","Contracting Authorities"])


    print("Scroll " + str(scr))
    scr = scr + 1
    dfs.append(df)
    if len(response["hits"]["hits"]) < 10000:
        break
final_df = pd.concat(dfs, ignore_index = True)
final_df.to_csv("ca_extraction.csv", index=False)
