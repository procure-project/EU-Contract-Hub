
from opensearchpy import OpenSearch, helpers
import pandas as pd
from deep_translator import GoogleTranslator
# Initialize the OpenSearch client
host = 'localhost'
port = 9200
auth = ('admin', 'admin')  # For testing only. Don't store credentials in code.

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

# Define your index and field
index_name = "ted-xml"
# Define the query to retrieve all documents
query = {
    "query": {
        "match_all": {}  # Retrieve all documents
    }
}

# Execute the initial search query to get the first batch of results
response = client.search(
    index=index_name,
    body=query,
    size=10000,  # Number of documents to retrieve per batch
    scroll="1m"  # Keep the scroll window open for 1 minute
)

scroll_id = response["_scroll_id"]

scr = 1
while True:
    # Continue scrolling
    response = client.scroll(scroll_id=scroll_id, scroll="1m")
    id_field_pairs = []

    # Extract document IDs and corresponding field values from the current batch of results
    for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
        if not (isinstance(hit["_source"]["CONTRACT_AWARD_NOTICE"],
                           list)):  # REMOVE CONDITION there should not be any list in final version
            doc_id = hit["_id"]
            title_translated = hit["_source"]["Title (Translation)"]
            description_translated = hit["_source"]["Description (Translation)"]

            id_field_pairs.append((doc_id, title_translated,  description_translated))
    # Processing fields Scroll-level
    df = pd.DataFrame(id_field_pairs, columns=["Document ID","Title (Translation)","Description (Translation)"])


    print("Scroll " + str(scr))
    pd.display(df)