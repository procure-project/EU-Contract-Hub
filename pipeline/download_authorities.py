
from opensearchpy import OpenSearch, helpers
import polars as pl
import getpass

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
    response = client.scroll(scroll_id=scroll_id, scroll="1m")
    id_field_pairs = []

    for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
            doc_id = hit["_id"]
            #CA = hit["_source"]["Contracting Authority"]
            Lots = hit["_source"]["Lots"]
            id_field_pairs.append((doc_id, Lots))

    #df = pl.DataFrame(id_field_pairs, columns=["Document ID","Contracting Authorities"]) # Processing fields Scroll-level
    df = pl.DataFrame(id_field_pairs, columns=["Document ID", "Lots"])
    dfs.append(df)
    if len(response["hits"]["hits"]) < 1000:
        break
final_df = pl.concat(dfs, ignore_index = True)
final_df.to_csv("lots_extraction.csv", index=False)
