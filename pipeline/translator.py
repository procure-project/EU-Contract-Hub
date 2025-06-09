import ssl
from pipelinepackage import translatormodule as trans
import requests
from opensearchpy import OpenSearch, helpers
import pandas as pd
import getpass
requests.packages.urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

from pipelinepackage.auth import get_opensearch_auth

host = 'localhost'
port = 9200
auth = get_opensearch_auth()

client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
    timeout=60
)

index_name = "procure"
scroll_size = 100
query = {
  "query": {
          "term": {
            "Title (Translation).keyword": "-"
          }
  },
  "size": scroll_size
}
while True:
    response = client.search(index=index_name, body=query)
    hits = response["hits"]["hits"]
    if not hits:
        print("No more documents to process.")
        break
    id_field_pairs = []
    for hit in hits:  # Processing and Extracting Info Document-wise
        doc_id = hit["_id"]
        title = hit["_source"]["Title"]
        description = hit["_source"]["Description"]
        title_translated = "-"
        description_translated = "-"
        id_field_pairs.append((doc_id, title, title_translated, description, description_translated))
    df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description", "Description (Translation)"])
    try:
        df_translated  = trans.batch_translate(df)
        actions = [
            {
                "_op_type": "update",
                "_index": index_name,
                "_id": row['Document ID'],
                "doc": {
                    "Title (Translation)": row['Title (Translation)'],
                    "Description (Translation)": row['Description (Translation)']
                }
            }
            for _, row in df_translated.iterrows()
        ]
        helpers.bulk(client, actions)
        print(f"Processed and updated {len(df_translated)} documents.")
    except Exception as e:
        print(e)
# Create a DataFrame to store the document IDs and field values