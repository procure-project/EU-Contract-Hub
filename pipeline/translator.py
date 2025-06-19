import ssl
from pipelinepackage import translatormodule as trans
import requests
from opensearchpy import OpenSearch, helpers
import pandas as pd
import getpass
requests.packages.urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context
import datetime
from pipelinepackage.auth import get_opensearch_auth
from math import ceil


def log_pipeline_status(client, id):
    doc_id = f"translator-contract-{id}"
    doc = {
        "pipeline": "translator-contract",
        "doc_id": id,
        "timestamp": datetime.datetime.now()
    }
    client.index(index="pipeline_status", id=doc_id, body=doc)


def is_doc_processed(client, id):
    doc_id = f"translator-contract-{id}"
    return client.exists(index="pipeline_status", id=doc_id)


def filter_untranslated_ids(client, candidate_ids):
    doc_ids = [f"translator-contract-{doc_id}" for doc_id in candidate_ids]
    mget_body = {"ids": doc_ids}
    resp = client.mget(index="pipeline_status", body=mget_body)
    already_translated = {doc["_id"].replace("translator-contract-", "") for doc in resp["docs"] if doc["found"]}
    return [doc_id for doc_id in candidate_ids if doc_id not in already_translated]


def get_all_candidate_ids(client, index_name):
    query = {
        "query": {"match_all": {}},
        "_source": False
    }
    response = client.search(index=index_name, body=query, scroll="5m")
    scroll_id = response["_scroll_id"]
    hits = response["hits"]["hits"]
    while hits:
        for hit in hits:
            yield hit["_id"]
        response = client.scroll(scroll_id=scroll_id, scroll="5m")
        hits = response["hits"]["hits"]
        if not hits:
            break



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
batch_size = 100

all_ids = list(get_all_candidate_ids(client, index_name))
untranslated_ids = filter_untranslated_ids(client, all_ids)
if not untranslated_ids:
    print("No more untranslated documents to process.")
else:
    for i in range(ceil(len(untranslated_ids) / batch_size)):
        batch_ids = untranslated_ids[i*batch_size:(i+1)*batch_size]
        # Fetch the docs for these IDs
        mget_body = {
            "ids": batch_ids,
            "_source": ["Title", "Description"]
        }
        docs = client.mget(index=index_name, body=mget_body)["docs"]
        id_field_pairs = []
        for doc in docs:
            if doc["found"]:
                doc_id = doc["_id"]
                source = doc["_source"]
                title = source.get("Title", "-")
                description = source.get("Description", "-")
                # Only process if either field is "-"
                if title != "-" or description != "-":
                    id_field_pairs.append((doc_id, title, "-", description, "-"))
        if not id_field_pairs:
            continue
        df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description", "Description (Translation)"])
        try:
            df_translated  = trans.batch_translate(df)
            actions = []
            for _, row in df_translated.iterrows():
                title_trans = row['Title (Translation)']
                desc_trans = row['Description (Translation)']
                # If translation is identical to original, treat as not translated
                if title_trans == row['Title']:
                    title_trans = "-"
                if desc_trans == row['Description']:
                    desc_trans = "-"
                actions.append({
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": row['Document ID'],
                    "doc": {
                        "Title (Translation)": title_trans,
                        "Description (Translation)": desc_trans
                    }
                })
            helpers.bulk(client, actions)
            print(f"Processed and updated {len(df_translated)} documents.")
            for doc_id in df_translated["Document ID"]:
                log_pipeline_status(client, doc_id)
        except Exception as e:
            print(e)