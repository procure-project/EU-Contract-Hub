import ssl
import requests
from opensearchpy import OpenSearch, helpers
import pandas as pd
import datetime
from math import ceil
from pipelinepackage import translatormodule as trans
from pipelinepackage.auth import get_opensearch_auth

requests.packages.urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

def log_pipeline_status(client, id):
    doc_id = f"translator-lot-{id}"
    doc = {
        "pipeline": "translator-lot",
        "doc_id": id,
        "timestamp": datetime.datetime.now()
    }
    client.index(index="pipeline_status", id=doc_id, body=doc)

def is_doc_processed(client, id):
    doc_id = f"translator-lot-{id}"
    return client.exists(index="pipeline_status", id=doc_id)

def filter_untranslated_ids(client, candidate_ids):
    doc_ids = [f"translator-lot-{doc_id}" for doc_id in candidate_ids]
    mget_body = {"ids": doc_ids}
    resp = client.mget(index="pipeline_status", body=mget_body)
    already_translated = {doc["_id"].replace("translator-lot-", "") for doc in resp["docs"] if doc["found"]}
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

# Get all doc IDs
all_ids = list(get_all_candidate_ids(client, index_name))
untranslated_ids = filter_untranslated_ids(client, all_ids)
if not untranslated_ids:
    print("No more untranslated lot/criteria documents to process.")
else:
    processed_docs = 0
    for i in range(ceil(len(untranslated_ids) / batch_size)):
        batch_ids = untranslated_ids[i*batch_size:(i+1)*batch_size]
        # Fetch only the Lots field
        mget_body = {
            "ids": batch_ids,
            "_source": ["Lots"]
        }
        docs = client.mget(index=index_name, body=mget_body)["docs"]
        # Prepare translation jobs
        translation_jobs = []  # Each job: (doc_id, lot_idx, field, text) or (doc_id, lot_idx, crit_idx, field, text)
        doc_lot_map = {}  # doc_id -> lots (for update)
        for doc in docs:
            if doc["found"]:
                doc_id = doc["_id"]
                lots = doc["_source"].get("Lots", [])
                doc_lot_map[doc_id] = lots
                for lot_idx, lot in enumerate(lots):
                    # Lot Title
                    title = lot.get("Title", "-")
                    title_trans = lot.get("Title (Translation)", "-")
                    if title and (not title_trans or title_trans == "-") and title != "-":
                        translation_jobs.append({
                            "doc_id": doc_id, "lot_idx": lot_idx, "field": "Title (Translation)", "text": title
                        })
                    # Lot Short Description
                    short_desc = lot.get("Short Description", "-")
                    short_desc_trans = lot.get("Short Description (Translation)", "-")
                    if short_desc and (not short_desc_trans or short_desc_trans == "-") and short_desc != "-":
                        translation_jobs.append({
                            "doc_id": doc_id, "lot_idx": lot_idx, "field": "Short Description (Translation)", "text": short_desc
                        })
                    # Criteria
                    for crit_idx, crit in enumerate(lot.get("Criteria", [])):
                        crit_type = crit.get("Type", "")
                        criterion = crit.get("Criterion", "-")
                        criterion_trans = crit.get("Criterion (Translation)", "-")
                        if crit_type in ["Quality", "Cost"] and criterion and (not criterion_trans or criterion_trans == "-") and criterion != "-":
                            translation_jobs.append({
                                "doc_id": doc_id, "lot_idx": lot_idx, "crit_idx": crit_idx, "field": "Criterion (Translation)", "text": criterion
                            })
        if not translation_jobs:
            continue
        # Prepare DataFrame for batch translation
        df_jobs = pd.DataFrame(translation_jobs)
        df_jobs["translated"] = trans.batch_translate(df_jobs[["text"]].rename(columns={"text": "Title"}))
        # Update docs in OpenSearch
        update_map = {}  # doc_id -> {lot_idx: {fields...}, ...}
        for idx, row in df_jobs.iterrows():
            doc_id = row["doc_id"]
            lot_idx = row["lot_idx"]
            field = row["field"]
            translated = row["translated"]
            if "crit_idx" in row and not pd.isnull(row["crit_idx"]):
                crit_idx = int(row["crit_idx"])
                # Update the nested criterion
                doc_lot_map[doc_id][lot_idx]["Criteria"][crit_idx][field] = translated
            else:
                # Update the lot field
                doc_lot_map[doc_id][lot_idx][field] = translated
        # Prepare bulk update actions
        actions = []
        for doc_id in batch_ids:
            if doc_id in doc_lot_map:
                actions.append({
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": doc_id,
                    "doc": {"Lots": doc_lot_map[doc_id]}
                })
        if actions:
            helpers.bulk(client, actions)
            processed_docs += len(actions)
            for doc_id in batch_ids:
                log_pipeline_status(client, doc_id)
    print(f"Processed and updated {processed_docs} documents.")
