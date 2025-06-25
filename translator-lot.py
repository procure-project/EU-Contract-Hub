import ssl
from pipelinepackage import translatormodule as trans
import requests
from opensearchpy import OpenSearch, helpers
import pandas as pd
import getpass
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('translator_lot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context
import datetime
from pipelinepackage.auth import get_opensearch_auth
from math import ceil


def log_pipeline_status(client, id):
    doc_id = f"translator-lot-{id}"
    doc = {
        "pipeline": "translator-lot",
        "doc_id": id,
        "timestamp": datetime.datetime.now()
    }
    try:
        client.index(index="pipeline_status", id=doc_id, body=doc)
        logger.info(f"Logged pipeline status for document {id}")
    except Exception as e:
        logger.error(f"Failed to log pipeline status for document {id}: {e}")


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


def main():
    # Configuration from environment variables
    host = os.getenv('OPENSEARCH_HOST', 'localhost')
    port = int(os.getenv('OPENSEARCH_PORT', 9200))
    index_name = os.getenv('OPENSEARCH_INDEX', 'procure')
    batch_size = int(os.getenv('TRANSLATION_BATCH_SIZE', 100))

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

    all_ids = list(get_all_candidate_ids(client, index_name))
    untranslated_ids = filter_untranslated_ids(client, all_ids)
    
    if not untranslated_ids:
        logger.info("No more untranslated documents to process.")
        return

    logger.info(f"Found {len(untranslated_ids)} untranslated documents.")

    for i in range(ceil(len(untranslated_ids) / batch_size)):
        batch_ids = untranslated_ids[i*batch_size:(i+1)*batch_size]
        
        # Fetch the docs for these IDs
        mget_body = {
            "ids": batch_ids,
            "_source": ["Lots"]
        }
        docs = client.mget(index=index_name, body=mget_body)["docs"]
        
        translation_batches = []
        for doc in docs:
            if doc["found"]:
                doc_id = doc["_id"]
                lots = doc["_source"].get("Lots", [])
                
                for lot_idx, lot in enumerate(lots):
                    # Lot Title
                    title = lot.get("Title", "-")
                    title_trans = lot.get("Title (Translation)", "-")
                    if title and (title_trans == "-" or not title_trans):
                        translation_batches.append({
                            "doc_id": doc_id,
                            "lot_idx": lot_idx,
                            "field": "Title (Translation)",
                            "original_text": title
                        })
                    
                    # Lot Short Description
                    short_desc = lot.get("Short Description", "-")
                    short_desc_trans = lot.get("Short Description (Translation)", "-")
                    if short_desc and (short_desc_trans == "-" or not short_desc_trans):
                        translation_batches.append({
                            "doc_id": doc_id,
                            "lot_idx": lot_idx,
                            "field": "Short Description (Translation)",
                            "original_text": short_desc
                        })
                    
                    # Criteria
                    for crit_idx, crit in enumerate(lot.get("Criteria", [])):
                        crit_type = crit.get("Type", "")
                        criterion = crit.get("Criterion", "-")
                        criterion_trans = crit.get("Criterion (Translation)", "-")
                        
                        if crit_type in ["Quality", "Cost"] and criterion and (criterion_trans == "-" or not criterion_trans):
                            translation_batches.append({
                                "doc_id": doc_id,
                                "lot_idx": lot_idx,
                                "crit_idx": crit_idx,
                                "field": f"{crit_type}.Criterion (Translation)",
                                "original_text": criterion
                            })
        
        if not translation_batches:
            logger.info("No translation batches found in this iteration.")
            continue
        
        # Prepare DataFrame for translation
        df = pd.DataFrame(translation_batches)
        
        try:
            df_translated = trans.batch_translate(df[["original_text"]])
            df_translated.columns = ["Translated Text"]
            df = pd.concat([df, df_translated], axis=1)
        except Exception as e:
            logger.error(f"Translation failed: {e}")
            continue
        
        # Prepare bulk update actions
        actions = []
        for _, row in df.iterrows():
            doc_id = row["doc_id"]
            lot_idx = row["lot_idx"]
            field = row["field"]
            translated_text = row["Translated Text"]
            
            # Skip if translation is identical to original
            if translated_text == row["original_text"]:
                translated_text = "-"
            
            action = {
                "_op_type": "update",
                "_index": index_name,
                "_id": doc_id,
                "script": {
                    "source": f"ctx._source.Lots[{lot_idx}][params.field] = params.translated_text",
                    "params": {
                        "field": field,
                        "translated_text": translated_text
                    }
                }
            }
            
            # Add criterion translation if applicable
            if "crit_idx" in row:
                action["script"]["source"] += f"\nctx._source.Lots[{lot_idx}].Criteria[{row['crit_idx']}][params.field] = params.translated_text"
            
            actions.append(action)
        
        # Perform bulk update
        try:
            success, failed = helpers.bulk(client, actions)
            logger.info(f"Processed and updated {success} translations. Failed updates: {failed}")
            
            # Log processed document IDs
            processed_doc_ids = set(df["doc_id"])
            for doc_id in processed_doc_ids:
                log_pipeline_status(client, doc_id)
        
        except Exception as e:
            logger.error(f"Error during translation update: {e}")


if __name__ == "__main__":
    main() 