from pipelinepackage.extractormodule import query_os, get_client, translation_upload_bulk_actions, process_bulk_batches
import datetime

def log_pipeline_status(client, id):
    doc_id = f"translator-contract-{id}"
    doc = {
        "pipeline": "translator-contract",
        "doc_id": id,
        "timestamp": datetime.datetime.now()
    }
    client.index(index="pipeline_status", id=doc_id, body=doc)

client = get_client()
translation_query = """{
        "_source": ["Title (Translation)", "Description (Translation)"],
            "query": {
                "match_all": {}
            }
        }"""
index1 = "procure"
index2 = input("Select output index:")

data = query_os(index1, translation_query, client)
data.to_csv("translations_extraction.csv", index=False)
print(data[:10])
actions = translation_upload_bulk_actions("translations_extraction.csv", index2)

# Process and log each batch
for batch in process_bulk_batches(actions, client, batch_size=10000):
    for action in batch:
        doc_id = action.get('_id')
        if doc_id:
            log_pipeline_status(client, doc_id)