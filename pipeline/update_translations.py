from pipelinepackage.extractormodule import query_os, get_client, translation_upload_bulk_actions, process_bulk_batches


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
print(data.loc[:5])
actions = translation_upload_bulk_actions("translations_extraction.csv", index2)
process_bulk_batches(actions, client, batch_size=10000)