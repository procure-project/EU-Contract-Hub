from pipelinepackage.extractormodule import query_os

query_os(   index = "procure",
            query = {
                "_source": ["Lots"],
                "query": {
                    "match_all": {}  # Retrieve all documents
                }
            }
         )

