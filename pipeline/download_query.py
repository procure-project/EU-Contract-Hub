from pipelinepackage.extractormodule import query_os
import click
import json


def get_user_query():
    # Provide a default query template
    default_query = """{
    "_source": [
        "Title",
        "Title (Translation)",
        "Description",
        "Description (Translation)",
        "CPV",
        "CPV Description",
        "Contract Nature",
        "Procurement Route",
        "Procurement Techniques",
        "Procurement Type",
        "Country",
        "Dispatch Date",
        "Lots",
        "Value"
    ],
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "CPV": {
                            "gte": 33600000,
                            "lte": 33698300
                        }
                    }
                }
            ]
        }
    }
}"""
    # Open the default editor with the template
    user_query = click.edit(default_query)

    if user_query is None:
        print("No query entered. Exiting.")
        return None

    # Validate or return the edited query
    return user_query.strip()

index = input("Select index:")
# Example usage
query = json.loads(get_user_query())
if query:
    data = query_os(index, query)
    data.to_csv("os_extraction.csv", index=False)
    print("Results saved on os_extraction.csv for query:")
    print(json.dumps(query, indent=4))