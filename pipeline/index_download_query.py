from pipelinepackage.extractormodule import query_os
import click
import json
import datetime

index = input("Select index:")

current_year = datetime.datetime.now().year

for y in range(2017, current_year + 1):
    print('Extraction year ' + str(y))
    start_date = datetime.date(y, 1, 1)
    end_date = datetime.date(y, 12, 31)

    query = {
        "query": {
            "range": {
                "Dispatch Date": {
                    "gte": start_date,
                    "lte": end_date
                }
            }
        }
    }

    data = query_os(index, query)
    if len(data) > 0:
        data.to_csv(f"os_extraction_{start_date}_{end_date}.csv", index=False)