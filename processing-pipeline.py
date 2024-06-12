import json

import concurrent.futures
from opensearchpy import OpenSearch, helpers
import pandas as pd
from deep_translator import GoogleTranslator
import getpass


translator = GoogleTranslator(source='auto', target='english')
def translate_title_batch(titles):
    return translator.translate_batch(titles)


def translate_description_batch(descriptions):
    all_lines = []
    line_mapping = []

    # Split descriptions into lines and keep track of line positions
    for i, description in enumerate(descriptions):
        description_split = description.splitlines()
        all_lines.extend(description_split)
        line_mapping.append((i, len(description_split)))

    # Translate all lines in batch
    translated_lines = translator.translate_batch(all_lines)

    # Reconstruct descriptions from translated lines
    translated_descriptions = []
    line_index = 0
    for doc_index, num_lines in line_mapping:
        translated_description = "\n".join(translated_lines[line_index:line_index + num_lines])
        translated_descriptions.append(translated_description)
        line_index += num_lines

    return translated_descriptions


# Function to apply parallel translation to the DataFrame
def batch_translate(df):
    title_translations = translate_title_batch(df['Title'].tolist())
    description_translations = translate_description_batch(df['Description'].tolist())
    df['Title (Translation)'] = title_translations
    df['Description (Translation)'] = description_translations
    return df
def processing_scroll(df):
    # VALUE FILTERING
    df['Value'] = df['Value'].where((df['Value'] > 100) & (df['Value'] < 10 ** 10), -1)

    # CPV HEALTHCARE CLASSIFICATION
    health_cpv_list = [33600000,
                       33110000,
                       33120000, 33130000, 33150000, 33160000, 33170000, 33180000, 33190000,
                       33141000, 33141420,
                       85100000,
                       35113400, 18143000]
    health_prefixes = [str(i).rstrip('0') for i in health_cpv_list]
    temp_cpv_strings = df['CPV'].astype(str).str.lstrip('0')
    df["Healthcare CPV"] = temp_cpv_strings.str.startswith(tuple(health_prefixes))

    # try:
    #     batch_translate(df)
    # except Exception as e:
    #     print(e)
    return df



# Initialize the OpenSearch client
host = 'localhost'
username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")

auth = (username, password)

# Create the client with SSL/TLS enabled, but hostname verification disabled.
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

# Define your index and field
index_name = "ted-xml"
# Define the query to retrieve all documents
query = {
    "query": {
        "match_all": {}  # Retrieve all documents
    }
}

# Execute the initial search query to get the first batch of results
response = client.search(
    index=index_name,
    body=query,
    size=100,  # Number of documents to retrieve per batch
    scroll="10m"  # Keep the scroll window open for 1 minute
)

scroll_id = response["_scroll_id"]

scr = 1
while True:
    # Continue scrolling
    response = client.scroll(scroll_id=scroll_id, scroll="1m")
    id_field_pairs = []

    # Extract document IDs and corresponding field values from the current batch of results
    for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
        if not (isinstance(hit["_source"]["CONTRACT_AWARD_NOTICE"],
                           list)):  # REMOVE CONDITION there should not be any list in final version
            doc_id = hit["_id"]
            title = hit["_source"]["CONTRACT_AWARD_NOTICE"]["OBJECT_CONTRACT"]["TITLE"]["P"]
            description = hit["_source"]["CONTRACT_AWARD_NOTICE"]["OBJECT_CONTRACT"]["SHORT_DESCR"]["P"]

            cpv = hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ORIGINAL_CPV"][0]["@CODE"] if isinstance(
                hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ORIGINAL_CPV"], list) else \
            hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ORIGINAL_CPV"]["@CODE"]
            cpv_desc = hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ORIGINAL_CPV"][0]["#text"] if isinstance(
                hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ORIGINAL_CPV"], list) else \
            hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ORIGINAL_CPV"]["#text"]
            health_cpv = False

            country = hit["_source"]["CODED_DATA_SECTION"]["NOTICE_DATA"]["ISO_COUNTRY"]["@VALUE"]

            ca_name = hit["_source"]["CONTRACT_AWARD_NOTICE"]["CONTRACTING_BODY"]["ADDRESS_CONTRACTING_BODY"][
                "OFFICIALNAME"]
            ca_details = hit["_source"]["CONTRACT_AWARD_NOTICE"]["CONTRACTING_BODY"]

            try:
                inner_hit = client.get(index="ted-csv", id=doc_id)
                value = inner_hit["_source"]["VALUE_EURO_FIN_2"]

                multiple_country = inner_hit["_source"]["B_MULTIPLE_COUNTRY"]
                central_body = inner_hit["_source"]["B_AWARDED_BY_CENTRAL_BODY"]
                joint_procurement = inner_hit["_source"]["B_INVOLVES_JOINT_PROCUREMENT"]
                cae_type = inner_hit["_source"]["CAE_TYPE"]
                if multiple_country:
                    proc_route = "Cross Country Procurement"
                elif joint_procurement:
                    proc_route = "Joint Procurement"
                elif not central_body:
                    proc_route = "Direct Procurement"
                elif cae_type == "1" or cae_type == "N":
                    proc_route = "Centralized Procurement at National Level"
                elif cae_type == "3" or cae_type == "R":
                    proc_route = "Centralized Procurement at Regional Level"
                elif cae_type == "4" or cae_type == "6" or cae_type == "8" or cae_type == "Z":
                    proc_route = "Centralized Procurement at Unspecified Level"
                else:
                    proc_route = "Not applicable"
            except Exception as e:
                print(f"An error occurred: {e}")
                value = -1
                proc_route = "Unknown"

            title_translated = "-"
            description_translated = "-"

            id_field_pairs.append((doc_id, title, title_translated, description, description_translated, cpv, cpv_desc,
                                   health_cpv, country, value, proc_route, ca_name, ca_details))
    # Processing fields Scroll-level
    df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description",
                                               "Description (Translation)", "CPV", "CPV Description", "Healthcare CPV",
                                               "Country", "Value", "Procurement Route", "Contracting Authority Name",
                                               "Contracting Authority Details"])

    processing_scroll(df)

    print("Scroll " + str(scr))
    actions = [
        {
            "_op_type": "index",
            "_index": "procure",
            "_id": doc['Document ID'],
            **{f"{col_name}": doc[col_name] for col_name in df.columns if col_name != "Document ID"}

        }
        for _, doc in df.iterrows()
    ]
    try:
        success, failed = helpers.bulk(client, actions, index="procure", raise_on_error=True, refresh=True)
        print(f"Successfully indexed {success} documents.")
        print(f"Failed to index {failed} documents.")
    except Exception as e:
        print(f"Error during bulk indexing: {e}")
    # Check if there are more results to fetch
    scr = scr + 1
    if len(response["hits"]["hits"]) < 100:
        break
# Create a DataFrame to store the document IDs and field values
