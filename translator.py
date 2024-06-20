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
    return df.drop(['Title', 'Description'], axis=1)



host = 'localhost'
port = 9200
username = input("Enter ProCureSpot username: ")
password = getpass.getpass(prompt="Enter ProCureSpot password: ")
auth = (username, password)

client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=True,
    ssl_show_warn=False,
)

index_name = "ted-xml"
# Define the query to retrieve all documents
query = {
    "query": {
        "match_all": {}  # Retrieve all documents
    }
}
scroll_size = 1000
# Execute the initial search query to get the first batch of results
response = client.search(
    index=index_name,
    body=query,
    size=scroll_size,  # Number of documents to retrieve per batch
    scroll="60m"  # Keep the scroll window open for 1 minute
)
scroll_id = response["_scroll_id"]
scr = 1
while True:
    translated = pd.read_csv('temp_translations.csv',usecols=['Document ID'])
    print("Already Translated: " + str(len(translated)))
    # Continue scrolling
    response = client.scroll(scroll_id=scroll_id, scroll="60m")
    id_field_pairs = []

    # Extract document IDs and corresponding field values from the current batch of results
    for hit in response["hits"]["hits"]:  # Processing and Extracting Info Document-wise
        if not (isinstance(hit["_source"]["CONTRACT_AWARD_NOTICE"],
                           list)):  # REMOVE CONDITION there should not be any list in final version
            doc_id = hit["_id"]
            title = hit["_source"]["CONTRACT_AWARD_NOTICE"]["OBJECT_CONTRACT"]["TITLE"]["P"]
            description = hit["_source"]["CONTRACT_AWARD_NOTICE"]["OBJECT_CONTRACT"]["SHORT_DESCR"]["P"]
            title_translated = "-"
            description_translated = "-"

            id_field_pairs.append((doc_id, title, title_translated, description, description_translated))
    # Processing fields Scroll-level
    df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description",
                                               "Description (Translation)"])
    print('Lines before dropping: '+ str(len(df)))
    df = df[~df['Document ID'].isin(translated['Document ID'])]
    print('Lines to translate: ' + str(len(df)))
    try:
        df_to_write = batch_translate(df)
        df.to_csv('temp_translations.csv',mode='a', index=False, header=False)
    except Exception as e:
        print(e)
    scr = scr + 1
    if len(response["hits"]["hits"]) < scroll_size:
        break
# Create a DataFrame to store the document IDs and field values