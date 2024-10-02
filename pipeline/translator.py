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

index_name = "procure"
scroll_size = 100
query = {
  "query": {
          "term": {
            "Title (Translation).keyword": "-"
          }
  },
  "size": scroll_size
}
while True:
    response = client.search(index=index_name, body=query)
    hits = response["hits"]["hits"]
    if not hits:
        print("No more documents to process.")
        break
    id_field_pairs = []
    for hit in hits:  # Processing and Extracting Info Document-wise
        doc_id = hit["_id"]
        title = hit["_source"]["Title"]
        description = hit["_source"]["Description"]
        title_translated = "-"
        description_translated = "-"
        id_field_pairs.append((doc_id, title, title_translated, description, description_translated))
    df = pd.DataFrame(id_field_pairs, columns=["Document ID", "Title", "Title (Translation)", "Description", "Description (Translation)"])
    try:
        df_translated  = batch_translate(df)
        actions = [
            {
                "_op_type": "update",
                "_index": index_name,
                "_id": row['Document ID'],
                "doc": {
                    "Title (Translation)": row['Title (Translation)'],
                    "Description (Translation)": row['Description (Translation)']
                }
            }
            for _, row in df_translated.iterrows()
        ]
        helpers.bulk(client, actions)
        print(f"Processed and updated {len(df_translated)} documents.")
    except Exception as e:
        print(e)
# Create a DataFrame to store the document IDs and field values