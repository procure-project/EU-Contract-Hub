from deep_translator import GoogleTranslator
from datetime import datetime

TRANSLATOR = GoogleTranslator(source='auto', target='english')

def translate_title_batch(titles):
    titles = [title if title is not None else '-' for title in titles]
    start_time = datetime.now()
    print(f"Translating {len(titles)} titles... Start: {start_time}", end='', flush=True)
    translated_titles = TRANSLATOR.translate_batch(titles)
    end_time = datetime.now()
    print(f", End: {end_time}, Duration: {end_time - start_time}")
    translated_titles = [title if title is not None else '' for title in translated_titles]
    return translated_titles

def translate_description_batch(descriptions):
    all_lines = []
    line_mapping = []
    # Split descriptions into lines and keep track of line positions
    for i, description in enumerate(descriptions):
        if description is None:
            description = ""
        description_split = description.splitlines()
        all_lines.extend(description_split)
        line_mapping.append((i, len(description_split)))
    start_time = datetime.now()
    print(f"Translating {len(all_lines)} lines from descriptions... Start: {start_time}", end='', flush=True)
    translated_lines = TRANSLATOR.translate_batch([line if line is not None else '' for line in all_lines])
    translated_lines = [line if line is not None else '' for line in translated_lines]
    end_time = datetime.now()
    print(f", End: {end_time}, Duration: {end_time - start_time}")

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