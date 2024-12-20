from collections import Counter
from tqdm import tqdm
import numpy as np
import pandas as pd

import re

from transformers import pipeline
from sentence_transformers import SentenceTransformer, util

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression

#from gensim.models import Word2Vec

def reg_exp_classifier(hospital_names):
    patterns = [
        # Spain
        r"(?=.*Universitario|Universidad|Clínico)(?!.*Centro de Salud)",

        # Austria
        r"(?=.*Universitätsklinik|Universitätsklinikum|Medizinische)(?!.*Allgemeines Krankenhaus)",

        # France
        r"(?=.*Universitaire|CHU|Hospitalier)(?!.*Clinique Privée)",

        # Belgium
        r"(?=.*Université|UZ|Universitaires|Universitair)(?!.*Stedelijk Ziekenhuis)",

        # Malta
        r"(?=.*Mater Dei|Teaching|University)(?!.*Polyclinic)",

        # Slovakia
        r"(?=.*Univerzitná|Univerzitné)(?!.*Špecializovaná Nemocnica)",

        # Hungary
        r"(?=.*Egyetemi Klinika|Orvostudományi|Klinikai)(?!.*Kórház)",

        # Romania
        r"(?=.*Universitar|Universitar|Universitatea)(?!.*Spitalul de Urgență)",

        # Sweden
        r"(?=.*Universitetssjukhus|Akademiska|Karolinska)(?!.*Närsjukhus)",

        # Portugal
        r"(?=.*Universitário|CHU|Universitário)(?!.*Centro de Saúde)",

        # Greece
        r"(?=.*Πανεπιστημιακό Νοσοκομείο|Ιατρική Σχολή|Πανεπιστημιακό)(?!.*Κέντρο Υγείας)"
    ]

    # Compile patterns into a combined regex pattern for matching
    combined_pattern = re.compile('|'.join(patterns), re.IGNORECASE)

    return [bool(combined_pattern.search(name)) for name in hospital_names]

def reg_exp_classifier_3(hospital_names):
    patterns_outer = [
        # Spain
        r"(?=.*Universitario|Universidad|Clínico)(?!.*Centro de Salud)",

        # Austria
        r"(?=.*Universitätsklinik|Universitätsklinikum|Medizinische)(?!.*Allgemeines Krankenhaus)",

        # France
        r"(?=.*Universitaire|CHU|Hospitalier)(?!.*Clinique Privée)",

        # Belgium
        r"(?=.*Université|UZ|Universitaires|Universitair)(?!.*Stedelijk Ziekenhuis)",

        # Malta
        r"(?=.*Mater Dei|Teaching|University)(?!.*Polyclinic)",

        # Slovakia
        r"(?=.*Univerzitná|Univerzitné)(?!.*Špecializovaná Nemocnica)",

        # Hungary
        r"(?=.*Egyetemi Klinika|Orvostudományi|Klinikai)(?!.*Kórház)",

        # Romania
        r"(?=.*Universitar|Universitar|Universitatea)(?!.*Spitalul de Urgență)",

        # Sweden
        r"(?=.*Universitetssjukhus|Akademiska|Karolinska)(?!.*Närsjukhus)",

        # Portugal
        r"(?=.*Universitário|CHU|Universitário)(?!.*Centro de Saúde)",

        # Greece
        r"(?=.*Πανεπιστημιακό Νοσοκομείο|Ιατρική Σχολή|Πανεπιστημιακό)(?!.*Κέντρο Υγείας)"
    ]

    patterns_inner = [
        # Spain
        r"(?=.*(Hospital|Clínico|Sanitario|Salud|Universitario))(?!.*(Residencia|Hogar|Ministerio|Seguros|Centro de Día|Universidad\b(?!.*Hospital)))",

        # Austria
        r"(?=.*(Krankenhaus|Klinikum|Medizinische|Universitätsklinik|Universitätsklinikum))(?!.*(Pflegeheim|Bundesministerium|Versicherung|Seniorenzentrum|Universität\b(?!.*Klinik)))",

        # France
        r"(?=.*(Hôpital|Hospitalier|CH|Clinique|Universitaire|CHU))(?!.*(Maison de Retraite|Ministère|Assurance|Centre de Soins|Université\b(?!.*Hôpital)))",

        # Belgium
        r"(?=.*(Ziekenhuis|UZ|Kliniek|Medisch Centrum|Universitair|Universitair Ziekenhuis))(?!.*(Rusthuis|Overheid|Verzekering|Zorgcentrum|Universiteit\b(?!.*Ziekenhuis)))",

        # Malta
        r"(?=.*(Hospital|Mater Dei|Health Centre|Teaching|University))(?!.*(Nursing Home|Government Office|Insurance|Care Facility|University\b(?!.*Hospital)))",

        # Slovakia
        r"(?=.*(Nemocnica|Zdravotnícke|Klinika|Univerzitná|Univerzitné))(?!.*(Domov Dôchodcov|Ministerstvo|Poistenie|Opatrovateľské Centrum|Univerzita\b(?!.*Nemocnica)))",

        # Hungary
        r"(?=.*(Kórház|Klinika|Egészségügyi|Egyetemi Klinika|Orvostudományi|Klinikai))(?!.*(Idősek Otthona|Kormányzati Szerv|Biztosító|Gondozási Központ|Egyetem\b(?!.*Klinika)))",

        # Romania
        r"(?=.*(Spital|Clinic|Sanitar|Universitar|Universitatea))(?!.*(Cămin de Bătrâni|Minister|Asigurare|Centru de Îngrijire|Universitate\b(?!.*Spital)))",

        # Sweden
        r"(?=.*(Sjukhus|Akutvård|Medicinskt Centrum|Universitetssjukhus|Karolinska|Akademiska))(?!.*(Äldreboende|Myndighet|Försäkring|Omsorgsboende|Universitet\b(?!.*Sjukhus)))",

        # Portugal
        r"(?=.*(Hospital|Sanitário|Saúde|Clínico|Universitário|CHU))(?!.*(Lar de Idosos|Governo|Seguros|Unidade de Cuidados|Universidade\b(?!.*Hospital)))",

        # Greece
        r"(?=.*(Νοσοκομείο|Υγειονομικό Κέντρο|Κλινική|Πανεπιστημιακό Νοσοκομείο|Πανεπιστημιακό))(?!.*(Γηροκομείο|Κυβέρνηση|Ασφάλιση|Μονάδα Φροντίδας|Πανεπιστήμιο\b(?!.*Νοσοκομείο)))"
    ]

    # Compile patterns into a combined regex pattern for matching
    combined_pattern_outer = re.compile('|'.join(patterns_outer), re.IGNORECASE)
    combined_pattern_inner = re.compile('|'.join(patterns_outer), re.IGNORECASE)

    return [
            'University Hospital' if combined_pattern_outer.search(name) and combined_pattern_inner.search(name)
            else 'Hospital' if combined_pattern_outer.search(name)
            else 'Other'
            for name in hospital_names
        ]

from collections import Counter

def generate_optimal_regex_patterns(hospital_names, labels, pos_tokens=10):
    keyword_counts = Counter()

    # Update keyword counts based on labels
    for name, is_university in zip(hospital_names, labels):
        tokens = re.findall(r'\w+', name.lower())  # Lowercase for case insensitive matching
        if is_university:
            keyword_counts.update(tokens)
        else:
            keyword_counts.subtract(tokens)

    # Get the most common and least common keywords
    include_keywords = [kw for kw, _ in keyword_counts.most_common(pos_tokens)]

    # Create regex patterns
    include_pattern = r"(?=.*" + r"|".join(include_keywords) + r")"

    # Combine patterns
    combined_pattern = re.compile(include_pattern, re.IGNORECASE)

    return combined_pattern

def reg_exp_classifier_auto(X_train, y_train, x_test, tokens):
    auto_pattern = generate_optimal_regex_patterns(X_train, y_train, tokens)
    return auto_pattern, [bool(auto_pattern.search(name)) for name in x_test]

def generate_optimal_regex_patterns_3(hospital_names, labels, pos_tokens=10):
    keyword_counts_outer = Counter()
    keyword_counts_inner = Counter()
    # Update keyword counts based on labels
    for name, label in zip(hospital_names, labels):
        tokens = re.findall(r'\w+', name.lower())  # Lowercase for case insensitive matching
        if label == 'Hospital' or label == 'University Hospital':
            keyword_counts_outer.update(tokens)
            if label == 'University Hospital': #Inner classification keyword generation
                keyword_counts_inner.update(tokens)
            else:
                keyword_counts_inner.subtract(tokens)
        else:
            keyword_counts_outer.subtract(tokens)

    include_keywords_outer = [kw for kw, _ in keyword_counts_outer.most_common(pos_tokens)]
    include_keywords_inner = [kw for kw, _ in keyword_counts_inner.most_common(pos_tokens)]

    # Create regex patterns
    include_pattern_outer = r"(?=.*" + r"|".join(include_keywords_outer) + r")"
    include_pattern_inner = r"(?=.*" + r"|".join(include_keywords_inner) + r")"
    # Combine patterns
    combined_pattern_outer = re.compile(include_pattern_outer, re.IGNORECASE)
    combined_pattern_inner = re.compile(include_pattern_inner, re.IGNORECASE)

    return combined_pattern_outer, combined_pattern_inner

def reg_exp_classifier_auto_3(X_train, y_train, x_test, tokens):
    auto_pattern_outer, auto_pattern_inner = generate_optimal_regex_patterns_3(X_train, y_train, tokens)
    classification = [
    'University Hospital' if auto_pattern_outer.search(name) and auto_pattern_inner.search(name)
    else 'Hospital' if auto_pattern_outer.search(name)
    else 'Other'
    for name in x_test
]
    return classification


# Zero-Shot Classifier using Hugging Face
def zero_shot_classifier_binary(hospital_names, classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")):
    candidate_labels = ["university hospital", "non-university hospital"]
    predictions = []

    for name in tqdm(hospital_names, desc="Classifying"):
        result = classifier(name, candidate_labels)
        # Get the label with the highest score
        predictions.append(1 if result['scores'][0] > result['scores'][1] else 0)

    return predictions

def zero_shot_classifier_binary_3(hospital_names,  classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")):
    candidate_labels = ["university hospital", "non-university hospital", "other"]
    label_mapping = {
        "university hospital": "University Hospital",
        "non-university hospital": "Hospital",
        "other": "Other"
    }
    predictions = []

    for name in tqdm(hospital_names, desc="Classifying"):
        result = classifier(name, candidate_labels)
        # Get the label with the highest score
        max_score_index = result['scores'].index(max(result['scores']))
        predicted_label = result['labels'][max_score_index]
        mapped_label = label_mapping.get(predicted_label, "Other")
        predictions.append(mapped_label)

    return predictions

def zero_shot_classifier(hospital_names, candidate_label = ["university hospital"], classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")):
    predictions = []

    for name in tqdm(hospital_names, desc="Classifying"):
        result = classifier(name, candidate_label)
        # Get the label with the highest score
        predictions.append(result['scores'][0])
    return predictions


# Semantic Similarity Classifier using Sentence-BERT
def semantic_similarity_classifier_binary(hospital_names, candidate_labels = ["university hospital", "non-university hospital"], model = SentenceTransformer('paraphrase-MiniLM-L6-v2')):

    predictions = []

    # Embed the candidate labels
    label_embeddings = model.encode(candidate_labels)

    for name in tqdm(hospital_names, desc="Classifying"):
        # Embed the hospital name
        name_embedding = model.encode(name)
        # Calculate similarity scores
        similarity_scores = util.pytorch_cos_sim(name_embedding, label_embeddings)
        # Classify based on the highest similarity
        predictions.append(1 if similarity_scores[0][0] > similarity_scores[0][1] else 0)

    return predictions


# Semantic Similarity Classifier using Sentence-BERT
def semantic_similarity_classifier_binary_3(hospital_names, model = SentenceTransformer('paraphrase-MiniLM-L6-v2')):
    candidate_labels = ["university hospital", "non-university hospital", "other"]
    label_mapping = {
        "university hospital": "University Hospital",
        "non-university hospital": "Hospital",
        "other": "Other"
    }
    predictions = []

    # Embed the candidate labels
    label_embeddings = model.encode(candidate_labels, convert_to_tensor=True)

    for name in tqdm(hospital_names, desc="Classifying"):
        # Embed the hospital name
        name_embedding = model.encode(name, convert_to_tensor=True)
        # Calculate similarity scores
        similarity_scores = util.pytorch_cos_sim(name_embedding, label_embeddings)
        max_score_index = similarity_scores.argmax().item()
        predicted_label = candidate_labels[max_score_index]
        mapped_label = label_mapping.get(predicted_label, "Other")
        predictions.append(mapped_label)

    return predictions

def semantic_similarity_classifier(hospital_names, candidate_label = ["university hospital"], model = SentenceTransformer('paraphrase-MiniLM-L6-v2')):

    predictions = []

    # Embed the candidate labels
    label_embeddings = model.encode(candidate_label)

    for name in tqdm(hospital_names, desc="Classifying"):
        # Embed the hospital name
        name_embedding = model.encode(name)
        # Calculate similarity scores
        similarity_scores = util.pytorch_cos_sim(name_embedding, label_embeddings)
        # Classify based on the highest similarity
        predictions.append(similarity_scores[0][0])

    return predictions

def tfidf_classifier(X_train, y_train, X_test, model = LogisticRegression()):
    vectorizer = TfidfVectorizer()

    # Step 1: Vectorize data
    X_train_tfidf = vectorizer.fit_transform(X_train)
    X_test_tfidf = vectorizer.transform(X_test)

    # Step 2: Train the provided model
    model.fit(X_train_tfidf, y_train)

    # Step 3: Make predictions on the test data
    predictions = model.predict(X_test_tfidf)

    # Return the trained model and predictions
    return model, predictions



# def word2vec_classifier(X_train, y_train, X_test, model=LogisticRegression()):
#     # Step 1: Tokenize the hospital names
#     tokenized_train = [name.lower().split() for name in X_train]
#
#     # Step 2: Train the Word2Vec model
#     word2vec_model = Word2Vec(sentences=tokenized_train, vector_size=100, window=5, min_count=1, workers=4)
#
#     # Step 3: Vectorize the data using Word2Vec embeddings
#     def vectorize_with_word2vec(hospital_names, word2vec):
#         vectors = []
#         for name in tqdm(hospital_names, desc="Vectorizing"):
#             tokenized = name.lower().split()
#             word_vectors = [word2vec.wv[word] for word in tokenized if word in word2vec.wv]
#
#             # Calculate the mean of word vectors to get a fixed-length vector
#             if word_vectors:
#                 name_vector = np.mean(word_vectors, axis=0)
#             else:
#                 # If no valid tokens, use a zero vector of the specified size
#                 name_vector = np.zeros(word2vec.vector_size)
#             vectors.append(name_vector)
#         return vectors
#
#     # Vectorize both training and test data
#     X_train_vectors = vectorize_with_word2vec(X_train, word2vec_model)
#     X_test_vectors = vectorize_with_word2vec(X_test, word2vec_model)
#
#     # Step 4: Train the provided classifier model
#     model.fit(X_train_vectors, y_train)
#
#     # Step 5: Make predictions on the test data
#     predictions = model.predict(X_test_vectors)
#
#     # Return the trained classifier model and predictions
#     return model, predictions



