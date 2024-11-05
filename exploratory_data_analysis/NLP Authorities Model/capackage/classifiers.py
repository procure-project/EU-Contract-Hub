from collections import Counter
from tqdm import tqdm
import numpy as np
import pandas as pd

import re

from transformers import pipeline
from sentence_transformers import SentenceTransformer, util

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression

from gensim.models import Word2Vec


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



# Zero-Shot Classifier using Hugging Face
def zero_shot_classifier_binary(hospital_names, classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")):
    candidate_labels = ["university hospital", "non-university hospital"]
    predictions = []

    for name in tqdm(hospital_names, desc="Classifying"):
        result = classifier(name, candidate_labels)
        # Get the label with the highest score
        predictions.append(1 if result['scores'][0] > result['scores'][1] else 0)

    return predictions

def zero_shot_classifier(hospital_names, classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")):
    candidate_label = ["university hospital"]
    predictions = []

    for name in tqdm(hospital_names, desc="Classifying"):
        result = classifier(name, candidate_label)
        # Get the label with the highest score
        predictions.append(result['scores'][0])
    return predictions


# Semantic Similarity Classifier using Sentence-BERT
def semantic_similarity_classifier_binary(hospital_names, model = SentenceTransformer('paraphrase-MiniLM-L6-v2')):
    candidate_labels = ["university hospital", "non-university hospital"]
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


def semantic_similarity_classifier(hospital_names, model = SentenceTransformer('paraphrase-MiniLM-L6-v2')):
    candidate_label = ["university hospital"]
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



def word2vec_classifier(X_train, y_train, X_test, model=LogisticRegression()):
    # Step 1: Tokenize the hospital names
    tokenized_train = [name.lower().split() for name in X_train]

    # Step 2: Train the Word2Vec model
    word2vec_model = Word2Vec(sentences=tokenized_train, vector_size=100, window=5, min_count=1, workers=4)

    # Step 3: Vectorize the data using Word2Vec embeddings
    def vectorize_with_word2vec(hospital_names, word2vec):
        vectors = []
        for name in tqdm(hospital_names, desc="Vectorizing"):
            tokenized = name.lower().split()
            word_vectors = [word2vec.wv[word] for word in tokenized if word in word2vec.wv]

            # Calculate the mean of word vectors to get a fixed-length vector
            if word_vectors:
                name_vector = np.mean(word_vectors, axis=0)
            else:
                # If no valid tokens, use a zero vector of the specified size
                name_vector = np.zeros(word2vec.vector_size)
            vectors.append(name_vector)
        return vectors

    # Vectorize both training and test data
    X_train_vectors = vectorize_with_word2vec(X_train, word2vec_model)
    X_test_vectors = vectorize_with_word2vec(X_test, word2vec_model)

    # Step 4: Train the provided classifier model
    model.fit(X_train_vectors, y_train)

    # Step 5: Make predictions on the test data
    predictions = model.predict(X_test_vectors)

    # Return the trained classifier model and predictions
    return model, predictions



