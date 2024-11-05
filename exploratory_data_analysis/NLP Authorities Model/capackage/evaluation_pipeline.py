import capackage as ca

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from transformers import pipeline
from sklearn.metrics import accuracy_score, recall_score, f1_score
from sentence_transformers import SentenceTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier


def get_train_test():
    df = ca.load_sparql_data()
    df = ca.clean_wd_data(df)
    # Plot the class distribution
    class_counts = df['category'].value_counts()
    print(class_counts)
    class_counts.plot(kind='barh', title='Distribution of Classes', figsize=(3, 0.5))
    plt.xlabel('Class')
    plt.ylabel('Count')
    plt.show()

    df = df.rename(columns={'category': 'label'})
    df['label'] = df['label'] == 'university hospital'
    return train_test_split(
        df['name'], df['label'], test_size=0.2, random_state=42
    )


evaluation_df = pd.DataFrame(columns=['method', 'model', 'parameters' , 'labels', 'predictions'])


def evaluate_reg_exp(train_test, evaluation_df = pd.DataFrame(columns=['method', 'model', 'parameters' , 'labels', 'predictions'])):
    X_train, X_test, y_train, y_test = train_test
    # MANUAL RULE-BASED CLASSIFIER
    predictions = ca.reg_exp_classifier(X_test)
    result_mr = pd.DataFrame({
        'method': 'Expression Matching',
        'model': 'N/A',
        'labels': y_test.tolist(),
        'predictions': predictions,
        'parameters': 'N/A'
    })

    # AUTO RULE-BASED CLASSIFIER
    results_ar = []
    for tokens in range(1, 20, 1):
        _, predictions = ca.reg_exp_classifier_auto(X_train, y_train, X_test, tokens)
        results_ar.append(pd.DataFrame({
            'method': 'Expression Matching',
            'model': 'N/A',
            'labels': y_test.tolist(),
            'predictions': predictions,
            'parameters': 'tokens=' + str(tokens)
        }))
    return pd.concat([evaluation_df] + [result_mr] + results_ar, ignore_index=True)

def evaluate_0shot(train_test, evaluation_df = pd.DataFrame(columns=['method', 'model', 'parameters' , 'labels', 'predictions'])):
    X_train, X_test, y_train, y_test = train_test
    # List of models for Zero-Shot Classification
    zero_shot_models = [
        ("facebook/bart-large-mnli", "BART"),
        ("MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7", "multilingual DeBERTa"),
        ("distilbert-base-uncased", "DistilBERT"),
        ("roberta-large-mnli", "RoBERTa"),
        ("xlnet-large-cased", "XLNet")
    ]
    results_0sb = []
    results_0st = []
    # Evaluate Zero-Shot Classifiers
    for model_id, model_name in zero_shot_models:
        print('Loading ' + model_name)
        classifier = pipeline("zero-shot-classification", model=model_id)
        # BINARY BASED CLASSIFIER
        predictions = ca.zero_shot_classifier_binary(X_test, classifier)
        results_0sb.append(pd.DataFrame({
            'method': '0-Shot',
            'model': model_name,
            'labels': y_test.tolist(),
            'predictions': predictions,
            'parameters': 'N/A'
        })
        )
        # THRESHOLD BASED CLASSIFIER
        print('Loading ' + model_name + ' with threshold')
        scores = ca.zero_shot_classifier(X_test, classifier)
        for t in np.arange(0, 1, 0.1):
            predictions = scores > t
            results_0st.append(pd.DataFrame({
                'method': '0-Shot',
                'model': model_name,
                'labels': y_test.tolist(),
                'predictions': predictions,
                'parameters': 'threshold=' + str(t)
            }))
    return pd.concat([evaluation_df] + results_0sb + results_0st, ignore_index=True)



def evaluate_semantic_sim(train_test, evaluation_df = pd.DataFrame(columns=['method', 'model', 'parameters' , 'labels', 'predictions'])):
    X_train, X_test, y_train, y_test = train_test
    # List of models for Semantic Similarity Classifier
    semantic_similarity_models = [
        ("paraphrase-MiniLM-L6-v2", "MiniLM"),
        ("sentence-transformers/all-MiniLM-L6-v2", "All MiniLM"),
        ("sentence-transformers/roberta-base-nli-stsb-mean-tokens", "RoBERTa"),
        ("sentence-transformers/xlm-r-bert-base-nli-stsb-mean-tokens", "XLM-R")
    ]

    results_ssb = []
    results_sst = []
    # Evaluate Semantic Similarity Classifiers
    for model_id, model_name in semantic_similarity_models:
        print('Loading ' + model_name)
        model = SentenceTransformer(model_id)

        # BINARY BASED CLASSIFIER
        predictions = ca.semantic_similarity_classifier_binary(X_test, model)
        results_ssb.append(pd.DataFrame({
            'method': 'Semantic Similarity',
            'model': model_name,
            'labels': y_test.tolist(),
            'predictions': predictions,
            'parameters': 'N/A'
        }))

        # THRESHOLD BASED CLASSIFIER
        print('Loading ' + model_name + ' with threshold')
        scores = ca.semantic_similarity_classifier(X_test, model)
        for t in np.arange(0, 1, 0.1):
            predictions = scores > t
            results_sst.append(pd.DataFrame({
                'method': 'Semantic Similarity',
                'model': model_name,
                'labels': y_test.tolist(),
                'predictions': predictions,
                'parameters': 'threshold=' + str(t)
            }))
    return pd.concat([evaluation_df] + results_ssb + results_sst, ignore_index=True)


def evaluate_supervised(train_test, evaluation_df = pd.DataFrame(columns=['method', 'model', 'parameters' , 'labels', 'predictions'])):
    X_train, X_test, y_train, y_test = train_test
    # Define the models and their possible parameters
    models = {
        'Logistic Regression': (LogisticRegression, {'C': [1.0, 0.1, 0.01], 'solver': ['lbfgs', 'liblinear']}),
        'Support Vector Classifier': (SVC, {'C': [1.0, 0.1, 0.01], 'kernel': ['linear', 'rbf']}),
        'Random Forest': (RandomForestClassifier, {'n_estimators': [100, 200], 'max_depth': [None, 5, 10]})
    }

    results_tfidf = []
    results_w2v = []

    # Call the TF-IDF classifiers with different models and parameters
    for model_name, (model_class, params) in models.items():
        from itertools import product
        param_combinations = [dict(zip(params, v)) for v in product(*params.values())]
        for param_set in param_combinations:
            print(f'Loading {model_name} with params {param_set}')
            model = model_class(**param_set)
            _, tfidf_predictions = ca.tfidf_classifier(X_train, y_train, X_test, model)
            results_tfidf.append(pd.DataFrame({
                'method': 'TF-IDF',
                'model': model_name,
                'labels': y_test.tolist(),
                'predictions': tfidf_predictions,
                'parameters': str(param_set)
            }))

    # Call the Word2Vec classifiers with different models and parameters
    for model_name, (model_class, params) in models.items():
        from itertools import product
        param_combinations = [dict(zip(params, v)) for v in product(*params.values())]
        for param_set in param_combinations:
            print(f'Loading {model_name} with params {param_set}')
            model = model_class(**param_set)
            _, word2vec_predictions = ca.word2vec_classifier(X_train, y_train, X_test, model)
            results_w2v.append(pd.DataFrame({
                'method': 'Word2Vec',
                'model': model_name,
                'labels': y_test.tolist(),
                'predictions': word2vec_predictions,
                'parameters': str(param_set)
            }))

    return pd.concat([evaluation_df] + results_tfidf + results_w2v, ignore_index=True)

def get_metrics(train_test, evaluation_df):
    X_train, X_test, y_train, y_test = train_test
    results = []

    # Add baseline metrics
    majority_class = y_train.value_counts().idxmax()
    baseline_accuracy = accuracy_score(y_test, [majority_class] * len(y_test))
    baseline_recall = recall_score(y_test, [majority_class] * len(y_test), pos_label=True)
    baseline_f1 = f1_score(y_test, [majority_class] * len(y_test), pos_label=True)
    results.append({
        'method': 'Majority Class',
        'model': 'N/A',
        'parameters': 'N/A',
        'accuracy': baseline_accuracy,
        'recall': baseline_recall,
        'f1': baseline_f1
    })
    for (method, model, parameters), group in evaluation_df.groupby(['method', 'model', 'parameters']):
        labels = np.array(group['labels'], dtype=int)
        predictions = np.array(group['predictions'], dtype=int)
        accuracy = accuracy_score(labels, predictions)
        recall = recall_score(labels, predictions, pos_label=True)
        f1 = f1_score(labels, predictions, pos_label=True)
        results.append({
            'method': method,
            'model': model,
            'parameters': parameters,
            'accuracy': accuracy,
            'recall': recall,
            'f1': f1
        })
    return pd.DataFrame(results)

def get_analytics(df):
    def plot_method(df, method):
        # Prepare the figure
        plt.figure(figsize=(10, 6))

        #Color palettes
        model_count = df['model'].nunique()

        def generate_colors(hue):
            return

        # Generate colors
        accuracy_colors = [sns.hls_palette(1, h=0.6, l=0.5, s=s)[0] for s in [1, 0.8, 0.6, 0.4, 0.2]]  # Blueish
        recall_colors = [sns.hls_palette(1, h=0.3, l=0.5, s=s)[0] for s in [1, 0.8, 0.6, 0.4, 0.2]]  # Greenish
        f1_colors = [sns.hls_palette(1, h=0.1, l=0.6, s=s)[0] for s in [1, 0.8, 0.6, 0.4, 0.2]]

        # Plot Baseline accuracy as a horizontal dashed line
        baseline_accuracy = df[(df['method'] == 'Majority Class')]['accuracy'].values[0]
        plt.axhline(y=baseline_accuracy, color='blue', linestyle='--', label=f'Baseline Accuracy ({baseline_accuracy:.2f})')

        # Filter data for the method
        rule_based_df = df[df['method'] == method]
        # Separate cases where 'parameters' is 'N/A' for special case plot
        special_case = rule_based_df[rule_based_df['parameters'] == 'N/A'].copy()
        normal_cases = rule_based_df[rule_based_df['parameters'] != 'N/A'].copy()

        # Extract float values from 'parameters' for the remaining cases
        normal_cases.loc[:,'param_value'] = normal_cases['parameters'].str.extract(r'=(\d*\.?\d+)').astype(float)

        # Sort by the extracted parameter values to ensure smooth plotting
        normal_cases = normal_cases.sort_values(by='param_value')

        # Plot lines for accuracy, recall, and f1
        for idx, model in enumerate(normal_cases['model'].unique()):
            model_df = normal_cases[normal_cases['model'] == model]
            plt.plot(model_df['param_value'], model_df['accuracy'], color=accuracy_colors[idx], marker='.',
                     label=f'Accuracy ({model})' if idx == 0 else "")
            plt.plot(model_df['param_value'], model_df['recall'], color=recall_colors[idx], marker='.',
                     label=f'Recall ({model})' if idx == 0 else "")
            plt.plot(model_df['param_value'], model_df['f1'], color=f1_colors[idx], marker='.',
                     label=f'F1 Score ({model})' if idx == 0 else "")

        # Plot special case as disconnected dots
        if not special_case.empty:
            x_min = normal_cases['param_value'].min()
            x_max = normal_cases['param_value'].max()
            x_shift = 0.1 * (x_max - x_min)
            for idx, model in enumerate(special_case['model'].unique()):
                model_df = special_case[special_case['model'] == model]
                plt.scatter(
                    x=[x_min-x_shift*(1+idx)] * len(model_df),  # Adjust x slightly to the left
                    y=model_df['accuracy'],
                    color=accuracy_colors[idx], marker='.', s=80, label=f'Accuracy ({model})' if idx == 0 else ""
                )
                plt.scatter(
                    x=[x_min-x_shift*(1+idx)] * len(model_df),
                    y=model_df['recall'],
                    color=recall_colors[idx], marker='.', s=80, label=f'Recall ({model})' if idx == 0 else ""
                )
                plt.scatter(
                    x=[x_min-x_shift*(1+idx)] * len(model_df),
                    y=model_df['f1'],
                    color=f1_colors[idx], marker='.', s=80, label=f'F1 Score ({model})' if idx == 0 else ""
                )


        # Add labels and title
        plt.xlabel("Parameter")
        plt.ylabel("Value")
        plt.title("Metrics for "+method)
        plt.legend()
        plt.show()

    plot_method(df,'Expression Matching')
    plot_method(df, '0-Shot')
    plot_method(df, 'Semantic Similarity')