# Aggregations Operators!
# After some feedback, this isn't really so sophisicated...
# That being said, it's still a work in progress.
MDB_LINK = "mongodb://host.docker.internal:27017"

def do_job_count(whatever):
    from pymongo import MongoClient
    import pandas as pd

    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    client = MongoClient(MDB_LINK)
    pipeline = [
        {
            '$match': {
                'created': {
                    '$ne': None
                }
            }
        }, {
            '$group': {
                '_id': {
                    '$dateToString': {
                        'format': '%Y-%m-%d', 
                        'date': '$created'
                    }
                }, 
                'jobCounts': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                '_id': -1
            }
        }
    ]
    result = list(client['transformed']['transformed'].aggregate(pipeline))
    
    # Output job counts to CSV
    df = pd.DataFrame(result)
    df.to_csv('/opt/airflow/dags/job_counts.csv', index=False)
    
    # Merge into DB
    for item in result:
        client['final']['jobCount'].update_one({'_id': item['_id']}, {'$set': item}, upsert=True)

def do_skills_count(whatever):
    from pymongo import MongoClient
    import pandas as pd

    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    client = MongoClient(MDB_LINK)
    pipeline = [
        {
            '$match': {
                'created': {
                    '$ne': None
                }
            }
        }, {
            '$project': {
                'created': 1, 
                'skills': 1
            }
        }, {
            '$unwind': {
                'path': '$skills'
            }
        }, {
            '$project': {
                'skills': {
                    '$toLower': '$skills'
                }, 
                'created': 1
            }
        }, {
            '$group': {
                '_id': {
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d', 
                            'date': '$created'
                        }
                    }, 
                    'skill': '$skills'
                }, 
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                '_id.date': 1, 
                'count': -1
            }
        }, {
            '$group': {
                '_id': '$_id.date', 
                'top_skills': {
                    '$push': {
                        'skill': '$_id.skill', 
                        'count': '$count'
                    }
                }
            }
        }, {
            '$project': {
                'top_skils': {
                    '$slice': [
                        '$top_skills', 5
                    ]
                }
            }
        }, {
            '$sort': {
                '_id': -1
            }
        }
    ]
    result = list(client['transformed']['transformed'].aggregate(pipeline))
    
    # Output top skills to CSV
    skills_data = []
    for doc in result:
        date = doc['_id']
        for skill in doc.get('top_skils', []):
            skills_data.append({'date': date, 'skill': skill['skill'], 'count': skill['count']})
    df = pd.DataFrame(skills_data)
    df.to_csv('/opt/airflow/dags/top_skills.csv', index=False)
    
    # Merge into DB
    for item in result:
        client['final']['top5Skills'].update_one({'_id': item['_id']}, {'$set': item}, upsert=True)


def do_job_description_wordcloud(whatever):
    from pymongo import MongoClient
    from collections import Counter
    import re
    import pandas as pd
    from wordcloud import WordCloud
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from nltk.stem import WordNetLemmatizer

    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('wordnet', quiet=True)

    def preprocess_text(text):
        stop_words = set(stopwords.words('english')) | {'software', 'experience', 'team', 'business', 'development', 'technical', 'job', 'role', 'skills', 'responsibilities', 'requirements', 'ability'}
        lemmatizer = WordNetLemmatizer()
        words = [lemmatizer.lemmatize(word.lower()) for word in text.split()]
        return ' '.join([word for word in words if word not in stop_words])

    client = MongoClient(MDB_LINK)
    collection = client['transformed']['transformed']

    query = {
        '$or': [
            {'job_description': {'$exists': True, '$ne': None}},
            {'description': {'$exists': True, '$ne': None}}
        ]
    }

    docs = collection.find(query, {'job_description': 1, 'description': 1})
    text_parts = []

    for doc in docs:
        text = None
        if 'job_description' in doc and isinstance(doc['job_description'], str) and doc['job_description'].strip():
            text = doc['job_description']
        elif 'description' in doc and isinstance(doc['description'], str) and doc['description'].strip():
            text = doc['description']
        if text:
            text_parts.append(text)

    if not text_parts:
        raise ValueError('No job_description or description text found in transformed.transformed collection')

    # Preprocess each text part
    preprocessed_texts = [preprocess_text(text) for text in text_parts]
    text_corpus = '\n'.join(preprocessed_texts)

    try:
        tokenized = word_tokenize(text_corpus)
    except LookupError as e:
        # NLTK punkt missing despite downloads; fallback to whitespace+regex split
        tokenized = re.findall(r"[A-Za-z0-9']+", text_corpus)

    words = [re.sub(r"[^a-z0-9]", "", w) for w in tokenized]
    tokens = [t for t in words if t and len(t) > 2]

    if not tokens:
        raise ValueError('No valid tokens available after normalization')

    freq = Counter(tokens)

    rel_path_prefix = '/opt/airflow/dags'
    word_freq_path = f'{rel_path_prefix}/job_description_word_freq.csv'
    image_path = f'{rel_path_prefix}/job_description_wordcloud.png'

    pd.DataFrame(freq.most_common(200), columns=['word', 'count']).to_csv(word_freq_path, index=False)

    wc = WordCloud(width=1600, height=800, background_color='white', colormap='viridis').generate_from_frequencies(freq)
    wc.to_file(image_path)

    return {
        'image_path': image_path,
        'freq_csv': word_freq_path,
        'total_words': sum(freq.values()),
        'unique_words': len(freq)
    }


def do_regression(whatever):
    from pymongo import MongoClient
    import pandas as pd
    import statsmodels.api as sm
    from sklearn.preprocessing import MultiLabelBinarizer
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    import json
    from statsmodels.nonparametric.smoothers_lowess import lowess

    client = MongoClient(MDB_LINK)
    
    # Query mcf collection for documents with salary and skills
    data = list(client['raw_api_result']['mcf'].find(
        {'salary': {'$exists': True}, 'skills': {'$exists': True, '$ne': []}},
        {'skills': 1, 'salary': 1}
    ))
    
    # Extract salary_mean
    for item in data:
        if 'salary' in item and 'minimum' in item['salary'] and 'maximum' in item['salary']:
            min_sal = item['salary']['minimum']
            max_sal = item['salary']['maximum']
            if min_sal is not None and max_sal is not None:
                item['salary_mean'] = (min_sal + max_sal) / 2
            else:
                item['salary_mean'] = None
        else:
            item['salary_mean'] = None
    
    # Extract skills as list of strings
    for item in data:
        if 'skills' in item and isinstance(item['skills'], list):
            item['skills'] = [s['skill'] for s in item['skills'] if 'skill' in s]
        else:
            item['skills'] = []
    
    # Load into DataFrame and perform regression
    df = pd.DataFrame(data)
    df = df.dropna(subset=['salary_mean', 'skills'])
    
    if df.empty:
        print("No valid data for regression")
        return
    
    mlb = MultiLabelBinarizer()
    encoded_skills = mlb.fit_transform(df["skills"])
    df_skills = pd.DataFrame(encoded_skills, columns=mlb.classes_)
    df_reg = pd.concat([df[["salary_mean"]], df_skills], axis=1)
    
    X = df_reg.drop(["salary_mean"], axis=1)
    y = df_reg["salary_mean"]
    
    X = sm.add_constant(X)
    
    model = sm.OLS(y, X).fit()
    
    with open("/opt/airflow/dags/Summary.txt", "w") as f:
        f.write(str(model.summary()))
    
    summary_df = pd.DataFrame({'Coefficients': model.params, 'P-Values': model.pvalues})
    summary_df = summary_df.sort_values(by="P-Values", ascending=True)
    summary_df.to_csv("/opt/airflow/dags/CoefficeintReport.csv")

    #draw graph
    coef = model.params.drop('const')
    pvals = model.pvalues.drop('const')
    sig = coef[pvals < 0.05].sort_values()
    
    #top 6 pos + neg
    top = pd.concat([sig.head(6), sig.tail(6)]).drop_duplicates()
    colors = ['#e05c5c' if v < 0 else '#4caf7d' for v in top.values]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(top.index, top.values, color=colors, height=0.6)
    ax.axvline(0, color='gray', linewidth=0.8, linestyle='--')
    ax.set_xlabel('Coefficient (SGD/month)')
    ax.set_title('Skill coefficients — significant only (p < 0.05)')
    pos_p = mpatches.Patch(color='#4caf7d', label='Positive effect')
    neg_p = mpatches.Patch(color='#e05c5c', label='Negative effect')
    ax.legend(handles=[pos_p, neg_p])
    plt.tight_layout()
    plt.savefig('/opt/airflow/dags/save_img/coef_plot.png', dpi=150)
    plt.close()
    ## chart 2
    fitted = model.fittedvalues
    residuals = model.resid

    fig2, ax2 = plt.subplots(figsize=(8, 5))
    ax2.scatter(fitted, residuals, alpha=0.35, s=18, color='#5b8dd9')
    ax2.axhline(0, color='gray', linewidth=0.8, linestyle='--')
    ax2.set_xlabel('Fitted values (SGD/month)')
    ax2.set_ylabel('Residuals')
    ax2.set_title('Residuals vs Fitted — OLS assumption check')
    
    sm_vals = lowess(residuals, fitted, frac=0.3)
    ax2.plot(sm_vals[:, 0], sm_vals[:, 1], color='#e07b54', linewidth=1.5, label='LOWESS trend')
    ax2.legend()
    plt.tight_layout()
    plt.savefig('/opt/airflow/dags/save_img/residuals_plot.png', dpi=150)
    plt.close()

def do_topic_modeling(whatever):
    from pymongo import MongoClient
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    from sklearn.feature_extraction.text import CountVectorizer
    from sklearn.decomposition import LatentDirichletAllocation
    from sklearn.metrics import silhouette_score, silhouette_samples
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import numpy as np
    import pandas as pd
    import nltk
    import os

    os.makedirs('/opt/airflow/dags/save_img', exist_ok=True)

    nltk.download('stopwords', quiet=True)
    nltk.download('wordnet', quiet=True)

    def preprocess_text(text):
        stop_words = set(stopwords.words('english')) | {
            'software', 'experience', 'team', 'business', 'development', 'technical',
            'strong', 'using', 'including', 'platform', 'work', 'support'
        }
        return ' '.join([word for word in text.split() if word.lower() not in stop_words])

    def lemmatise_text(text):
        lemmatizer = WordNetLemmatizer()
        return ' '.join([lemmatizer.lemmatize(word) for word in text.split()])

    def perform_topic_modeling(texts, num_topics=5):
        preprocessed_texts = [lemmatise_text(text) for text in texts]
        preprocessed_texts = [preprocess_text(text) for text in preprocessed_texts]
        vectorizer = CountVectorizer()
        dtm = vectorizer.fit_transform(preprocessed_texts)
        lda = LatentDirichletAllocation(n_components=num_topics, random_state=42)
        lda.fit(dtm)
        topics = []
        for topic_idx, topic in enumerate(lda.components_):
            top_words = [vectorizer.get_feature_names_out()[i] for i in topic.argsort()[:-11:-1]]
            topics.append(f"Topic {topic_idx + 1}: " + ", ".join(top_words))
        return topics, lda, vectorizer, dtm, preprocessed_texts

    # --- Fetch data ---
    client = MongoClient(MDB_LINK)
    collection = client['transformed']['transformed']

    query = {
        '$or': [
            {'job_description': {'$exists': True, '$ne': None}},
            {'description': {'$exists': True, '$ne': None}}
        ]
    }

    docs = collection.find(query, {'job_description': 1, 'description': 1})
    texts = []

    for doc in docs:
        text = None
        if 'job_description' in doc and isinstance(doc['job_description'], str) and doc['job_description'].strip():
            text = doc['job_description']
        elif 'description' in doc and isinstance(doc['description'], str) and doc['description'].strip():
            text = doc['description']
        if text:
            texts.append(text)

    if not texts:
        raise ValueError('No job_description or description text found')

    topics, lda, vectorizer, dtm, preprocessed_texts = perform_topic_modeling(texts, num_topics=9)

    # --- Save topics to txt ---
    with open('/opt/airflow/dags/topics.txt', 'w') as f:
        f.write("Identified Topics:\n")
        for topic in topics:
            f.write(topic + "\n")
        f.write(f"Correct as of {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # --- Merge into DB ---
    topic_doc = {'_id': 'latest_topics', 'topics': topics}
    client['final']['topics'].update_one({'_id': 'latest_topics'}, {'$set': topic_doc}, upsert=True)

    # --- Silhouette Score ---
    # Convert LDA topic distributions to hard cluster labels (argmax = dominant topic)
    topic_distributions = lda.transform(dtm)       # shape: (n_docs, n_topics)
    cluster_labels = np.argmax(topic_distributions, axis=1)  # each doc assigned to dominant topic

    # Need dense matrix for silhouette
    X_dense = dtm.toarray()

    avg_score = silhouette_score(X_dense, cluster_labels)
    sample_scores = silhouette_samples(X_dense, cluster_labels)

    # Save score to txt
    with open('/opt/airflow/dags/topics.txt', 'a') as f:
        f.write(f"\nSilhouette Score (avg): {avg_score:.4f}\n")
        f.write("Interpretation: >0.5 strong, 0.25-0.5 reasonable, <0.25 weak clusters\n")

    # --- Chart 1: Per-sample silhouette plot ---
    fig, ax = plt.subplots(figsize=(8, 5))
    y_lower = 10
    colors = cm.tab10(np.linspace(0, 1, lda.n_components))

    for i in range(lda.n_components):
        cluster_scores = np.sort(sample_scores[cluster_labels == i])
        size = cluster_scores.shape[0]
        if size == 0:
            continue
        y_upper = y_lower + size
        ax.fill_betweenx(np.arange(y_lower, y_upper), 0, cluster_scores,
                         facecolor=colors[i], alpha=0.75, label=f'Topic {i+1} (n={size})')
        y_lower = y_upper + 8

    ax.axvline(avg_score, color='red', linestyle='--', linewidth=1.2,
               label=f'Avg score: {avg_score:.3f}')
    ax.set_xlabel('Silhouette coefficient')
    ax.set_title('Per-document silhouette score by dominant topic')
    ax.set_yticks([])
    ax.legend(loc='lower right', fontsize=8)
    plt.tight_layout()
    plt.savefig('/opt/airflow/dags/save_img/silhouette_plot.png', dpi=150)
    plt.close()

    # --- Chart 2: Topic-Word Heatmap ---
    feature_names = vectorizer.get_feature_names_out()
    top_n = 10
    top_indices = np.argsort(lda.components_, axis=1)[:, -top_n:][:, ::-1]
    shared_indices = top_indices[0]
    top_labels = [feature_names[i] for i in shared_indices]

    weight_matrix = np.array([
        lda.components_[t][shared_indices] for t in range(lda.n_components)
    ])
    weight_norm = weight_matrix / weight_matrix.max(axis=1, keepdims=True)

    fig2, ax2 = plt.subplots(figsize=(10, 5))
    im = ax2.imshow(weight_norm, aspect='auto', cmap='Blues')
    ax2.set_xticks(range(top_n))
    ax2.set_xticklabels(top_labels, rotation=35, ha='right', fontsize=9)
    ax2.set_yticks(range(lda.n_components))
    ax2.set_yticklabels([f'Topic {i+1}' for i in range(lda.n_components)], fontsize=9)
    plt.colorbar(im, ax=ax2, label='Normalised weight')
    ax2.set_title('Topic-word weight heatmap (top 10 words of topic 1)')
    plt.tight_layout()
    plt.savefig('/opt/airflow/dags/save_img/topic_heatmap.png', dpi=150)
    plt.close()