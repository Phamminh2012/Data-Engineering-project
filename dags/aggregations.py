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

    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)

    STOPWORDS = set(stopwords.words('english')) | {
        'job', 'role', 'skills', 'experience', 'responsibilities', 'requirements', 'ability'
    }

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

    text_corpus = '\n'.join(text_parts)

    try:
        tokenized = word_tokenize(text_corpus.lower())
    except LookupError as e:
        # NLTK punkt missing despite downloads; fallback to whitespace+regex split
        tokenized = re.findall(r"[A-Za-z0-9']+", text_corpus.lower())

    words = [re.sub(r"[^a-z0-9]", "", w) for w in tokenized]
    tokens = [t for t in words if t and t not in STOPWORDS and len(t) > 2]

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
    import json

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
    
    # Save as JSON
    with open('/opt/airflow/dags/regression_data.json', 'w') as f:
        json.dump(data, f)
    
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