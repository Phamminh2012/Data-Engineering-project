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