import json
import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher
from datetime import datetime
from pymongo import MongoClient

def do_skill_tagging_jsearch(collection_name):
    # Load SPACY Model
    nlp = spacy.load("en_core_web_sm")

    # Load pre-defined skills
    # Source: https://www.kaggle.com/datasets/dhivyadharunaba/it-job-roles-skills-dataset

    skills_data = pd.read_csv("/opt/airflow/dags/distinct_skills.csv")["skill"].dropna().tolist()
    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    patterns = [nlp.make_doc(skill) for skill in skills_data if isinstance(skill, str)]
    matcher.add("SKILLS", patterns)

    def extract_skills(text):

        doc = nlp(text or "")

        matches = matcher(doc)

        found = set()

        for _, start, end in matches:
            found.add(doc[start:end].text.lower())

        return sorted(found)
    
    # Connect to MongoDB
    client = MongoClient('mongodb://host.docker.internal:27017')
    db = client['raw_api_result']
    collection = db[collection_name]
    
    # Find documents that don't have skills yet
    cursor = collection.find({'skills': {'$exists': False}})
    
    updated_count = 0
    for doc in cursor:
        skills = extract_skills(doc.get('job_description', ''))
        collection.update_one({'_id': doc['_id']}, {'$set': {'skills': skills}})
        updated_count += 1
    
    client.close()
    
    return updated_count