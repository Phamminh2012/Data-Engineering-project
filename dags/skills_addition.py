import json
import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher
from datetime import datetime

def do_skill_tagging_jsearch(raw_api_result):
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
    
    with open(raw_api_result, "r", encoding = "utf-8") as f:
        data = json.load(f)
    
    for entry in data:
        entry["skills"] = extract_skills(entry["job_description"])
    
    with open("/opt/airflow/processed_jsearch_skills.json", "w", encoding = "utf-8") as h:
        json.dump(data, h, indent = False)
    
    return "/opt/airflow/processed_jsearch_skills.json"