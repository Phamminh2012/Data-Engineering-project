import json
import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher
from datetime import datetime


# load spacy model
nlp = spacy.load("en_core_web_sm")


# load skill vocabulary
def load_skills(skill_csv):
    df = pd.read_csv(skill_csv)
    return df["skill"].dropna().tolist()


# build matcher once
def build_matcher(skills):

    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")

    patterns = [nlp.make_doc(skill) for skill in skills if isinstance(skill, str)]

    matcher.add("SKILLS", patterns)

    return matcher


# skill extraction
def extract_skills(text, matcher):

    doc = nlp(text or "")

    matches = matcher(doc)

    found = set()

    for _, start, end in matches:
        found.add(doc[start:end].text.lower())

    return sorted(found)


# transform pipeline
def transform_job_search(json_path, skill_csv):

    skills = load_skills(skill_csv)

    matcher = build_matcher(skills)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = []

    for job in data:

        description = job.get("job_description", "")

        transformed_job = {
            "title": job.get("job_title"),
            "description": description,
            "company": job.get("employer_name"),
            "createAt": None,
            "source": "job_search_api",
            "skills": extract_skills(description, matcher)
        }

        created_str = job.get("job_posted_at_datetime_utc")

        if created_str:
            try:
                dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                transformed_job["createAt"] = dt.strftime("%Y-%m-%d")
            except Exception:
                transformed_job["createAt"] = created_str

        result.append(transformed_job)

    with open("/opt/airflow/data/processed/res_job_search.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

  