import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

REED_API_KEY = os.getenv("Reed_API_key")

REED_API_URL = "https://www.reed.co.uk/api/1.0/search"


def get_from_reed():
    if not REED_API_KEY:
        raise ValueError("Missing Reed API key in environment variables.")

    params = {
        "keywords": "software developer and AI",
        "resultsToTake": 50
    }

    response = requests.get(
        REED_API_URL,
        params=params,
        auth=(REED_API_KEY, "")  # Reed uses Basic Auth
    )

    if response.status_code != 200:
        print("Failed!")
        return []

    data = response.json()
    results = data.get("results", [])

    jobs = []

    for job in results:
        jobs.append({
            "job_id": job.get("jobId"),
            "title": job.get("jobTitle"),
            "company": job.get("employerName"),
            "location": job.get("locationName"),
            "description": job.get("jobDescription", ""),
            "salary_min": job.get("minimumSalary"),
            "salary_max": job.get("maximumSalary"),
            "date": job.get("date"),
            "url": job.get("jobUrl"),
            "source": "reed"
        })

    f_path = "/opt/airflow/data/raw/reed_jobs.json"

    with open(f_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=4, default=str)

    return jobs