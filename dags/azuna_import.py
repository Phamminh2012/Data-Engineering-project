import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

ADZUNA_APP_ID = os.getenv("Adzuna_API_application_ID")
ADZUNA_API_KEY = os.getenv("Adzuna_API_key")


def adzuna_import(keywords):
    if not ADZUNA_APP_ID or not ADZUNA_API_KEY:
        raise ValueError("Missing Adzuna API credentials.")

    country = "sg"
    page = 1

    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_API_KEY,
        "results_per_page": 20,
        "what": keywords,
        "sort_by": "date",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    print("TOP LEVEL KEYS:", list(data.keys()))
    print("COUNT RESULTS:", len(data.get("results", [])))

    jobs = data.get("results", [])
    

    with open("/opt/airflow/data/raw/adzuna_jobs.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=4, ensure_ascii=False)

    return jobs