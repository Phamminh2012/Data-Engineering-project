
import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

BASE_URL="https://api.mycareersfuture.gov.sg/v2/jobs/"
def mcf_scrape(keywords, limit = None):
    params = {
        "search": keywords,
        "limit": limit
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    
    data = response.json()
    jobs = data.get("results", [])
    with open("/opt/airflow/data/raw/mcf_data.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=4, ensure_ascii=False)

    return data
