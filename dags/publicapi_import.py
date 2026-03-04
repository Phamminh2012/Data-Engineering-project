import asyncio
import datetime as dt
import os
import json
import requests
from dotenv import load_dotenv
from datetime import timedelta, datetime

JOB_PORTAL_API = "https://singaporejobs.com.sg/api/v1/front/assignments/open?key_words=&page_size=50"

# fetch from one known job board with exposed API, most recent 50 entries.

def get_from_job_portal():
    api_result = requests.get(JOB_PORTAL_API)
    if api_result.status_code != 200:
        print("Failed!")
        return []
    data = api_result.json()
    results = data["results"]
    for entry in results:
        entry["description"] = "\n".join(entry["description"])
    with open(f"/opt/airflow/data/raw/job-board-public-{datetime.now()}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4, default=str)
    return data