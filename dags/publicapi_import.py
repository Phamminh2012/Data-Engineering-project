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
        entry["source"] = "job_portal_direct_api"
    return data