import asyncio
import datetime as dt
import os
import json
import requests
from dotenv import load_dotenv
from datetime import timedelta, datetime
from airflow.sdk import Variable

#get apip key
load_dotenv(".env") 

# Fetch from JSearch API hosted on RapidAPI
JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HEADERS = {
    "X-RapidAPI-Key": "8ed28f0641msh48b1de0a6a1e831p124b05jsnba51e1e53b52",
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}

# fetch from rapidapi
def fetch_jsearch_jobs(
    query: str = "IT job in Singapore",
    page: int = 5,
    num_pages: int = 5,
    language: str = "en",
) -> list[dict]:
  
    params = {
        "query": query,
        "page": page,
        "num_pages": num_pages,
        "language": language,
    }

    response = requests.get(JSEARCH_URL, headers=JSEARCH_HEADERS, params=params)
    response.raise_for_status()

    data = response.json().get("data", [])
    print(f"[JSearch] Returned {len(data)} jobs.")

    return data
