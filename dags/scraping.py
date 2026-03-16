import os
import json
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()
    
def fetch_jsearch_jobs(
    query: str = "IT job in Singapore",
    page: int = 1,
    num_pages: int = 10,
    language: str = "en",
    output_path: str = "/opt/airflow/job_search.json",
) -> list[dict]:
  
    params = {
        "query": query,
        "page": page,
        "num_pages": num_pages,
        "language": language,
    }

    JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"
    JSEARCH_HEADERS = {
        "X-RapidAPI-Key": os.getenv("JSEARCH_API_KEY"),
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    response = requests.get(JSEARCH_URL, headers=JSEARCH_HEADERS, params=params)
    response.raise_for_status()

    data = response.json().get("data", [])
    print(f"[JSearch] Returned {len(data)} jobs.")

    for entry in data:
        entry["_id"] = entry["job_id"]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4, default=str)
    print(f"[JSearch] Saved to {output_path}")

    return output_path


def mcf_scrape(keywords, limit = None):
    BASE_URL="https://api.mycareersfuture.gov.sg/v2/jobs/"
    params = {
        "search": keywords,
        "limit": limit
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    jobs = data.get("results", [])
    for entry in jobs:
        entry["_id"] = entry["uuid"]
        entry["description"] = BeautifulSoup(entry["description"], "html.parser").get_text(separator=" ")
    
    with open("/opt/airflow/mcf_data.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=4, ensure_ascii=False)

    return "/opt/airflow/mcf_data.json"