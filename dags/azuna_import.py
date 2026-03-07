import os
import requests
import datetime as dt
import json
from dotenv import load_dotenv

load_dotenv()


ADZUNA_APP_ID = os.getenv("Adzuna_API_application_ID")
ADZUNA_API_KEY = os.getenv("Adzuna_API_key")


def adzuna_import():
    """
    Fetch recent job data from Adzuna API and save as JSON.

    Returns:
        str: path to the saved JSON file
    """
    if not ADZUNA_APP_ID or not ADZUNA_API_KEY:
        raise ValueError("Missing Adzuna API credentials in environment variables.")

    f_path = "/opt/airflow/data/raw/adzuna_jobs.json"
    jobs = []

    # Example: Singapore jobs, software-related
    country = "sg"
    page = 1
    results_per_page = 50
    what = "software developer OR backend OR full stack OR data engineer"

    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"

    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_API_KEY,
        "results_per_page": results_per_page,
        "what": what,
        "sort_by": "date",
        "content-type": "application/json"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    for job in data.get("results", []):
        created_raw = job.get("created")
        created_iso = None

        # Try converting Adzuna date string to ISO
        if created_raw:
            try:
                created_dt = dt.datetime.strptime(created_raw, "%Y-%m-%dT%H:%M:%SZ")
                created_iso = created_dt.isoformat()
            except ValueError:
                created_iso = created_raw

        jobs.append({
            "job_id": job.get("id"),
            "title": job.get("title", ""),
            "company": job.get("company", {}).get("display_name", ""),
            "location": job.get("location", {}).get("display_name", ""),
            "category": job.get("category", {}).get("label", ""),
            "description": job.get("description", ""),
            "redirect_url": job.get("redirect_url", ""),
            "salary_min": job.get("salary_min"),
            "salary_max": job.get("salary_max"),
            "contract_time": job.get("contract_time"),
            "contract_type": job.get("contract_type"),
            "date": created_iso,
            "source": "adzuna"
        })

    with open(f_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=4, ensure_ascii=False)

    return f_path