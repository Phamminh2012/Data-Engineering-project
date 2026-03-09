import requests
import time
import random

BASE_URL = "https://www.reed.co.uk/api/1.0/"

API_KEY = "c5da5dc3-6e98-40f7-8314-ce67c60fcf77"

def get_reed_listings(keywords, location = None, limit = None):
    session = requests.Session()
    session.auth = (API_KEY, '')
    params = {
        "keywords": keywords,
        "locationName": location,
        "resultsToTake": limit
    }

    RESULT = []

    results = session.get(f"{BASE_URL}/search", params=params)

    if results.status_code != 200:
        print(f"Error! {results.status_code}")
        raise Exception("Failed to fetch data from Reed API")
    
    results = results.json()["results"]

    for entry in results:
        time.sleep(random.uniform(0.5, 1.5))
        jobID = entry["jobId"]
        job_actual = session.get(f"{BASE_URL}/jobs/{jobID}")
        if job_actual.status_code != 200:
            print(f"Error! {job_actual.status_code}")
            continue
        job_actual = job_actual.json()
        RESULT.append(job_actual)
    
    return RESULT

# To test

print(get_reed_listings("data scientist", "london", 5))