BASE_URL = "https://api.mycareersfuture.gov.sg/v2/jobs/"
import requests

def mcf_scrape(keywords, limit = None):
    params = {
        "search": keywords,
        "limit": limit
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    
    data = response.json()

    return data


# To test

print(mcf_scrape("data scientist", 5))