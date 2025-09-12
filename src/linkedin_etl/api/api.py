import time
import requests
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

def get_request(url, headers, delay=1):
    if delay:
        time.sleep(1)
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def add_parameters(url: str, count: int = 50, start: int = 0):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    query["count"] = [str(count)]
    query["start"] = [str(start)]

    new_query = urlencode(query, doseq=True, safe=":,()")

    return urlunparse(parsed._replace(query=new_query))

def get_jobs(url: str, headers: dict):
        
        url = add_parameters(url)

        get_job_id = lambda job:   job.get("jobCardUnion",{}).\
                                    get("jobPostingCard",{}).\
                                    get("preDashNormalizedJobPostingUrn","").\
                                    split(":")[-1]
        
        start = 0
        step = 50

        while True:
            print(f"Fetching jobs, current pagination: {start}")
            job_response = get_request(url, headers=headers)
            if not job_response["elements"]:
                break

            total_jobs = job_response.get("paging",{}).\
                                        get("total",0)

            elements = job_response["elements"]
            
            yield {"total_jobs": int(total_jobs), "jobs": [int(get_job_id(element)) for element in elements if get_job_id(element)]}
            
            start += step
            url = add_parameters(url=url, start=start)