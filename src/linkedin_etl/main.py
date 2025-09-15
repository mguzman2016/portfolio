import os
import csv
from api.api import get_jobs
from database.load_tables import load_id_files, dump_missing_job_ids_to_file

def main():
    url = "https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-211&count=50&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:Data%20Engineer,locationUnion:(geoId:105646813),selectedFilters:(distance:List(25)),spellCorrectionEnabled:true)&start=0"
    headers = {
        'Cookie' : os.environ.get("cookie"),
        'Csrf-Token' : os.environ.get("csfrtoken")
    }

    total_jobs = 0
    with open("tmp_data/ids.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id"])
        for jobs_page in get_jobs(url=url, headers=headers):
            total_jobs = jobs_page["total_jobs"]
            job_ids = jobs_page["jobs"]
            for id_ in job_ids:
                writer.writerow([id_,1])
            f.flush()
    
    if total_jobs:
        load_id_files()
        dump_missing_job_ids_to_file()
    
    with open("tmp_data/missing_ids.csv") as infile:
        for line in infile:
            value = line.strip()
            value = value.strip('"')
            print(f"{value}...next...")

main()
