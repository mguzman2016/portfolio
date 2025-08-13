import os
import csv
from pathlib import Path

from .files import delete_all_files, create_tmp_dir
from api.api import get_jobs, get_job_details
from database.load_tables import stage_table, dump_data_to_file

TMP_DIR = Path("tmp_data")

IDS_FILE_CSV = TMP_DIR / "ids.csv"
IDS_FILE_COLUMNS = ["id"]

MISSING_IDS_FILE_CSV = TMP_DIR / "missing_ids.csv"

URL = "https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-211&count=50&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:Data%20Engineer,locationUnion:(geoId:105646813),selectedFilters:(distance:List(25)),spellCorrectionEnabled:true)&start=0"
DETAIL_URL = "https://www.linkedin.com/voyager/api/jobs/jobPostings/job_id?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-65&topN=1&topNRequestedFlavors=List(TOP_APPLICANT,IN_NETWORK,COMPANY_RECRUIT,SCHOOL_RECRUIT,HIDDEN_GEM,ACTIVELY_HIRING_COMPANY)"

HEADERS = {
    'Cookie' : os.environ.get("cookie"),
    'Csrf-Token' : os.environ.get("csfrtoken")
}

def clean_temporary_data_directory():
    print(f"Cleaning directory: {TMP_DIR}")
    create_tmp_dir(TMP_DIR)
    delete_all_files(TMP_DIR)

def fetch_linkedin_job_ids():
    ids_file_writer, ids_file = get_file_handle(IDS_FILE_CSV,IDS_FILE_COLUMNS)
    print(f"Fetching jobs from URL: {URL}")
    total_jobs = 0

    for jobs_page in get_jobs(url=URL, headers=HEADERS):
        total_jobs = jobs_page["total_jobs"]
        job_ids = jobs_page["jobs"]
        for id_ in job_ids:
            print(f"Writing {id_}")
            ids_file_writer.writerow({"id": id_, "etl_id": 1})
        ids_file.flush()

    return total_jobs

def load_job_ids_into_stage_table():
    data_path = os.environ.get('CONTAINER_DATA_PATH')+"/ids.csv" or IDS_FILE_CSV.absolute()
    table_fields = ["job_id"]
    stage_table(data_path, "lk_staging_jobs", ",".join(table_fields))

def dump_missing_job_ids_to_file():
    data_path = os.environ.get('CONTAINER_DATA_PATH')+"/missing_ids.csv" or MISSING_IDS_FILE_CSV.absolute()
    sql = """SELECT lsj.job_id
        FROM lk_staging_jobs lsj 
        LEFT JOIN lk_jobs lj ON lsj.job_id = lj.job_id
        WHERE lj.job_id IS NULL"""
    dump_data_to_file(data_path, sql)

def get_file_handle(file_location, columns):
    file = open(file_location, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(
            file,
            fieldnames=columns,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            escapechar='\\'
    )
    writer.writeheader()
    file.flush()
    return (writer, file)

def stream_file_lines(file_location):
    with open(file_location, "r", encoding="utf-8") as file:
        for line in file:
            yield line.strip()
