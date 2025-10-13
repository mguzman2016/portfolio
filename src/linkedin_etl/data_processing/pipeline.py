import os
import csv
from pathlib import Path
from datetime import datetime

from .files import delete_all_files, create_tmp_dir
from api.api import get_jobs, get_job_details
from database.load_tables import stage_table, dump_data_to_file, load_tables, execute_sql

TMP_DIR = Path("tmp_data")

IDS_FILE_CSV = TMP_DIR / "ids.csv"
IDS_FILE_COLUMNS = ["job_id"]

MISSING_IDS_FILE_CSV = TMP_DIR / "missing_ids.csv"

JOBS_CSV = TMP_DIR / "jobs.csv"
JOB_COLS = [
    "job_id",
    "job_name",
    "standardized_name",
    "job_url",
    "job_description",
    "job_type",
    "job_functions",
    "job_experience_level",
    "job_views",
    "company_id",
]

COMPANIES_CSV = TMP_DIR / "companies.csv"
COMPANY_COLS = [
    "company_id",
    "company_name",
    "company_image_url",
    "company_description",
    "company_staff_count",
    "company_url",
    "company_follower_count",
    "company_industries",
]

DETAIL_URL = "https://www.linkedin.com/voyager/api/jobs/jobPostings/job_id?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-65&topN=1&topNRequestedFlavors=List(TOP_APPLICANT,IN_NETWORK,COMPANY_RECRUIT,SCHOOL_RECRUIT,HIDDEN_GEM,ACTIVELY_HIRING_COMPANY)"

HEADERS = {
    'Cookie' : os.environ.get("cookie"),
    'Csrf-Token' : os.environ.get("csfrtoken")
}

def _sanitize(value):
    """Make values safe for line-based CSV loading.
    - Remove internal newlines and carriage returns.
    - Remove NULs.
    - Strip surrounding whitespace.
    - Leave quoting to csv module.
    """
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return value
    s = str(value)
    s = s.replace("\x00", "")
    s = s.replace("\r", " ").replace("\n", "<br>")
    return s.strip()

def _prep_row(raw: dict, columns: list[str]) -> dict:
    return {col: _sanitize(raw.get(col)) for col in columns}

def clean_temporary_data_directory():
    print(f"Cleaning directory: {TMP_DIR}")
    create_tmp_dir(TMP_DIR)
    delete_all_files(TMP_DIR)

def fetch_linkedin_job_ids(url):
    ids_file_writer, ids_file = get_file_handle(IDS_FILE_CSV,IDS_FILE_COLUMNS)
    print(f"Fetching jobs from URL: {url}")
    total_jobs = 0

    for jobs_page in get_jobs(url=url, headers=HEADERS):
        total_jobs = jobs_page["total_jobs"]
        job_ids = jobs_page["jobs"]
        for id_ in job_ids:
            ids_file_writer.writerow({"job_id": id_})
        ids_file.flush()

    return total_jobs

def fetch_missing_job_details():
    jobs_file_writer, jobs_file = get_file_handle(JOBS_CSV,JOB_COLS)
    companies_file_writer, companies_file = get_file_handle(COMPANIES_CSV,COMPANY_COLS)
    print("Fetching job details")
    for job_id in stream_file_lines(MISSING_IDS_FILE_CSV.absolute()):
        job_information, company_information = get_job_details(DETAIL_URL.replace('job_id',job_id), HEADERS)

        if job_information:
            jobs_file_writer.writerow(_prep_row(job_information, JOB_COLS))
            jobs_file.flush()
            
        if company_information:
            companies_file_writer.writerow(_prep_row(company_information, COMPANY_COLS))
            companies_file.flush()

def load_job_ids_into_stage_table():
    data_path = os.environ.get('CONTAINER_DATA_PATH')+"/ids.csv" or IDS_FILE_CSV.absolute()
    stage_table(data_path, "lk_staging_jobs", ",".join(IDS_FILE_COLUMNS))

def load_jobs_into_stage_table():
    data_path = os.environ.get('CONTAINER_DATA_PATH')+"/jobs.csv" or JOBS_CSV.absolute()
    stage_table(data_path, "lk_staging_job_details", ",".join(JOB_COLS))

def load_companies_into_stage_table():
    data_path = os.environ.get('CONTAINER_DATA_PATH')+"/companies.csv" or COMPANIES_CSV.absolute()
    stage_table(data_path, "lk_staging_companies", ",".join(COMPANY_COLS))

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
    with open(file_location, "r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                yield row[0]

def load_datamart_tables(etl_id):
    print("Loading tables")
    load_tables(etl_id)
    print("Finished ETL")

def fetch_etl_configs():
    sql = f"""
        SELECT
            etl_id
            , etl_url
        FROM lk_etl_status
        WHERE last_updated < CURDATE() OR last_updated IS NULL
    """
    return execute_sql(sql)

def change_etl_status_to_running(etl_id):
    sql = f"""
        UPDATE lk_etl_status
        SET is_running = true
        WHERE etl_id = {etl_id}
    """
    execute_sql(sql)

def change_etl_status_to_not_running(etl_id):
    sql = f"""
        UPDATE lk_etl_status
        SET is_running = true
        WHERE etl_id = {etl_id}
    """
    execute_sql(sql)

def change_etl_last_updated(etl_id):
    sql = f"""
        UPDATE lk_etl_status
        SET last_updated = CURDATE()
        WHERE etl_id = {etl_id}
    """
    execute_sql(sql)

def update_total_jobs(etl_id, total_jobs):
    sql = f"""
        INSERT INTO lk_search_history(etl_search_id, search_date, total_jobs)
        VALUES ({etl_id}, CURDATE(), {total_jobs})
        ON DUPLICATE KEY UPDATE
            total_jobs = {total_jobs}
    """
    execute_sql(sql)
