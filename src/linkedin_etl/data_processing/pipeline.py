import os
from pathlib import Path

from api.api import get_jobs, get_job_details
from database.load_tables import stage_table, load_tables, dump_missing_job_ids_to_file
from filesystem.file_manager import delete_all_files, create_tmp_dir, get_file_handle, stream_file_lines
from utils.data_cleaner import prep_row

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

def clean_temporary_data_directory():
    print(f"Cleaning directory: {TMP_DIR}")
    create_tmp_dir(TMP_DIR)
    delete_all_files(TMP_DIR)

def fetch_linkedin_job_ids(url):
    ids_file_writer, ids_file = get_file_handle(IDS_FILE_CSV,IDS_FILE_COLUMNS)
    print(f"Fetching jobs from URL: {url}")
    total_jobs = 0

    for jobs_page in get_jobs(url=url):
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
        job_information, company_information = get_job_details(job_id)

        if job_information:
            jobs_file_writer.writerow(prep_row(job_information, JOB_COLS))
            jobs_file.flush()
            
        if company_information:
            companies_file_writer.writerow(prep_row(company_information, COMPANY_COLS))
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

def save_missing_job_ids_to_file():
    data_path = os.environ.get('CONTAINER_DATA_PATH')+"/missing_ids.csv" or MISSING_IDS_FILE_CSV.absolute()
    dump_missing_job_ids_to_file(data_path)

def load_datamart_tables(etl_id):
    print("Loading tables")
    load_tables(etl_id)
    print("Finished ETL")

