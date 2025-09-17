import os
import csv
from pathlib import Path

from api.api import get_jobs, get_job_details
from database.load_tables import load_id_files, dump_missing_job_ids_to_file

TMP_DIR = Path("tmp_data")
TMP_DIR.mkdir(parents=True, exist_ok=True)

JOBS_CSV = TMP_DIR / "jobs.csv"
COMPANIES_CSV = TMP_DIR / "companies.csv"

# Columns (order matters for LOAD DATA INFILE)
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

def _is_empty(path: Path) -> bool:
    return (not path.exists()) or path.stat().st_size == 0

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

def _open_writer(path: Path, fieldnames: list[str]) -> csv.DictWriter:
    # newline="" is crucial to avoid blank lines on Windows
    f = open(path, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(
        f,
        fieldnames=fieldnames,
        extrasaction="ignore",
        quoting=csv.QUOTE_ALL,           # ensures ENCLOSED BY '"' works
        lineterminator="\n",
        escapechar='\\'                  # if ever needed, MySQL also accepts ESCAPED BY '\'
    )
    if _is_empty(path):
        writer.writeheader()
        f.flush()
    return writer

def delete_file_if_exists(path: Path):
    path.unlink(missing_ok=True)

def main():
    url = "https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-211&count=50&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_SEARCH_BUTTON,keywords:Data%20Engineer,locationUnion:(geoId:105646813),selectedFilters:(distance:List(25)),spellCorrectionEnabled:true)&start=0"
    detail_url = "https://www.linkedin.com/voyager/api/jobs/jobPostings/job_id?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-65&topN=1&topNRequestedFlavors=List(TOP_APPLICANT,IN_NETWORK,COMPANY_RECRUIT,SCHOOL_RECRUIT,HIDDEN_GEM,ACTIVELY_HIRING_COMPANY)"

    headers = {
        'Cookie' : os.environ.get("cookie"),
        'Csrf-Token' : os.environ.get("csfrtoken")
    }

    total_jobs = 1
    # with open("tmp_data/ids.csv", "w", newline="", encoding="utf-8") as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["id"])
    #     for jobs_page in get_jobs(url=url, headers=headers):
    #         total_jobs = jobs_page["total_jobs"]
    #         job_ids = jobs_page["jobs"]
    #         for id_ in job_ids:
    #             writer.writerow([id_,1])
    #         f.flush()
    
    if total_jobs:
        # load_id_files()
        # dump_missing_job_ids_to_file()

        delete_file_if_exists(JOBS_CSV)
        delete_file_if_exists(COMPANIES_CSV)

        jobs_file = open(JOBS_CSV, "a", encoding="utf-8", newline="")
        companies_file = open(COMPANIES_CSV, "a", encoding="utf-8", newline="")

        jobs_writer = csv.DictWriter(
            jobs_file,
            fieldnames=JOB_COLS,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            escapechar='\\'
        )

        jobs_writer.writeheader()
        jobs_file.flush()

        companies_writer = csv.DictWriter(
            companies_file,
            fieldnames=COMPANY_COLS,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            escapechar='\\'
        )

        companies_writer.writeheader()
        companies_file.flush()
    
    with open("tmp_data/missing_ids.csv") as infile:
        for line in infile:
            value = line.strip().strip('"')

            if not value:
                continue

            job_information, company_information = get_job_details(detail_url.replace('job_id',value), headers)
            
            jobs_writer.writerow(_prep_row(job_information, JOB_COLS))
            companies_writer.writerow(_prep_row(company_information, COMPANY_COLS))

            jobs_file.flush()
            companies_file.flush()

main()
