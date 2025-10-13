import pathlib
import os

from typing import Iterable, Optional

from sqlalchemy import text

from models.session import engine

def execute_sql(sql: str, dry_run: bool = False):
    if dry_run:
        print("\n-- DRY RUN --")
        print(sql)
        return None
    with engine.begin() as conn:
        return conn.execute(text(sql))

def execute_many(sql_statements: Iterable[str], *, dry_run: bool = False):
    for s in sql_statements:
        execute_sql(s, dry_run=dry_run) 

def stage_table(data_path, table_name, fields):
    clean_sql_table = f"TRUNCATE TABLE {table_name}"
    sql = f"""
        LOAD DATA INFILE '{data_path}'
        IGNORE
        INTO TABLE {table_name} 
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        ({fields});
    """
    execute_many(sql_statements=[clean_sql_table,sql], dry_run=True)
    execute_many(sql_statements=[clean_sql_table,sql])

def dump_data_to_file(data_path, sql):
    sql = f"""
        {sql}
        INTO OUTFILE '{data_path}'
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n';
    """
    execute_sql(sql, True)
    execute_sql(sql)
 
def load_id_files():
    working_directory = pathlib.Path().resolve()
    current_path = os.environ.get('CONTAINER_DATA_PATH') or f"{working_directory}/tmp_data"
    file_path =  f"{current_path}/ids.csv"
    sql = f"""
        LOAD DATA INFILE '{file_path}'
        IGNORE
        INTO TABLE lk_staging_jobs 
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (job_id, etl_id);
    """
    print(sql)
    execute_sql(sql)

def dump_missing_job_ids_to_file():
    working_directory = pathlib.Path().resolve()
    current_path = os.environ.get('CONTAINER_DATA_PATH') or f"{working_directory}/tmp_data"
    file_path =  f"{current_path}/missing_ids.csv"
    sql = f"""
        SELECT DISTINCT lsj.job_id
        FROM lk_staging_jobs lsj 
        LEFT JOIN lk_jobs lj ON lsj.job_id = lj.job_id
        WHERE lj.job_id IS NULL
        INTO OUTFILE '{file_path}'
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n';
    """
    execute_sql(sql)

def stage_jobs_file():
    working_directory = pathlib.Path().resolve()
    current_path = os.environ.get('CONTAINER_DATA_PATH') or f"{working_directory}/tmp_data"
    file_path =  f"{current_path}/jobs.csv"
    truncate_sql = "TRUNCATE TABLE lk_staging_job_details;"
    sql = f"""
        LOAD DATA INFILE '{file_path}'
        IGNORE
        INTO TABLE lk_staging_job_details
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (job_id,job_name,standardized_name,job_url,job_description,job_type,job_functions,job_experience_level,job_views,company_id);
    """
    execute_sql(truncate_sql)
    execute_sql(sql)

def stage_companies_file():
    working_directory = pathlib.Path().resolve()
    current_path = os.environ.get('CONTAINER_DATA_PATH') or f"{working_directory}/tmp_data"
    file_path =  f"{current_path}/companies.csv"
    truncate_sql = "TRUNCATE TABLE lk_staging_companies;"
    sql = f"""
        LOAD DATA INFILE '{file_path}'
        IGNORE
        INTO TABLE lk_staging_companies
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (company_id,company_name,company_image_url,company_description,company_staff_count,company_url,company_follower_count,company_industries);
    """
    execute_sql(truncate_sql)
    execute_sql(sql)

def load_tables(etl_id):
    load_jobs_sql = f"""
        INSERT INTO lk_jobs (
            job_id,
            job_name,
            standardized_name,
            job_url,
            job_description,
            job_type,
            job_min_salary,
            job_max_salary,
            job_pay_period,
            job_functions,
            job_experience_level,
            job_views,
            job_lang,
            etl_id,
            company_id,
            last_updated
        )
        SELECT DISTINCT
            job_id,
            job_name,
            standardized_name,
            job_url,
            job_description,
            job_type,
            job_min_salary,
            job_max_salary,
            job_pay_period,
            job_functions,
            job_experience_level,
            job_views,
            NULL AS job_lang,
            {etl_id} AS etl_id,
            company_id,
            DATE(CURRENT_DATE()) AS last_updated
        FROM lk_staging_job_details lsjd
        ON DUPLICATE KEY UPDATE
            job_name = VALUES(job_name),
            standardized_name = VALUES(standardized_name),
            job_url = VALUES(job_url),
            job_description = VALUES(job_description),
            job_type = VALUES(job_type),
            job_min_salary = VALUES(job_min_salary),
            job_max_salary = VALUES(job_max_salary),
            job_pay_period = VALUES(job_pay_period),
            job_functions = VALUES(job_functions),
            job_experience_level = VALUES(job_experience_level),
            job_views = VALUES(job_views),
            job_lang = VALUES(job_lang),
            etl_id = VALUES(etl_id),
            company_id = VALUES(company_id),
            last_updated = VALUES(last_updated);
    """

    load_companies_sql = f"""
        INSERT INTO lk_companies (
            company_id,
            company_name,
            company_image_url,
            company_description,
            company_staff_count,
            company_url,
            company_follower_count,
            company_industries
        )
        SELECT DISTINCT
            company_id,
            company_name,
            company_image_url,
            company_description,
            company_staff_count,
            company_url,
            company_follower_count,
            company_industries
        FROM lk_staging_companies lsc
        ON DUPLICATE KEY UPDATE
            company_name = VALUES(company_name),
            company_image_url = VALUES(company_image_url),
            company_description = VALUES(company_description),
            company_staff_count = VALUES(company_staff_count),
            company_url = VALUES(company_url),
            company_follower_count = VALUES(company_follower_count),
            company_industries = VALUES(company_industries);
    """
    execute_sql(load_jobs_sql)
    execute_sql(load_companies_sql)