import pathlib
import os

from sqlalchemy import text

from models.session import engine

def execute_sql(sql_text):
    with engine.connect() as connection:
        result = connection.execute(text(sql_text))
        connection.commit()
        return result
    
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
    execute_sql(sql)

def dump_missing_job_ids_to_file():
    working_directory = pathlib.Path().resolve()
    current_path = os.environ.get('CONTAINER_DATA_PATH') or f"{working_directory}/tmp_data"
    file_path =  f"{current_path}/missing_ids.csv"
    
    sql = f"""
        SELECT lsj.job_id
        FROM lk_staging_jobs lsj 
        LEFT JOIN lk_jobs lj ON lsj.job_id = lj.job_id
        WHERE lj.job_id IS NULL
        INTO OUTFILE '{file_path}'
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n';
    """
    execute_sql(sql)