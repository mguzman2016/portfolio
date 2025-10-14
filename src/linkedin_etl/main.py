
from data_processing.pipeline import clean_temporary_data_directory, fetch_linkedin_job_ids, load_job_ids_into_stage_table, save_missing_job_ids_to_file, fetch_missing_job_details, load_companies_into_stage_table, load_jobs_into_stage_table, load_datamart_tables
from database.etl_status_manager import fetch_etl_configs, change_etl_status_to_running, change_etl_last_updated, change_etl_status_to_not_running, update_total_jobs

def main():
    for etl_config in fetch_etl_configs():
        etl_id, url = etl_config
        change_etl_status_to_running(etl_id)
        clean_temporary_data_directory()
        total_jobs_found = fetch_linkedin_job_ids(url)
        if total_jobs_found:
            update_total_jobs(etl_id, total_jobs_found)
            load_job_ids_into_stage_table()
            save_missing_job_ids_to_file()
            fetch_missing_job_details()
            load_jobs_into_stage_table()
            load_companies_into_stage_table()
            load_datamart_tables(etl_id)
        change_etl_last_updated(etl_id)
        change_etl_status_to_not_running(etl_id)

main()
