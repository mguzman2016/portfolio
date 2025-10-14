from database.load_tables import execute_sql

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
