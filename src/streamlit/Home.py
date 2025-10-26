import streamlit as st
import pandas as pd

from utils.db import run_query

st.set_page_config(page_title="Portfolio Overview", page_icon="📊", layout="wide")

st.title("Data Engineer Job Market in Switzerland: Zurich, Basel, Geneva and Bern")
st.subheader("Daily LinkedIn data, normalized and quality-checked to showcase ETL best practices.")
st.text("This dashboard aggregates public LinkedIn job posts for “Data Engineer” across four Swiss cities. The pipeline runs daily, standardizes key fields, and applies data-quality checks before serving analytics. This is part of my Data Engineering portfolio with a focus on data freshness and reliability.")

st.divider()
st.subheader("Main KPIs")

# --- Core timestamps (no filters in Home) ---
last_run_df = run_query("SELECT MAX(last_updated) AS last_ts FROM lk_jobs;")
last_run_ts = pd.to_datetime(last_run_df["last_ts"].iloc[0]) if not last_run_df.empty else None
last_run_lbl = last_run_ts.strftime("%Y-%m-%d") if last_run_ts is not None else "—"

# --- Totals (all history) ---
total_jobs_df = run_query("SELECT COUNT(*) AS cnt FROM lk_jobs;")
total_jobs = int(total_jobs_df["cnt"].iloc[0]) if not total_jobs_df.empty else 0

# --- Jobs ingested in the last load (exact timestamp match) ---
jobs_last_df = run_query(
    "SELECT COUNT(*) AS cnt FROM lk_jobs WHERE last_updated = :ts;",
    {"ts": last_run_ts} if last_run_ts is not None else {}
)
jobs_last_load = int(jobs_last_df["cnt"].iloc[0]) if not jobs_last_df.empty else 0

# --- Companies with roles in the last load ---
companies_df = run_query(
    """
    SELECT COUNT(DISTINCT company_id) AS companies
    FROM lk_jobs
    WHERE last_updated = :ts
      AND company_id IS NOT NULL;
    """,
    {"ts": last_run_ts} if last_run_ts is not None else {}
)
companies_last_load = int(companies_df["companies"].iloc[0]) if not companies_df.empty else 0

# --- ETL status (simple freshness rule) ---
fresh_hours = ((pd.Timestamp.now() - last_run_ts).total_seconds() / 3600) if last_run_ts is not None else None
etl_status = "OK" if (fresh_hours is not None and fresh_hours <= 48) else ("Degraded" if fresh_hours is not None else "Unknown")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Total jobs (all-time)", f"{total_jobs:,}".replace(",", " "))
with c2:
    st.metric("Jobs ingested in last load", f"{jobs_last_load:,}".replace(",", " "))
with c3:
    st.metric("Companies with roles (last load)", f"{companies_last_load:,}".replace(",", " "))
with c4:
    # Muestra solo la fecha/hora de la última carga
    st.metric("Last load", last_run_lbl, delta=f"ETL status: {etl_status}")

# Copy corto explicativo debajo de KPIs
st.caption(
    "The KPIs summarize the pipeline’s current health and coverage: "
    "total historical volume, the number of jobs ingested in the latest load, "
    "distinct companies present in that load, and the timestamp of the last successful ingestion. "
    "The pipeline runs every day at 5PM CET"
)

st.divider()
st.subheader("Mini Data Quality Panel")

dq_df = run_query(
    """
    SELECT 'job_name' AS field, SUM(j.job_name IS NOT NULL)/COUNT(*) AS completeness FROM lk_jobs j
    UNION ALL
    SELECT 'job_url',    SUM(j.job_url IS NOT NULL)/COUNT(*) FROM lk_jobs j
    UNION ALL
    SELECT 'company_id', SUM(j.company_id IS NOT NULL)/COUNT(*) FROM lk_jobs j
    UNION ALL
    SELECT 'job_description', SUM(j.job_description IS NOT NULL)/COUNT(*) FROM lk_jobs j
    """
)
if not dq_df.empty:
    dq_df["completeness"] = (dq_df["completeness"] * 100).round(1).astype(str) + "%"
    st.dataframe(dq_df.rename(columns={"field": "Field", "completeness": "Completeness"}), use_container_width=True, hide_index=True)
else:
    st.caption("No data-quality summary available yet.")