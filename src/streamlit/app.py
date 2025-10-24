import streamlit as st
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

@st.cache_resource
def get_engine() -> Engine:
    ms = st.secrets["mysql"]
    url = URL.create(
        drivername="mysql+pymysql",
        username=ms["user"],
        password=ms["password"],
        host=ms["host"],
        port=int(ms.get("port", 3306)),
        database=ms["database"],
        query={"charset": "utf8mb4"}
    )
    return create_engine(
        url,
        pool_size=5,
        max_overflow=5,
        pool_pre_ping=True,
        pool_recycle=1800
    )

@st.cache_data(ttl=600)
def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as con:
        return pd.read_sql(text(sql), con=con, params=params or {})

def execute(sql: str, params: dict = None) -> int:
    eng = get_engine()
    with eng.begin() as con:
        res = con.execute(text(sql), params or {})
        return res.rowcount

st.set_page_config(page_title="Portfolio Overview", page_icon="📊", layout="wide")

st.title("Data Engineer Job Market in Switzerland: Zurich, Basel, Geneva and Bern")
st.subheader("Daily LinkedIn data, normalized and quality-checked to showcase ETL best practices.")
st.text("This dashboard aggregates public LinkedIn job posts for “Data Engineer” across four Swiss cities. The pipeline runs daily, standardizes key fields, and applies data-quality checks before serving analytics. This is part of my Data Engineering portfolio with a focus on data freshness and reliability.")

st.divider()
st.subheader("Main KPIs")

last_updated = run_query("SELECT DATE(MAX(last_updated)) as updated_at FROM lk_jobs")['updated_at'][0]
total_jobs = run_query(f"SELECT COUNT(*) AS cnt FROM lk_jobs")['cnt'][0]
total_jobs_last_run = run_query(f"SELECT COUNT(*) AS cnt FROM lk_jobs WHERE last_updated = '{last_updated}'")['cnt'][0]

st.text(last_updated)

c1, c2, c3, c4, c5, c6 = st.columns(6)
with c1:
    st.metric("Total jobs (range)", f"{total_jobs}".replace(",", " "))
with c2:
    st.metric("New in since last run", f"{total_jobs_last_run}".replace(",", " "))
with c3:
    st.metric("% with salary", f"{100}%")
with c4:
    st.metric("Median salary (CHF/y)", f"{1000}")
with c5:
    st.metric("Active companies", f"{100}")
with c6:
    last_lbl = "ETL Status"
    st.metric("Last load date", "OK", delta=f"Last load: {last_updated}")
