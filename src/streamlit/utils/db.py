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
