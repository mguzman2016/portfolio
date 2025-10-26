import datetime as dt
import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(page_title="Search Trends", page_icon="📈", layout="wide")

with st.sidebar:
    st.header("Filters")

    end_d = dt.date.today()
    start_d = end_d - dt.timedelta(days=30)
    start_date, end_date = st.date_input("Date range", (start_d, end_d))

    seniority = st.multiselect("Experience level", ["Intern","Junior","Mid","Senior","Lead","Manager"], default=[])

