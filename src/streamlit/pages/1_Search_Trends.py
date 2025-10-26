import datetime as dt
import pandas as pd
import streamlit as st
import altair as alt
from utils.db import run_query

st.set_page_config(page_title="Search Trends", page_icon="📈", layout="wide")

# ---------- Options ----------
cities_opts = (
    run_query("SELECT DISTINCT city FROM lk_etl_status")["city"]
    .dropna().astype(str).str.strip().replace("", pd.NA).dropna().drop_duplicates().tolist()
)
experience_opts = (
    run_query("SELECT DISTINCT job_experience_level FROM lk_jobs")["job_experience_level"]
    .dropna().astype(str).str.strip().replace("", pd.NA).dropna().drop_duplicates().tolist()
)

# ---------- Defaults ----------
end_d_default = dt.date.today()
start_d_default = end_d_default - dt.timedelta(days=30)

def set_default_widget_state():
    st.session_state.setdefault("date_range_input", (start_d_default, end_d_default))
    st.session_state.setdefault("cities_sel_input", cities_opts[:])
    st.session_state.setdefault("seniority_sel_input", [])

# --- HANDLE RESET FLAG *BEFORE* CREATING WIDGETS ---
if st.session_state.get("reset_filters", False):
    # borra keys de widgets para que tomen defaults en este run
    for k in ("date_range_input", "cities_sel_input", "seniority_sel_input"):
        st.session_state.pop(k, None)
    set_default_widget_state()
    st.session_state["reset_filters"] = False
else:
    set_default_widget_state()

# ---------- Helpers ----------
def normalize_date_range(val, default_start, default_end):
    if isinstance(val, (list, tuple)):
        if len(val) == 2 and val[0] and val[1]:
            a, b = val
            return (min(a, b), max(a, b))
        if len(val) == 1 and val[0]:
            return (val[0], val[0])
    if isinstance(val, dt.date):
        return (val, val)
    return (default_start, default_end)

def expand_in(sql: str, name: str, values: list[str]):
    if not values:
        return sql, {}
    placeholders = ", ".join(f":{name}{i}" for i in range(len(values)))
    return sql.replace(f":{name}", f"({placeholders})"), {f"{name}{i}": v for i, v in enumerate(values)}

# ---------- Filter Bar (form) ----------
with st.form("filters_form"):
    c1, c2, c3 = st.columns(3)
    c1.date_input("Date range", key="date_range_input")
    c2.multiselect("Cities", options=cities_opts, key="cities_sel_input")
    c3.multiselect("Experience level", options=experience_opts, key="seniority_sel_input")

    left, right = st.columns([1, 1])
    apply_clicked = left.form_submit_button("Apply")
    reset_clicked = right.form_submit_button("Reset")

    if reset_clicked:
        # solo marcamos el flag y relanzamos; los defaults se aplican al principio del script
        st.session_state["reset_filters"] = True
        st.rerun()

# ---------- Read normalized filters ----------
start_date, end_date = normalize_date_range(
    st.session_state["date_range_input"], start_d_default, end_d_default
)
cities_sel = st.session_state["cities_sel_input"]
seniority_sel = st.session_state["seniority_sel_input"]

# ---------- Derive timestamps ----------
start_ts = dt.datetime.combine(start_date, dt.time.min)
end_ts   = dt.datetime.combine(end_date, dt.time.max)

# --- New vs Updated: GLOBAL (country = Switzerland), no city facet ---
def expand_in(sql: str, name: str, values: list[str]):
    if not values:
        return sql, {}
    placeholders = ", ".join(f":{name}{i}" for i in range(len(values)))
    return sql.replace(f":{name}", f"({placeholders})"), {f"{name}{i}": v for i, v in enumerate(values)}

base_sql_daily = """
SELECT DATE(j.last_updated) AS d, s.city, COUNT(*) AS total_jobs
FROM lk_jobs j
LEFT JOIN lk_etl_status s ON s.etl_id = j.etl_id
WHERE j.last_updated BETWEEN :start_ts AND :end_ts
  {city_clause}
  {sen_clause}
GROUP BY DATE(j.last_updated), s.city
ORDER BY d, s.city;
"""

params = {"start_ts": start_ts, "end_ts": end_ts}
city_clause = ""
sen_clause = ""

sql = base_sql_daily
if cities_sel:
    city_clause = "AND s.city IN :cities"
    sql, b1 = expand_in(sql.replace("{city_clause}", city_clause), "cities", cities_sel)
    params.update(b1)
else:
    sql = sql.replace("{city_clause}", "")

if seniority_sel:
    sen_clause = "AND j.job_experience_level IN :seniority"
    sql, b2 = expand_in(sql.replace("{sen_clause}", sen_clause), "seniority", seniority_sel)
    params.update(b2)
else:
    sql = sql.replace("{sen_clause}", "")

daily_df = run_query(sql, params)

st.subheader("Daily series by city")
if not daily_df.empty:
    daily_df["d"] = pd.to_datetime(daily_df["d"])
    if "city" not in daily_df.columns:
        daily_df["city"] = "All"
    chart = (
        alt.Chart(daily_df)
        .mark_line()
        .encode(
            x=alt.X("d:T", title="Date"),
            y=alt.Y("total_jobs:Q", title="Jobs/day"),
            color=alt.Color("city:N", title="City"),
            tooltip=["d:T","city:N","total_jobs:Q"],
        ).properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.caption("No data for the selected range/filters.")
st.divider()

sql_heat = """
SELECT DAYOFWEEK(j.last_updated) AS weekday, s.city, COUNT(*) AS total_jobs
FROM lk_jobs j
LEFT JOIN lk_etl_status s ON s.etl_id = j.etl_id
WHERE j.last_updated BETWEEN :start_ts AND :end_ts
  {city_clause}
  {sen_clause}
GROUP BY weekday, s.city
ORDER BY weekday, s.city;
"""
sql2 = sql_heat
params2 = {"start_ts": start_ts, "end_ts": end_ts}

if cities_sel:
    sql2, b1h = expand_in(sql2.replace("{city_clause}", "AND s.city IN :cities"), "cities", cities_sel)
    params2.update(b1h)
else:
    sql2 = sql2.replace("{city_clause}", "")
if seniority_sel:
    sql2, b2h = expand_in(sql2.replace("{sen_clause}", "AND j.job_experience_level IN :seniority"), "seniority", seniority_sel)
    params2.update(b2h)
else:
    sql2 = sql2.replace("{sen_clause}", "")

heat_df = run_query(sql2, params2)

st.subheader("Weekday heatmap")
if not heat_df.empty:
    weekday_map = {1:"Sun",2:"Mon",3:"Tue",4:"Wed",5:"Thu",6:"Fri",7:"Sat"}
    order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    heat_df["weekday_name"] = pd.Categorical(heat_df["weekday"].map(weekday_map), categories=order, ordered=True)
    chart2 = (
        alt.Chart(heat_df)
        .mark_rect()
        .encode(
            x=alt.X("weekday_name:N", title="Weekday", sort=order),
            y=alt.Y("city:N", title="City"),
            color=alt.Color("total_jobs:Q", title="Jobs"),
            tooltip=["city:N","weekday_name:N","total_jobs:Q"],
        ).properties(height=220)
    )
    st.altair_chart(chart2, use_container_width=True)
else:
    st.caption("No data to build the heatmap.")
st.divider()

sql_nvu_global = """
WITH first_seen AS (
  SELECT j2.job_url, MIN(DATE(j2.last_updated)) AS first_seen_date
  FROM lk_jobs j2
  GROUP BY j2.job_url
)
SELECT
  DATE(j.last_updated) AS d,
  SUM(CASE WHEN fs.first_seen_date = DATE(j.last_updated) THEN 1 ELSE 0 END) AS new_jobs,
  SUM(CASE WHEN fs.first_seen_date < DATE(j.last_updated) THEN 1 ELSE 0 END) AS updated_jobs
FROM lk_jobs j
JOIN first_seen fs ON fs.job_url = j.job_url
LEFT JOIN lk_etl_status s ON s.etl_id = j.etl_id
WHERE j.last_updated BETWEEN :start_ts AND :end_ts
  AND s.country = :country
  {city_clause}
  {sen_clause}
GROUP BY DATE(j.last_updated)
ORDER BY d;
"""

params3 = {
    "start_ts": start_ts,
    "end_ts": end_ts,
    "country": "Switzerland",
}

sql3 = sql_nvu_global

# filtros opcionales (NO afectan al grouping)
if cities_sel:
    sql3 = sql3.replace("{city_clause}", "AND s.city IN :cities")
    sql3, b_c = expand_in(sql3, "cities", cities_sel)
    params3.update(b_c)
else:
    sql3 = sql3.replace("{city_clause}", "")

if seniority_sel:
    sql3 = sql3.replace("{sen_clause}", "AND j.job_experience_level IN :seniority")
    sql3, b_s = expand_in(sql3, "seniority", seniority_sel)
    params3.update(b_s)
else:
    sql3 = sql3.replace("{sen_clause}", "")

nvu_df = run_query(sql3, params3)

st.subheader("New vs. updated (Switzerland)")
if not nvu_df.empty:
    nvu_long = nvu_df.melt(
        id_vars=["d"],
        value_vars=["new_jobs", "updated_jobs"],
        var_name="kind",
        value_name="count",
    )
    nvu_long["d"] = pd.to_datetime(nvu_long["d"])
    chart3 = (
        alt.Chart(nvu_long)
        .mark_bar()
        .encode(
            x=alt.X("d:T", title="Date"),
            y=alt.Y("count:Q", title="Jobs"),
            color=alt.Color("kind:N", title="Type"),
            tooltip=["d:T","kind:N","count:Q"],
        )
        .properties(height=260)
    )
    st.altair_chart(chart3, use_container_width=True)
else:
    st.caption("No data to split new vs. updated for Switzerland.")
