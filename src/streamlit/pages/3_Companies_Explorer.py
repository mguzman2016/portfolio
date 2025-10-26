import datetime as dt
import re
import pandas as pd
import streamlit as st
from utils.db import run_query

st.set_page_config(page_title="Companies Explorer", page_icon="🏢", layout="wide")
st.title("Companies Explorer")
st.caption("Top companies by open roles (in the selected period), with followers and industries. Click a card to drill down to its jobs.")

# -------------------- Helpers --------------------
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

def parse_followers_val(s: str | None) -> int | None:
    if s is None:
        return None
    s = str(s).strip().lower()
    if not s:
        return None
    # elimina comas/espacios
    s_clean = re.sub(r"[,\s]", "", s)
    # soporta 12k / 1.2m / 12000
    m = re.match(r"^(\d+(\.\d+)?)([km])?$", s_clean)
    if m:
        num = float(m.group(1))
        suf = m.group(3)
        if suf == "k":
            num *= 1_000
        elif suf == "m":
            num *= 1_000_000
        return int(num)
    # fallback: intenta extraer dígitos
    digits = re.sub(r"\D", "", s_clean)
    return int(digits) if digits else None

def format_followers(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}k"
    return f"{n}"

def short_industries(txt: str | None, max_items: int = 2) -> str:
    if not txt:
        return "—"
    parts = [p.strip() for p in str(txt).split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return "—"
    show = parts[:max_items]
    more = len(parts) - len(show)
    return ", ".join(show) + (f" (+{more})" if more > 0 else "")

# -------------------- Options (from DB) --------------------
cities_opts = (
    run_query("SELECT DISTINCT city FROM lk_etl_status")["city"]
    .dropna().astype(str).str.strip().replace("", pd.NA).dropna().drop_duplicates().tolist()
)

# -------------------- Defaults & state --------------------
END_DEF = dt.date.today()
START_DEF = END_DEF - dt.timedelta(days=30)
TOP_N_DEFAULT = 24
st.session_state.setdefault("ce_top_n", TOP_N_DEFAULT)
st.session_state.setdefault("ce_selected_company_id", None)

def _seed_company_widget_defaults():
    st.session_state.setdefault("ce_date_range", (START_DEF, END_DEF))
    st.session_state.setdefault("ce_cities", cities_opts[:])
    st.session_state.setdefault("ce_stdname", "")   # <-- NUEVO

if st.session_state.get("ce_do_reset", False):
    for k in ("ce_date_range", "ce_cities", "ce_stdname"):  # <-- incluye stdname
        st.session_state.pop(k, None)
    st.session_state["ce_top_n"] = TOP_N_DEFAULT
    st.session_state["ce_selected_company_id"] = None
    _seed_company_widget_defaults()
    st.session_state["ce_do_reset"] = False
else:
    _seed_company_widget_defaults()

# -------------------- Filter bar (form) --------------------
with st.form("ce_filters"):
    c0, c1, c2, c3 = st.columns([2, 2, 2, 1])
    c0.text_input("Standardized name contains", key="ce_stdname", placeholder="e.g., data engineer")  # <-- NUEVO
    c1.date_input("Date range (last_updated)", key="ce_date_range")
    c2.multiselect("Cities", options=cities_opts, key="ce_cities")
    new_top_n = c3.selectbox("Cards to show", options=[12, 24, 36, 48],
                             index=[12,24,36,48].index(st.session_state["ce_top_n"]))

    b1, b2 = st.columns([1,1])
    apply_clicked = b1.form_submit_button("Apply")
    reset_clicked = b2.form_submit_button("Reset")

    if reset_clicked:
        st.session_state["ce_do_reset"] = True
        st.rerun()
    else:
        if new_top_n != st.session_state["ce_top_n"]:
            st.session_state["ce_top_n"] = new_top_n
            st.rerun()

# -------------------- Leer filtros ya normalizados --------------------
start_d, end_d = normalize_date_range(st.session_state["ce_date_range"], START_DEF, END_DEF)
start_ts = dt.datetime.combine(start_d, dt.time.min)
end_ts   = dt.datetime.combine(end_d, dt.time.max)
cities_sel = st.session_state["ce_cities"]
std_q = st.session_state["ce_stdname"].strip().lower()   # <-- NUEVO
TOP_N = st.session_state["ce_top_n"]

# -------------------- Query: Top companies --------------------
base_sql = f"""
SELECT
  c.company_id,
  c.company_name,
  c.company_url,
  c.company_image_url,
  c.company_follower_count,
  c.company_industries,
  COUNT(*) AS vacancies
FROM lk_jobs j
JOIN lk_companies c ON c.company_id = j.company_id
LEFT JOIN lk_etl_status s ON s.etl_id = j.etl_id
WHERE j.last_updated BETWEEN :start_ts AND :end_ts
  {{city_clause}}
  {{std_clause}}
GROUP BY
  c.company_id, c.company_name, c.company_url, c.company_image_url,
  c.company_follower_count, c.company_industries
ORDER BY vacancies DESC, c.company_name ASC
LIMIT :limit
"""
params = {"start_ts": start_ts, "end_ts": end_ts, "limit": int(TOP_N)}
sql = base_sql
if cities_sel:
    sql = sql.replace("{city_clause}", "AND s.city IN :cities")
    sql, bind = expand_in(sql, "cities", cities_sel)
    params.update(bind)
else:
    sql = sql.replace("{city_clause}", "")

if std_q:
    sql = sql.replace("{std_clause}", "AND LOWER(j.standardized_name) LIKE :std_q")
    params["std_q"] = f"%{std_q}%"
else:
    sql = sql.replace("{std_clause}", "")

top_df = run_query(sql, params)

# Parse followers to numeric for display/sorting consistency
if not top_df.empty:
    top_df["followers_num"] = top_df["company_follower_count"].apply(parse_followers_val)
else:
    top_df["followers_num"] = pd.Series(dtype="float64")

# -------------------- Render: Cards grid --------------------
st.subheader("Top companies (by vacancies)")
if top_df.empty:
    st.info("No companies found for the selected period/filters.")
else:
    cols = st.columns(4)
    for i, row in top_df.iterrows():
        col = cols[int(i) % 4]
        with col:
            box = st.container(border=True)
            with box:
                if pd.notna(row.get("company_image_url")) and str(row["company_image_url"]).strip():
                    st.image(str(row["company_image_url"]), use_container_width=True)
                name = row.get("company_name") or "(Unnamed company)"
                url = (row.get("company_url") or "").strip()
                if url:
                    st.markdown(f"### [{name}]({url})", help="Open company website")
                else:
                    st.markdown(f"### {name}")
                st.write(f"**Vacancies:** {int(row['vacancies']):,}".replace(",", " "))
                st.write(f"**Followers:** {format_followers(row['followers_num'])}")
                st.write(f"**Industries:** {short_industries(row.get('company_industries'))}")
                # Drill-down button
                if st.button("View jobs", key=f"view_jobs_{int(row['company_id'])}"):
                    st.session_state["ce_selected_company_id"] = int(row["company_id"])
                    st.rerun()

st.divider()

# -------------------- Drill-down: jobs of selected company --------------------
company_id = st.session_state.get("ce_selected_company_id")
if company_id is not None:
    # Get company name for header
    nm_df = top_df[top_df["company_id"] == company_id]
    comp_name = nm_df["company_name"].iloc[0] if not nm_df.empty else f""

    st.subheader(f"Jobs at {comp_name}")
    # Query jobs for the selected company (same date range + cities)
    jobs_sql = f"""
    SELECT
      j.job_name,
      j.standardized_name,
      c.company_name,
      j.job_type,
      j.job_views,
      j.job_experience_level,
      j.created_at,
      s.city,
      j.last_updated,
      j.job_url
    FROM lk_jobs j
    JOIN lk_companies c ON c.company_id = j.company_id
    LEFT JOIN lk_etl_status s ON s.etl_id = j.etl_id
    WHERE j.last_updated BETWEEN :start_ts AND :end_ts
      AND j.company_id = :cid
      {{city_clause}}
      {{std_clause}}
    ORDER BY j.last_updated DESC, j.job_id DESC
    LIMIT 2000
    """
    params_jobs = {"start_ts": start_ts, "end_ts": end_ts, "cid": int(company_id)}
    if cities_sel:
        jobs_sql = jobs_sql.replace("{city_clause}", "AND s.city IN :cities")
        jobs_sql, bind2 = expand_in(jobs_sql, "cities", cities_sel)
        params_jobs.update(bind2)
    else:
        jobs_sql = jobs_sql.replace("{city_clause}", "")

    if std_q:
        jobs_sql = jobs_sql.replace("{std_clause}", "AND LOWER(j.standardized_name) LIKE :std_q")
        params_jobs["std_q"] = f"%{std_q}%"
    else:
        jobs_sql = jobs_sql.replace("{std_clause}", "")

    jobs_df = run_query(jobs_sql, params_jobs)

    if jobs_df.empty:
        st.info("No jobs for this company under the selected filters.")
    else:
        # Tidy types for display
        for col in ["created_at", "last_updated"]:
            if col in jobs_df.columns:
                jobs_df[col] = pd.to_datetime(jobs_df[col], errors="coerce")

        jobs_display = jobs_df[[
            "job_name", "job_url", "standardized_name", "company_name",
            "job_type", "job_views", "job_experience_level",
            "created_at", "city", "last_updated"
        ]].rename(columns={
            "job_name": "Job title",
            "job_url": "Job link",
            "standardized_name": "Standardized name",
            "company_name": "Company",
            "job_type": "Type",
            "job_views": "Views",
            "job_experience_level": "Experience",
            "created_at": "Created at",
            "city": "City",
            "last_updated": "Last updated",
        })

        st.data_editor(
            jobs_display,
            use_container_width=True,
            hide_index=True,
            disabled=True,
            column_config={
                "Job link": st.column_config.LinkColumn(
                    label="Job link",
                    help="Open job posting",
                    display_text="Open",
                ),
                "Views": st.column_config.NumberColumn(format="%d"),
                "Created at": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                "Last updated": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
            },
        )

        # Export CSV for this company
        st.download_button(
            label=f"Download CSV for {comp_name}",
            data=jobs_display.to_csv(index=False).encode("utf-8"),
            file_name=f"jobs_{comp_name}_{start_d}_{end_d}.csv",
            mime="text/csv",
        )

        # Clear selection
        if st.button("Back to grid"):
            st.session_state["ce_selected_company_id"] = None
            st.rerun()
