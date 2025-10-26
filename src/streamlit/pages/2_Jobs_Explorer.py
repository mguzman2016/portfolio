# pages/2_Jobs_Explorer.py
import datetime as dt
import pandas as pd
import streamlit as st
from utils.db import run_query

st.set_page_config(page_title="Jobs Explorer", page_icon="🗂️", layout="wide")
st.title("Jobs Explorer")
st.caption("Filter, page through, and export the normalized job postings.")

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

# -------------------- Options (from DB) --------------------
exp_opts = (
    run_query("SELECT DISTINCT job_experience_level FROM lk_jobs")["job_experience_level"]
    .dropna().astype(str).str.strip().replace("", pd.NA).dropna().drop_duplicates().tolist()
)

# -------------------- Defaults & state --------------------
# --- Defaults ---
END_DEF = dt.date.today()
START_DEF = END_DEF - dt.timedelta(days=30)

# Inicializa estado (si no existe)
st.session_state.setdefault("je_page", 1)
st.session_state.setdefault("je_page_size", 25)

def _seed_widget_defaults():
    st.session_state.setdefault("je_name_input", "")
    st.session_state.setdefault("je_date_range", (START_DEF, END_DEF))
    st.session_state.setdefault("je_exp_levels", [])

# --- HANDLE RESET FLAG *ANTES* de crear widgets ---
if st.session_state.get("je_do_reset", False):
    # Borra keys de widgets para que nazcan con defaults en este mismo run
    for k in ("je_name_input", "je_date_range", "je_exp_levels"):
        st.session_state.pop(k, None)
    # Resetea paginación
    st.session_state["je_page"] = 1
    st.session_state["je_page_size"] = 25
    # Re-semilla defaults
    _seed_widget_defaults()
    # Limpia flag
    st.session_state["je_do_reset"] = False
else:
    _seed_widget_defaults()

# -------------------- Filter bar (form) --------------------
with st.form("je_filters"):
    c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
    # Importante: usa SOLO key= (sin value/default)
    c1.text_input("Standardized name (substring match)", key="je_name_input", placeholder="e.g., data engineer")
    c2.date_input("Date range (last_updated)", key="je_date_range")
    c3.multiselect("Experience level", options=exp_opts, key="je_exp_levels")
    new_page_size = c4.selectbox("Rows/page", options=[25, 50, 100, 250],
                                 index=[25, 50, 100, 250].index(st.session_state["je_page_size"]))

    col_btn1, col_btn2 = st.columns([1, 1])
    apply_clicked = col_btn1.form_submit_button("Apply")
    reset_clicked = col_btn2.form_submit_button("Reset")

    if reset_clicked:
        # Solo marca flag y relanza; los defaults se aplican arriba antes de crear widgets
        st.session_state["je_do_reset"] = True
        st.rerun()
    else:
        # Si cambia el page size, actualiza y vuelve a página 1
        if new_page_size != st.session_state["je_page_size"]:
            st.session_state["je_page_size"] = new_page_size
            st.session_state["je_page"] = 1

# Lee filtros normalizados
start_d, end_d = normalize_date_range(st.session_state["je_date_range"], START_DEF, END_DEF)
start_ts = dt.datetime.combine(start_d, dt.time.min)
end_ts   = dt.datetime.combine(end_d, dt.time.max)
name_q = st.session_state["je_name_input"].strip()
exp_sel = st.session_state["je_exp_levels"]
page = st.session_state["je_page"]
page_size = st.session_state["je_page_size"]

# -------------------- WHERE builder --------------------
where_clauses = ["j.last_updated BETWEEN :start_ts AND :end_ts"]
params = {"start_ts": start_ts, "end_ts": end_ts}

if name_q:
    where_clauses.append("LOWER(j.standardized_name) LIKE :name_q")
    params["name_q"] = f"%{name_q.lower()}%"

if exp_sel:
    where_clauses.append("j.job_experience_level IN :exp_levels")
    # expand later
where_sql = " AND ".join(where_clauses)

# -------------------- COUNT (for pagination) --------------------
count_sql = f"""
SELECT COUNT(*) AS cnt
FROM lk_jobs j
LEFT JOIN lk_companies c ON c.company_id = j.company_id
LEFT JOIN lk_etl_status s ON s.etl_id   = j.etl_id
WHERE {where_sql}
"""

count_params = params.copy()
if exp_sel:
    count_sql, bind_exp = expand_in(count_sql, "exp_levels", exp_sel)
    count_params.update(bind_exp)

total_rows = int(run_query(count_sql, count_params)["cnt"].iloc[0])

# Ajusta página si está fuera de rango
max_page = max(1, (total_rows + page_size - 1) // page_size)
if page > max_page:
    page = max_page
    st.session_state["je_page"] = page

offset = (page - 1) * page_size

# -------------------- PAGE QUERY --------------------
page_sql = f"""
SELECT
  j.job_id,
  j.job_name,
  j.standardized_name,
  j.job_url,
  c.company_name,
  j.job_type,
  j.job_views,
  j.job_experience_level,
  j.created_at,
  s.city,
  j.last_updated
FROM lk_jobs j
LEFT JOIN lk_companies c ON c.company_id = j.company_id
LEFT JOIN lk_etl_status s ON s.etl_id   = j.etl_id
WHERE {where_sql}
ORDER BY j.last_updated DESC, j.job_id DESC
LIMIT :limit OFFSET :offset
"""

page_params = params.copy() | {"limit": int(page_size), "offset": int(offset)}
if exp_sel:
    page_sql, bind_exp2 = expand_in(page_sql, "exp_levels", exp_sel)
    page_params.update(bind_exp2)

df = run_query(page_sql, page_params)

# -------------------- Render table (with clickable job_name) --------------------
st.write(f"**Results:** {total_rows:,} rows — page {page} / {max_page}")

if not df.empty:
    df = df.copy()

    # Asegura tipos datetime para que DatetimeColumn formatee bien
    for col in ["created_at", "last_updated"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Prepara columnas para mostrar
    df_display = df[[
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
        df_display,
        use_container_width=True,
        hide_index=True,
        disabled=True,
        column_config={
            # La URL está en la celda; display_text es un texto único para todas las filas
            "Job link": st.column_config.LinkColumn(
                label="Job link",
                help="Open the original job posting",
                display_text="Open"
            ),
            "Views": st.column_config.NumberColumn(format="%d"),
            "Created at": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
            "Last updated": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
        },
    )
else:
    st.info("No results for the selected filters.")

# -------------------- Pagination controls --------------------
col_prev, col_mid, col_next = st.columns([1, 4, 1])
with col_prev:
    if st.button("← Prev", disabled=(page <= 1)):
        st.session_state["je_page"] = max(1, page - 1)
        st.rerun()
with col_mid:
    st.write(f"Page **{page}** of **{max_page}**")
with col_next:
    if st.button("Next →", disabled=(page >= max_page)):
        st.session_state["je_page"] = min(max_page, page + 1)
        st.rerun()

# Reset page to 1 when filters change (simple heuristic)
if (apply_clicked and st.session_state["je_page"] != 1):
    st.session_state["je_page"] = 1
    st.rerun()

st.divider()

# -------------------- Export CSV (full filtered result) --------------------
# Tope de filas a exportar para no “reventar” la app; ajusta según convenga:
EXPORT_LIMIT = 50000

export_sql = f"""
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
LEFT JOIN lk_companies c ON c.company_id = j.company_id
LEFT JOIN lk_etl_status s ON s.etl_id   = j.etl_id
WHERE {where_sql}
ORDER BY j.last_updated DESC, j.job_id DESC
LIMIT :limit
"""

export_params = params.copy() | {"limit": int(EXPORT_LIMIT)}
if exp_sel:
    export_sql, exp_bind = expand_in(export_sql, "exp_levels", exp_sel)
    export_params.update(exp_bind)

export_df = run_query(export_sql, export_params)

st.download_button(
    label=f"Download CSV (filtered, up to {EXPORT_LIMIT:,} rows)",
    data=export_df.to_csv(index=False).encode("utf-8"),
    file_name=f"jobs_filtered_{start_d}_{end_d}.csv",
    mime="text/csv",
)
