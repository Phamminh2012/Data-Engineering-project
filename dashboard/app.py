import os
import pandas as pd
import streamlit as st

# =========================================================
# Page config
# =========================================================
st.set_page_config(
    page_title="Job Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# Paths
# =========================================================
DATA_DIR = "/opt/airflow/dags"
IMG_DIR = os.path.join(DATA_DIR, "save_img")

JOB_COUNTS_CSV = os.path.join(DATA_DIR, "job_counts.csv")
TOP_SKILLS_CSV = os.path.join(DATA_DIR, "top_skills.csv")
WORD_FREQ_CSV = os.path.join(DATA_DIR, "job_description_word_freq.csv")
WORDCLOUD_IMG = os.path.join(DATA_DIR, "job_description_wordcloud.png")
TOPICS_TXT = os.path.join(DATA_DIR, "topics.txt")
SUMMARY_TXT = os.path.join(DATA_DIR, "Summary.txt")
COEFF_CSV = os.path.join(DATA_DIR, "CoefficeintReport.csv")

COEF_PLOT = os.path.join(IMG_DIR, "coef_plot.png")
RESIDUAL_PLOT = os.path.join(IMG_DIR, "residuals_plot.png")
SILHOUETTE_PLOT = os.path.join(IMG_DIR, "silhouette_plot.png")
TOPIC_HEATMAP = os.path.join(IMG_DIR, "topic_heatmap.png")


# =========================================================
# Styling
# =========================================================
st.markdown("""
<style>
    .main {
        padding-top: 1rem;
    }

    .hero {
        padding: 1.4rem 1.6rem;
        border-radius: 18px;
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 45%, #334155 100%);
        color: white;
        margin-bottom: 1.25rem;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.18);
    }

    .hero h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 700;
    }

    .hero p {
        margin-top: 0.5rem;
        margin-bottom: 0;
        opacity: 0.92;
        font-size: 1rem;
    }

    .section-card {
        background: #ffffff;
        border: 1px solid rgba(100, 116, 139, 0.18);
        border-radius: 16px;
        padding: 1rem 1rem 0.8rem 1rem;
        box-shadow: 0 4px 16px rgba(15, 23, 42, 0.05);
        margin-bottom: 1rem;
    }

    .metric-card {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid rgba(100, 116, 139, 0.15);
        padding: 1rem 1rem 0.9rem 1rem;
        border-radius: 16px;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.05);
    }

    .metric-label {
        font-size: 0.9rem;
        color: #475569;
        margin-bottom: 0.35rem;
    }

    .metric-value {
        font-size: 1.7rem;
        font-weight: 700;
        color: #0f172a;
    }

    .metric-sub {
        font-size: 0.82rem;
        color: #64748b;
        margin-top: 0.2rem;
    }

    .small-note {
        color: #64748b;
        font-size: 0.9rem;
        margin-top: -0.2rem;
        margin-bottom: 0.8rem;
    }

    .tab-caption {
        color: #64748b;
        font-size: 0.92rem;
        margin-bottom: 0.6rem;
    }

    div[data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)


# =========================================================
# Helpers
# =========================================================
@st.cache_data
def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

def read_text(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    return "File not found."

def prepare_date(df, col):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def show_image(path, caption=None):
    if os.path.exists(path):
        st.image(path, caption=caption, use_container_width=True)
    else:
        st.warning(f"Missing file: {os.path.basename(path)}")

def metric_box(label, value, sub=""):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

def section_open(title, caption=""):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader(title)
    if caption:
        st.markdown(f'<div class="tab-caption">{caption}</div>', unsafe_allow_html=True)

def section_close():
    st.markdown('</div>', unsafe_allow_html=True)


# =========================================================
# Load data
# =========================================================
job_counts = prepare_date(load_csv(JOB_COUNTS_CSV), "_id")
top_skills = prepare_date(load_csv(TOP_SKILLS_CSV), "date")
word_freq = load_csv(WORD_FREQ_CSV)
coeff_df = load_csv(COEFF_CSV)

# =========================================================
# Derived metrics
# =========================================================
total_jobs = 0
latest_date = "N/A"
unique_skills = 0
top_skill = "N/A"

if not job_counts.empty and "jobCounts" in job_counts.columns:
    total_jobs = int(job_counts["jobCounts"].sum())

if not job_counts.empty and "_id" in job_counts.columns:
    valid_dates = job_counts["_id"].dropna()
    if not valid_dates.empty:
        latest_date = valid_dates.max().strftime("%Y-%m-%d")

if not top_skills.empty and "skill" in top_skills.columns:
    unique_skills = int(top_skills["skill"].nunique())

if not top_skills.empty and {"skill", "count"}.issubset(top_skills.columns):
    skill_rank = (
        top_skills.groupby("skill", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
    )
    if not skill_rank.empty:
        top_skill = skill_rank.iloc[0]["skill"]

# =========================================================
# Sidebar
# =========================================================
with st.sidebar:
    st.title("Dashboard")
    st.markdown("Analytics view for your Airflow aggregation outputs.")
    st.divider()

    st.markdown("### Quick filters")

    if not top_skills.empty and "date" in top_skills.columns:
        available_dates = sorted(
            top_skills["date"].dropna().dt.strftime("%Y-%m-%d").unique(),
            reverse=True
        )
        selected_skill_date = st.selectbox(
            "Skills date",
            ["All"] + list(available_dates)
        )
    else:
        selected_skill_date = "All"
        st.caption("No skill dates available yet.")

    top_n_skills = st.slider("Top N skills", 5, 30, 10)

    st.divider()
    st.markdown("### Notes")
    st.caption("This UI only reads outputs generated by your Airflow pipeline. It does not rerun aggregation logic.")

# =========================================================
# Hero
# =========================================================
st.markdown("""
<div class="hero">
    <h1>📊 Job Analytics Dashboard</h1>
    <p>Interactive dashboard for job trends, extracted skills, text mining, regression analysis, and topic modeling.</p>
</div>
""", unsafe_allow_html=True)

# =========================================================
# Overview
# =========================================================
st.markdown("## Overview")
st.markdown('<div class="small-note">A quick summary across all aggregation outputs.</div>', unsafe_allow_html=True)

m1, m2, m3, m4 = st.columns(4)
with m1:
    metric_box("Total Jobs", f"{total_jobs:,}", "From daily job count aggregation")
with m2:
    metric_box("Latest Date", latest_date, "Most recent aggregation date")
with m3:
    metric_box("Unique Skills", f"{unique_skills:,}", "Distinct extracted skills")
with m4:
    metric_box("Top Skill", top_skill, "Most frequent across the dataset")

st.markdown("")

# =========================================================
# Tabs
# =========================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Job Count",
    "Skills Count",
    "Word Cloud",
    "Regression",
    "Topic Modeling"
])

# =========================================================
# Tab 1 - Job Count
# =========================================================
with tab1:
    section_open("Job Count Aggregation", "Output of do_job_count()")

    if not job_counts.empty and {"_id", "jobCounts"}.issubset(job_counts.columns):
        trend_df = (
            job_counts.dropna(subset=["_id"])
            .sort_values("_id")
            .rename(columns={"_id": "date"})
        )

        c1, c2 = st.columns([2, 1])
        with c1:
            st.markdown("**Daily job posting volume**")
            st.line_chart(trend_df.set_index("date")["jobCounts"], use_container_width=True)

        with c2:
            total_days = len(trend_df)
            avg_jobs = round(trend_df["jobCounts"].mean(), 2) if not trend_df.empty else 0
            peak_jobs = int(trend_df["jobCounts"].max()) if not trend_df.empty else 0

            metric_box("Tracked Days", total_days, "Days with available job counts")
            metric_box("Average / Day", avg_jobs, "Mean jobs collected per day")
            metric_box("Peak / Day", peak_jobs, "Highest daily count")

        st.markdown("**Raw data**")
        st.dataframe(trend_df, use_container_width=True, hide_index=True)
    else:
        st.info("job_counts.csv not found or missing expected columns.")

    section_close()

# =========================================================
# Tab 2 - Skills Count
# =========================================================
with tab2:
    section_open("Skills Count Aggregation", "Output of do_skills_count()")

    if not top_skills.empty and {"date", "skill", "count"}.issubset(top_skills.columns):
        filtered = top_skills.copy()
        if selected_skill_date != "All":
            filtered = filtered[filtered["date"].dt.strftime("%Y-%m-%d") == selected_skill_date]

        display_df = (
            filtered.groupby("skill", as_index=False)["count"]
            .sum()
            .sort_values("count", ascending=False)
            .head(top_n_skills)
        )

        c1, c2 = st.columns([2, 1])

        with c1:
            st.markdown("**Top extracted skills**")
            if not display_df.empty:
                st.bar_chart(display_df.set_index("skill")["count"], use_container_width=True)
            else:
                st.info("No skill data for the selected filter.")

        with c2:
            if not display_df.empty:
                metric_box("Selected Date", selected_skill_date, "Current filter")
                metric_box("Shown Skills", len(display_df), "Rows displayed in chart")
                metric_box("Top Count", int(display_df["count"].max()), "Highest count in current view")
            else:
                metric_box("Selected Date", selected_skill_date, "Current filter")
                metric_box("Shown Skills", 0, "No rows in current view")
                metric_box("Top Count", 0, "No data available")

        st.markdown("**Skill table**")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("top_skills.csv not found or missing expected columns.")

    section_close()

# =========================================================
# Tab 3 - Word Cloud
# =========================================================
with tab3:
    section_open("Job Description Word Cloud Aggregation", "Output of do_job_description_wordcloud()")

    c1, c2 = st.columns([1.6, 1])

    with c1:
        st.markdown("**Word cloud**")
        show_image(WORDCLOUD_IMG, "Job description word cloud")

    with c2:
        st.markdown("**Top frequent words**")
        if not word_freq.empty and {"word", "count"}.issubset(word_freq.columns):
            st.dataframe(word_freq.head(25), use_container_width=True, hide_index=True)
        else:
            st.info("job_description_word_freq.csv not found.")

    section_close()

# =========================================================
# Tab 4 - Regression
# =========================================================
with tab4:
    section_open("Regression Aggregation", "Output of do_regression()")

    p1, p2 = st.columns(2)
    with p1:
        st.markdown("**Significant skill coefficients**")
        show_image(COEF_PLOT, "Skill coefficients — significant only")
    with p2:
        st.markdown("**Residual diagnostics**")
        show_image(RESIDUAL_PLOT, "Residuals vs fitted")

    st.markdown("**Coefficient report**")
    if not coeff_df.empty:
        st.dataframe(coeff_df, use_container_width=True, hide_index=True)
    else:
        st.info("CoefficeintReport.csv not found.")

    with st.expander("OLS Summary", expanded=False):
        st.text(read_text(SUMMARY_TXT))

    section_close()

# =========================================================
# Tab 5 - Topic Modeling
# =========================================================
with tab5:
    section_open("Topic Modeling Aggregation", "Output of do_topic_modeling()")

    st.markdown("**Extracted topics**")
    st.text_area(
        "Topic output",
        read_text(TOPICS_TXT),
        height=240,
        label_visibility="collapsed"
    )

    p1, p2 = st.columns(2)
    with p1:
        st.markdown("**Silhouette plot**")
        show_image(SILHOUETTE_PLOT, "Topic clustering silhouette")
    with p2:
        st.markdown("**Topic-word heatmap**")
        show_image(TOPIC_HEATMAP, "Topic-word heatmap")

    section_close()