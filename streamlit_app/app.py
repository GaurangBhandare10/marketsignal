"""
MarketSignal — Streamlit Front-End
Interactive UI for exploring news spikes around SEC filings and asking
RAG-powered questions about specific filings.

Run:
    streamlit run streamlit_app/app.py
"""

import os
import sys

import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# RAG pipeline lives one level up
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "rag"))
from rag_pipeline import answer_question  # noqa: E402

load_dotenv()

st.set_page_config(
    page_title="MarketSignal",
    page_icon="📈",
    layout="wide",
)

# ── Snowflake connection (cached) ──────────────────────────────────────────────
@st.cache_resource
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="dbt",
    )


@st.cache_data(ttl=300)
def load_spike_data(ticker: str | None = None) -> pd.DataFrame:
    conn = get_snowflake_conn()
    where = f"WHERE c.ticker = '{ticker}'" if ticker else ""
    query = f"""
        SELECT
            c.ticker,
            a.filing_date,
            a.pre_filing_7d_mentions,
            a.avg_daily_baseline,
            ROUND(a.spike_ratio, 2)   AS spike_ratio,
            a.is_spike
        FROM marketsignal.dbt.agg_news_spike_pre_filing a
        JOIN marketsignal.dbt.dim_company c ON a.company_id = c.company_id
        {where}
        ORDER BY a.filing_date DESC
        LIMIT 200
    """
    return pd.read_sql(query, conn)


# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.title("MarketSignal 📈")
st.sidebar.caption("Financial News Intelligence")

tickers = ["All"] + ["AAPL", "MSFT", "TSLA", "NVDA", "META", "GOOGL", "AMZN"]
selected_ticker = st.sidebar.selectbox("Ticker", tickers)
ticker_filter = None if selected_ticker == "All" else selected_ticker

# ── Main dashboard ─────────────────────────────────────────────────────────────
st.title("📰 News Spike Detection — Pre 8-K Filing")
st.caption("Detects unusual news volume in the 7 days before an 8-K filing vs. 30-day baseline")

df = load_spike_data(ticker_filter)

col1, col2, col3 = st.columns(3)
col1.metric("Total Filings Analysed", len(df))
col2.metric("Spikes Detected", df["IS_SPIKE"].sum())
col3.metric("Avg Spike Ratio", f"{df['SPIKE_RATIO'].mean():.2f}x")

# Spike table
st.subheader("Filings with News Spikes")
spike_df = df[df["IS_SPIKE"] == True].copy()
st.dataframe(
    spike_df[["TICKER", "FILING_DATE", "PRE_FILING_7D_MENTIONS", "SPIKE_RATIO"]],
    use_container_width=True,
)

# Chart
st.subheader("Spike Ratio Over Time")
chart_df = df[["FILING_DATE", "TICKER", "SPIKE_RATIO"]].copy()
chart_df = chart_df.pivot_table(index="FILING_DATE", columns="TICKER", values="SPIKE_RATIO")
st.line_chart(chart_df)

# ── RAG Q&A ────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("🤖 Ask About a Filing")
st.caption("Powered by sentence-transformers + ChromaDB + Groq (Llama 3)")

rag_ticker = st.text_input("Ticker (optional)", value=ticker_filter or "")
question = st.text_area("Your question", placeholder="What risks did the company disclose?")

if st.button("Ask", type="primary") and question:
    with st.spinner("Searching filings and generating answer…"):
        try:
            answer = answer_question(question, ticker=rag_ticker or None)
            st.success(answer)
        except Exception as e:
            st.error(f"Error: {e}")
