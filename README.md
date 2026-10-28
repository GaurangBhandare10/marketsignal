# MarketSignal 📈

> **AI Data Engineering Portfolio Project**  
> Detects unusual news activity around SEC 8-K filings using a fully automated, end-to-end pipeline.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?logo=apacheairflow)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![Streamlit](https://img.shields.io/badge/Streamlit-App-FF4B4B?logo=streamlit)
![ChromaDB](https://img.shields.io/badge/ChromaDB-Vector%20DB-orange)
![Groq](https://img.shields.io/badge/Groq-Llama%203.3-black)

---

## What Does It Do?

MarketSignal answers one question: **was there unusually high news coverage about a company in the days before it filed an 8-K with the SEC?**

An 8-K is a material event disclosure — earnings surprises, mergers, executive departures. If news volume spikes before the filing, that's a potential signal worth investigating.

### Real signals detected (Oct 2026)

| Company | 8-K Filed | 7-day Pre-Filing Mentions | Baseline Avg | Spike Ratio | Signal? |
|---------|-----------|--------------------------|--------------|-------------|---------|
| META (Meta Platforms) | 2026-10-21 | 766 | 55.1 | **1.99×** | ✅ |
| TSLA (Tesla) | 2026-10-21 | 112 | 9.0 | **1.78×** | ✅ |

> A spike ratio above **1.5×** is flagged as a signal — meaning the 7-day pre-filing news volume was 50%+ higher than the rolling 30-day baseline.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│                                                                     │
│   SEC EDGAR API                    GDELT 2.0 GKG                   │
│  (8-K filings metadata)         (15-min global news feed)          │
└────────────┬───────────────────────────────┬────────────────────────┘
             │                               │
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    OBJECT STORE — MinIO (Docker)                    │
│                                                                     │
│   edgar/filings_8k/{ticker}/{date}/    gdelt/gkg/{date}/{ts}.csv   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE — Snowflake                        │
│                                                                     │
│   RAW schema                    DBT schema                          │
│   ├── raw.gdelt_gkg             ├── stg_gdelt_news                  │
│   └── raw.edgar_filings         ├── stg_edgar_filings               │
│                                 ├── dim_company  dim_date           │
│                                 ├── fact_news_events                │
│                                 ├── fact_filings                    │
│                                 └── agg_news_spike_pre_filing  ◄──  │
└────────────────────────────┬────────────────────────────────────────┘
                             │
             ┌───────────────┴───────────────┐
             ▼                               ▼
┌─────────────────────────┐   ┌─────────────────────────────────────┐
│  VECTOR STORE — ChromaDB│   │      STREAMLIT DASHBOARD            │
│                         │   │                                     │
│  SEC 8-K text chunks    │   │  • News spike table & chart         │
│  embedded with          │   │  • Spike ratio over time            │
│  all-MiniLM-L6-v2       │   │  • RAG Q&A: "What did Tesla        │
│                         │   │    disclose before the Oct filing?" │
│  ◄── Groq Llama 3.3 ──► │   │                                     │
└─────────────────────────┘   └─────────────────────────────────────┘
             │
┌─────────────────────────────────────────────────────────────────────┐
│               ORCHESTRATION — Apache Airflow (Docker Compose)       │
│                                                                     │
│   gdelt_15min_dag    ──► every 15 min  — GDELT → MinIO → Snowflake │
│   edgar_daily_dag    ──► 06:00 UTC     — EDGAR → MinIO → Snowflake │
│                                          → ChromaDB (auto index)   │
│   dbt_full_refresh   ──► 02:00 UTC     — full dbt refresh          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Why |
|-------|------|-----|
| Object store | MinIO (Docker) | Local S3-compatible raw lake |
| Data warehouse | Snowflake | Scalable, free $400 trial credits |
| Transformations | dbt Core + dbt-snowflake | SQL-based, tested, documented models |
| Orchestration | Apache Airflow 2.9 | Scheduling, retries, dependency graph |
| Embeddings | sentence-transformers (`all-MiniLM-L6-v2`) | Free, local, no API key |
| Vector DB | ChromaDB | Persistent local vector store |
| LLM | Groq API — `llama-3.3-70b-versatile` | Free tier, fast inference |
| Front-end | Streamlit | Fast interactive dashboard |

**All components except Snowflake run locally for free.**

---

## dbt Model Lineage

```
raw.gdelt_gkg          raw.edgar_filings
      │                       │
      ▼                       ▼
stg_gdelt_news       stg_edgar_filings
      │                       │
      ▼                       ▼
fact_news_events ──► fact_filings
      │                       │
      └──────────┬────────────┘
                 ▼
    agg_news_spike_pre_filing   ◄── Gold layer: spike detection
```

### Spike detection logic (`agg_news_spike_pre_filing`)

```
spike_ratio = (mentions in days -7 to -1 before filing)
              ────────────────────────────────────────────
              (avg daily mentions in days -37 to -8) × 7

is_spike = spike_ratio > 1.5
```

The baseline window (`-37 to -8`) deliberately excludes the 7 days directly before the filing to avoid contaminating the baseline with pre-announcement noise.

---

## Airflow DAGs

| DAG | Schedule | Tasks |
|-----|----------|-------|
| `gdelt_15min_dag` | Every 15 min | Download GDELT GKG → filter finance themes → MinIO → Snowflake |
| `edgar_daily_dag` | 06:00 UTC daily | Fetch 8-K filings → MinIO → Snowflake → **index into ChromaDB** |
| `dbt_full_refresh` | 02:00 UTC daily | Full `dbt run --full-refresh` across all models |

The `edgar_daily_dag` automatically indexes new filings into ChromaDB after every run — so the RAG chatbot is always up to date without manual steps.

---

## RAG Chatbot

The Streamlit app includes a Q&A interface backed by:

1. **ChromaDB** — stores chunked 8-K filing text (800-char chunks, 100-char overlap)
2. **sentence-transformers** (`all-MiniLM-L6-v2`) — local embeddings, no API key
3. **Groq** (`llama-3.3-70b-versatile`) — free LLM for answer generation

Example questions:
- *"What risks did Meta disclose before the October filing?"*
- *"Did Tesla mention supply chain issues?"*
- *"What was the reason for the NVDA 8-K?"*

---

## Project Structure

```
marketsignal/
├── ingestion/
│   ├── edgar_ingest.py        # SEC EDGAR Submissions API → MinIO
│   └── gdelt_ingest.py        # GDELT 2.0 GKG → MinIO → Snowflake (parallel backfill)
├── dbt/marketsignal/
│   ├── models/
│   │   ├── staging/           # stg_gdelt_news, stg_edgar_filings
│   │   ├── dimensions/        # dim_company, dim_date
│   │   ├── facts/             # fact_news_events, fact_filings
│   │   └── aggregates/        # agg_news_spike_pre_filing (Gold)
│   ├── seeds/                 # company_name_aliases.csv
│   └── tests/                 # schema tests
├── airflow/
│   ├── Dockerfile             # Airflow image with dbt + chromadb + sentence-transformers
│   ├── docker-compose.yml
│   └── dags/
│       ├── edgar_daily_dag.py
│       ├── gdelt_15min_dag.py
│       └── dbt_full_refresh_dag.py
├── rag/
│   ├── rag_pipeline.py        # ChromaDB + sentence-transformers + Groq
│   └── index_edgar_filings.py # Fetches 8-K text from EDGAR, indexes into ChromaDB
├── streamlit_app/
│   └── app.py                 # Dashboard + RAG Q&A
├── .env.example
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop
- Python 3.11+
- Snowflake account (free $400 trial at [snowflake.com](https://www.snowflake.com))
- Groq API key (free at [console.groq.com](https://console.groq.com))

### 1. Clone & configure

```bash
git clone https://github.com/GaurangBhandare10/marketsignal.git
cd marketsignal
cp .env.example .env
# Fill in SNOWFLAKE_*, GROQ_API_KEY in .env
```

### 2. Set up Snowflake

```sql
-- Run as ACCOUNTADMIN
CREATE DATABASE MARKETSIGNAL;
CREATE SCHEMA MARKETSIGNAL.RAW;
CREATE ROLE MARKETSIGNAL_ROLE;
CREATE USER MARKETSIGNAL_USER PASSWORD='...' DEFAULT_ROLE=MARKETSIGNAL_ROLE;
GRANT ROLE MARKETSIGNAL_ROLE TO USER MARKETSIGNAL_USER;
GRANT ALL ON DATABASE MARKETSIGNAL TO ROLE MARKETSIGNAL_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE MARKETSIGNAL TO ROLE MARKETSIGNAL_ROLE;
CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MARKETSIGNAL_ROLE;
```

### 3. Build and start Airflow + MinIO

```bash
cd airflow
docker compose build   # one-time: bakes dbt + chromadb + sentence-transformers into image
docker compose up -d
```

Airflow UI → [localhost:8090](http://localhost:8090) (admin / admin)  
MinIO UI → [localhost:9001](http://localhost:9001) (minioadmin / minioadmin)

### 4. Set Airflow Variables

In the Airflow UI → **Admin → Variables**, create:

| Key | Example Value |
|-----|---------------|
| `SNOWFLAKE_ACCOUNT` | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | `MARKETSIGNAL_USER` |
| `SNOWFLAKE_PASSWORD` | `your-password` |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` |
| `MINIO_ENDPOINT` | `http://minio:9000` |
| `MINIO_ACCESS_KEY` | `minioadmin` |
| `MINIO_SECRET_KEY` | `minioadmin` |
| `EDGAR_USER_AGENT` | `YourName/1.0 your@email.com` |
| `WATCHLIST_TICKERS` | `["AAPL","MSFT","TSLA","NVDA","META"]` |

### 5. Run initial backfill

```bash
# Backfill 51 days of GDELT news data to MinIO, then load to Snowflake
python ingestion/gdelt_ingest.py --backfill 51
python ingestion/gdelt_ingest.py --load-snowflake 51

# Trigger EDGAR + dbt in Airflow, or run manually:
python ingestion/edgar_ingest.py
```

### 6. Set up dbt

```bash
cd dbt/marketsignal
pip install dbt-snowflake==1.8.4
dbt deps
dbt seed        # loads company aliases
dbt run         # builds all models
```

### 7. Launch Streamlit

```bash
# Run from project root so ChromaDB path resolves correctly
pip install -r streamlit_app/requirements.txt
python -m streamlit run streamlit_app/app.py
```

Open [localhost:8501](http://localhost:8501).

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier (e.g. `abc123.us-east-1`) |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_ROLE` | Role with access to MARKETSIGNAL database |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse name |
| `SNOWFLAKE_DATABASE` | Database name (default: `MARKETSIGNAL`) |
| `MINIO_ENDPOINT` | MinIO S3 endpoint (e.g. `http://localhost:9000`) |
| `MINIO_ACCESS_KEY` | MinIO access key |
| `MINIO_SECRET_KEY` | MinIO secret key |
| `MINIO_BUCKET` | Bucket name (default: `marketsignal-raw`) |
| `EDGAR_USER_AGENT` | SEC fair-use header e.g. `MyApp/1.0 you@email.com` |
| `GROQ_API_KEY` | Groq API key from [console.groq.com](https://console.groq.com) |
| `GROQ_MODEL` | Groq model ID (default: `llama-3.3-70b-versatile`) |
| `CHROMA_PERSIST_PATH` | Path to ChromaDB store (default: `./rag/chroma_store`) |

---

## Data Sources

| Source | What it provides | Update frequency |
|--------|-----------------|-----------------|
| [SEC EDGAR Submissions API](https://data.sec.gov/submissions/) | 8-K filing dates, CIK, primary document | Daily (06:00 UTC) |
| [GDELT 2.0 GKG](https://www.gdeltproject.org/) | Global news events, themes, org mentions, tone | Every 15 minutes |

---

## Author

**Gaurang Bhandare** — MSIM @ UIUC  
GitHub: [@GaurangBhandare10](https://github.com/GaurangBhandare10)

---

*Built as an AI Data Engineering portfolio project demonstrating end-to-end pipeline design: ingestion, storage, transformation, orchestration, and an AI query layer.*
