# MarketSignal 📈

> **AI Data Engineering Portfolio Project**  
> Financial News & Market Intelligence — SEC EDGAR + GDELT → Snowflake → dbt → Airflow → RAG

---

## Overview

MarketSignal is an end-to-end AI data engineering pipeline that detects **unusual news activity around SEC filings**.  
It ingests SEC EDGAR filings and GDELT news events, transforms them with dbt, orchestrates with Apache Airflow, and surfaces insights via a Streamlit app powered by a local RAG (Retrieval-Augmented Generation) stack.

---

## Architecture

```
SEC EDGAR API ──┐
                ├──► MinIO (raw lake) ──► Snowflake (raw schema)
GDELT API ──────┘          │
                           ▼
                    dbt (Bronze → Silver → Gold)
                           │
                    Apache Airflow (orchestration)
                           │
                    ChromaDB + Groq (RAG / AI layer)
                           │
                    Streamlit (front-end)
```

---

## Tech Stack

| Layer | Tool | Cost |
|-------|------|------|
| Local object store | MinIO (Docker) | Free |
| Data warehouse | Snowflake | Free trial ($400 credits) |
| Transformations | dbt Core + dbt-snowflake | Free |
| Orchestration | Apache Airflow (Docker Compose) | Free |
| Embeddings | sentence-transformers | Free (local) |
| Vector DB | ChromaDB | Free (local) |
| LLM | Groq API (Llama 3) | Free tier |
| Front-end | Streamlit | Free |

---

## Project Structure

```
marketsignal/
├── ingestion/          # Python scripts — SEC EDGAR & GDELT ingestion
├── dbt/
│   └── marketsignal/
│       ├── models/
│       │   ├── staging/      # Bronze: raw Snowflake sources
│       │   ├── dimensions/   # Silver: dim_company, dim_date
│       │   ├── facts/        # Silver: fact_filings, fact_news_events
│       │   └── aggregates/   # Gold: agg_news_spike_pre_filing
│       ├── seeds/            # company_name_aliases.csv
│       ├── tests/            # dbt tests
│       └── macros/           # custom Jinja macros
├── airflow/
│   ├── dags/           # edgar_daily_dag.py, gdelt_15min_dag.py, dbt_full_refresh_dag.py
│   └── docker-compose.yml
├── rag/                # Chunking, embedding, ChromaDB, Groq completions
├── streamlit_app/      # Streamlit front-end
├── .env.example        # Template — copy to .env and fill in secrets (never commit .env)
└── README.md
```

---

## Quick Start

### 1. Clone & configure environment
```bash
git clone https://github.com/GaurangBhandare10/marketsignal.git
cd marketsignal
cp .env.example .env
# Edit .env with your credentials
```

### 2. Start MinIO + Airflow
```bash
cd airflow
docker-compose up -d
```

### 3. Set up Snowflake
```sql
CREATE DATABASE marketsignal;
CREATE SCHEMA marketsignal.raw;
CREATE SCHEMA marketsignal.dbt;
CREATE WAREHOUSE compute_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### 4. Initialise dbt
```bash
cd dbt/marketsignal
pip install dbt-snowflake
dbt deps
dbt debug   # verify connection
dbt seed    # load company aliases
dbt run
```

### 5. Run ingestion
```bash
pip install -r ingestion/requirements.txt
python ingestion/edgar_ingest.py
python ingestion/gdelt_ingest.py
```

### 6. Launch Streamlit app
```bash
pip install -r streamlit_app/requirements.txt
streamlit run streamlit_app/app.py
```

---

## Key dbt Model: `agg_news_spike_pre_filing`

Detects news volume spikes in the **7 days before an 8-K filing** vs. a 30-day baseline — a signal of unusual pre-announcement activity.

---

## Data Sources

- **SEC EDGAR** — Company Facts API & Full-Text Search (EFTS) — [edgar.sec.gov](https://efts.sec.gov)
- **GDELT** — Global Database of Events, Language, and Tone — [gdeltproject.org](https://www.gdeltproject.org)

---

## Authors

- Gaurang Bhandare ([@GaurangBhandare10](https://github.com/GaurangBhandare10))
- Bhalerao

---

*MSIM @ UIUC — AI Data Engineering Portfolio Project*
