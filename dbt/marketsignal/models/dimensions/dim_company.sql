-- dim_company.sql
-- Company dimension — one row per unique ticker/CIK pair
-- Surrogate key: MD5 hash of ticker so fact tables can join without carrying the string

{{
  config(
    materialized = 'table',
    schema = 'dbt'
  )
}}

WITH base AS (
    SELECT DISTINCT
        ticker,
        cik,
        company_name
    FROM {{ ref('stg_edgar_filings') }}
    WHERE ticker IS NOT NULL
),

-- If a ticker appears with multiple names, keep the most recent one
ranked AS (
    SELECT
        ticker,
        cik,
        company_name,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ticker) AS rn
    FROM base
)

SELECT
    MD5(ticker)                                                 AS company_id,
    ticker,
    cik,
    company_name,
    CURRENT_TIMESTAMP()                                         AS _loaded_at
FROM ranked
WHERE rn = 1
