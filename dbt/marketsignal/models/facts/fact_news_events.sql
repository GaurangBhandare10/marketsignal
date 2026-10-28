-- fact_news_events.sql
-- Fact table: one row per (company × event_date) with mention count and avg tone
-- Grain: company_id + event_date (daily rollup from GDELT GKG)
-- Incremental: surrogate_key = MD5(company_id + event_date)

{{
  config(
    materialized = 'incremental',
    unique_key = 'surrogate_key',
    schema = 'dbt'
  )
}}

WITH companies AS (
    SELECT company_id, ticker, company_name
    FROM {{ ref('dim_company') }}
),

-- Explode pipe-delimited organizations column and match against known tickers/names
news_exploded AS (
    SELECT
        n.event_date,
        n.article_url,
        n.tone,
        n._loaded_at,
        TRIM(org.value)                                         AS org_mention
    FROM {{ ref('stg_gdelt_news') }} n,
         LATERAL SPLIT_TO_TABLE(
             CASE WHEN COALESCE(n.raw_organizations, '') IN ('', 'nan') THEN ''
                  ELSE n.raw_organizations END,
             ';'   -- GDELT V2Organizations field is semicolon-delimited
         ) AS org

    {% if is_incremental() %}
    WHERE n._loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Match organizations to dim_company via ticker or company_name (case-insensitive)
matched AS (
    SELECT
        c.company_id,
        ne.event_date,
        ne.tone,
        ne._loaded_at
    FROM news_exploded ne
    JOIN companies c
      ON UPPER(ne.org_mention) LIKE '%' || UPPER(c.ticker) || '%'
      -- Strip punctuation before comparing: "Tesla, Inc." → "TESLA INC" matches "tesla inc"
      OR REGEXP_REPLACE(UPPER(ne.org_mention),  '[^A-Z0-9 ]', '') LIKE '%' || REGEXP_REPLACE(UPPER(c.company_name), '[^A-Z0-9 ]', '') || '%'
      OR REGEXP_REPLACE(UPPER(c.company_name), '[^A-Z0-9 ]', '') LIKE '%' || REGEXP_REPLACE(UPPER(ne.org_mention),  '[^A-Z0-9 ]', '') || '%'
    WHERE ne.org_mention != ''
      AND LENGTH(ne.org_mention) >= 4   -- avoid short tokens like "Inc" matching everything
),

-- Daily rollup per company
daily_agg AS (
    SELECT
        company_id,
        event_date,
        COUNT(*)                                                AS num_mentions,
        AVG(tone)                                               AS avg_tone,
        MAX(_loaded_at)                                         AS _loaded_at
    FROM matched
    GROUP BY 1, 2
)

SELECT
    MD5(CONCAT(company_id, '|', event_date::STRING))            AS surrogate_key,
    company_id,
    event_date,
    num_mentions,
    avg_tone,
    _loaded_at
FROM daily_agg
