-- agg_news_spike_pre_filing.sql
-- Gold aggregate: detects unusual news volume in 7 days before an 8-K filing
-- vs. a 30-day rolling baseline → spike_ratio > 1.5 = potential signal

{{
  config(
    materialized = 'table',
    schema = 'dbt'
  )
}}

WITH filing_dates AS (
    SELECT
        company_id,
        filing_date,
        filing_type
    FROM {{ ref('fact_filings') }}
    WHERE filing_type = '8-K'
),

news_windowed AS (
    SELECT
        n.company_id,
        f.filing_date,
        -- Sum of mentions in 7-day pre-filing window
        SUM(
            CASE
                WHEN n.event_date BETWEEN DATEADD('day', -7, f.filing_date)
                                      AND DATEADD('day', -1, f.filing_date)
                THEN n.num_mentions
                ELSE 0
            END
        )                                                       AS pre_filing_7d_mentions,
        -- Average daily mentions over 30-day baseline (days -37 to -8)
        AVG(n.num_mentions)                                     AS avg_daily_baseline
    FROM {{ ref('fact_news_events') }} n
    JOIN filing_dates f
      ON n.company_id = f.company_id
    WHERE n.event_date BETWEEN DATEADD('day', -37, f.filing_date)
                           AND DATEADD('day', -1,  f.filing_date)
    GROUP BY 1, 2
)

SELECT
    nw.*,
    -- Spike ratio: pre-filing 7-day vs expected baseline (7 * avg_daily)
    DIV0(
        pre_filing_7d_mentions,
        avg_daily_baseline * 7
    )                                                           AS spike_ratio,
    CASE
        WHEN DIV0(pre_filing_7d_mentions, avg_daily_baseline * 7) > 1.5
        THEN TRUE
        ELSE FALSE
    END                                                         AS is_spike
FROM news_windowed nw
