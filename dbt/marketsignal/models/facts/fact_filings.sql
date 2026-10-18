-- fact_filings.sql
-- Fact table for SEC EDGAR 8-K filings
-- Grain: one row per filing (ticker + filing_date + accession_number)
-- Incremental: appends new rows only; surrogate_key is the unique key

{{
  config(
    materialized = 'incremental',
    unique_key = 'surrogate_key',
    schema = 'dbt'
  )
}}

WITH staged AS (
    SELECT
        MD5(CONCAT(ticker, '|', filing_date::STRING, '|', accession_number))
                                                                AS surrogate_key,
        MD5(ticker)                                             AS company_id,
        filing_date,
        filing_type,
        accession_number,
        period_of_report,
        _loaded_at
    FROM {{ ref('stg_edgar_filings') }}

    {% if is_incremental() %}
    -- Only process rows newer than the latest already in the table
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM staged
