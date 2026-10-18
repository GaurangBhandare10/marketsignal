-- stg_edgar_filings.sql
-- Bronze → Silver staging for EDGAR 8-K filing metadata
-- Source: marketsignal.raw.edgar_filings (flat VARCHAR columns)

{{
  config(
    materialized = 'view',
    schema = 'dbt'
  )
}}

SELECT
    UPPER(TRIM(ticker))                                         AS ticker,
    TRIM(cik)                                                   AS cik,
    file_date                                                   AS filing_date,
    UPPER(TRIM(form_type))                                      AS filing_type,
    TRIM(display_names)                                         AS company_name,
    TRIM(file_name)                                             AS accession_number,
    period_of_report,
    ingested_at                                                 AS _loaded_at
FROM {{ source('raw', 'edgar_filings') }}
WHERE form_type = '8-K'
  AND file_date IS NOT NULL
  AND ticker IS NOT NULL
