-- stg_edgar_filings.sql
-- Bronze → Silver staging for raw EDGAR filing JSON loaded into Snowflake
-- Source: marketsignal.raw.edgar_filings (VARIANT column)

{{
  config(
    materialized = 'view',
    schema = 'dbt'
  )
}}

SELECT
    f.raw_data:ticker::STRING                                   AS ticker,
    f.raw_data:cik::STRING                                      AS cik,
    filing.value:form::STRING                                   AS filing_type,
    filing.value:filingDate::DATE                               AS filing_date,
    filing.value:accessionNumber::STRING                        AS accession_number,
    filing.value:primaryDocument::STRING                        AS primary_document,
    CURRENT_TIMESTAMP()                                         AS _loaded_at
FROM marketsignal.raw.edgar_filings f,
     LATERAL FLATTEN(input => f.raw_data:filings) AS filing
WHERE filing.value:form::STRING = '8-K'
  AND filing.value:filingDate::DATE IS NOT NULL
