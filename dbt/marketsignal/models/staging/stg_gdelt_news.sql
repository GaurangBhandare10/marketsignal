-- stg_gdelt_news.sql
-- Bronze → Silver staging for raw GDELT GKG CSV data in Snowflake
-- Source: marketsignal.raw.gdelt_gkg

{{
  config(
    materialized = 'view',
    schema = 'dbt'
  )
}}

SELECT
    GKGRECORDID                                                 AS gkg_record_id,
    TO_DATE(LEFT(DATE::STRING, 8), 'YYYYMMDD')                 AS event_date,
    SourceCommonName                                            AS source_name,
    DocumentIdentifier                                          AS article_url,
    Organizations                                               AS raw_organizations,
    Themes                                                      AS raw_themes,
    Locations                                                   AS raw_locations,
    CURRENT_TIMESTAMP()                                         AS _loaded_at
FROM marketsignal.raw.gdelt_gkg
WHERE DATE IS NOT NULL
