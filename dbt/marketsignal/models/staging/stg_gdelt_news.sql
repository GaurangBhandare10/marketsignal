-- stg_gdelt_news.sql
-- Bronze → Silver staging for GDELT Global Knowledge Graph news events
-- Source: marketsignal.raw.gdelt_gkg (flat columns)

{{
  config(
    materialized = 'view',
    schema = 'dbt'
  )
}}

SELECT
    capture_time,
    DATE_TRUNC('day', capture_time)                            AS event_date,
    TRIM(source_url)                                            AS article_url,
    themes                                                      AS raw_themes,
    tone,
    organizations                                               AS raw_organizations,
    ingested_at                                                 AS _loaded_at
FROM {{ source('raw', 'gdelt_gkg') }}
WHERE capture_time IS NOT NULL
  AND source_url IS NOT NULL
