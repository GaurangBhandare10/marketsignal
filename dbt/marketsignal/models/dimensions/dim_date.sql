-- dim_date.sql
-- Date dimension covering 2020-01-01 → 2030-12-31
-- Generated entirely in SQL — no seed file needed

{{
  config(
    materialized = 'table',
    schema = 'dbt'
  )
}}

WITH date_spine AS (
    SELECT
        DATEADD('day', SEQ4(), '2020-01-01'::DATE)              AS date_actual
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))   -- 2020-01-01 to 2030-12-31
),

enriched AS (
    SELECT
        date_actual,
        TO_NUMBER(TO_CHAR(date_actual, 'YYYYMMDD'))             AS date_id,
        YEAR(date_actual)                                       AS year,
        QUARTER(date_actual)                                    AS quarter,
        MONTH(date_actual)                                      AS month,
        TO_CHAR(date_actual, 'Month')                           AS month_name,
        DAY(date_actual)                                        AS day_of_month,
        DAYOFWEEK(date_actual)                                  AS day_of_week,       -- 0=Sun, 6=Sat
        TO_CHAR(date_actual, 'DY')                              AS day_name,
        DAYOFYEAR(date_actual)                                  AS day_of_year,
        WEEKOFYEAR(date_actual)                                  AS week_of_year,
        CASE
            WHEN DAYOFWEEK(date_actual) IN (0, 6) THEN FALSE
            ELSE TRUE
        END                                                     AS is_weekday
    FROM date_spine
)

SELECT * FROM enriched
